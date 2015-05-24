;;;; Copyright (c) Frank James 2015 <frank.a.james@gmail.com>
;;;; This code is licensed under the MIT license.

(defpackage #:pounds.log
  (:use #:cl #:pounds #:trivial-gray-streams)
  (:nicknames #:plog)
  (:export #:open-log
	   #:close-log
	   #:start-following
	   #:stop-following
	   #:dump-log
	   #:write-message 
	   #:read-message))

(in-package #:pounds.log)


;; we want to define a circular log stream from the buffer
;; the operations should be to read and write a log structure to/from it


;; we define two structures which get written to disk. I actually 
;; serialze the structures using XDR, but because the frpc system (which contains the xdr serializer)
;; depends on this package, I just hand-type the functions. There's only two structures anyway: the log header,
;; which consumes the 1st block (block 0), and the log message itself (which consumes as many blocks as needed).


(defstruct log-header 
  id ;; id of next msg to write
  index ;; index of next msg to write
  count ;; number of blocks 
  size) ;; block size

(defun read-log-header (stream)
  (declare (type stream stream))
  (make-log-header 
   :id (nibbles:read-ub32/be stream)
   :index (nibbles:read-ub32/be stream)
   :count (nibbles:read-ub32/be stream)
   :size (nibbles:read-ub32/be stream)))

(defun write-log-header (stream header)
  (declare (type stream stream)
	   (type log-header header))
  (let ((id (log-header-id header))
	(count (log-header-count header))
	(size (log-header-size header))
	(index (log-header-index header)))
    (nibbles:write-ub32/be id stream)
    (nibbles:write-ub32/be index stream)
    (nibbles:write-ub32/be count stream)
    (nibbles:write-ub32/be size stream)))


(defconstant +msg-magic+ #x3D5B612E
  "Magic number placed at the start of a message block to make it easy to identify value messages in the log.")

(defstruct log-message
  magic ;; message start delimiter
  id ;; unique message id
  lvl ;; message severity level
  time ;; timestamp
  tag ;; user-configurable string of exactly 4 octets
  msg) ;; variable length message

(defun level-int (lvl)
  (ecase lvl
    (:trace 0)
    (:debug 1)
    (:info 2)
    (:error 3)
    (:warning 4)))
(defun int-level (int)
  (ecase int
    (0 :trace)
    (1 :debug)
    (2 :info)
    (3 :error)
    (4 :warning)))

(defun read-log-message (stream block-size fsize)
  (declare (type stream stream))
  (let ((magic (nibbles:read-ub32/be stream)))
    (unless (= magic +msg-magic+)
      (return-from read-log-message nil))
    (let ((id (nibbles:read-ub32/be stream))
	  (lvl (nibbles:read-ub32/be stream))
	  (time (nibbles:read-ub64/be stream))
	  (tag (let ((octets (nibbles:make-octet-vector 4)))
		 (read-sequence octets stream)
		 (babel:octets-to-string octets)))
	  (len (nibbles:read-ub32/be stream)))
      ;;    (assert (= magic +msg-magic+))
      (let ((octets (nibbles:make-octet-vector len))
	    (pos (file-position stream)))
	(cond
	  ((<= (+ pos (length octets)) fsize)
	   (read-sequence octets stream))
	  (t 
	   (read-sequence octets stream :end (- fsize pos))
	   (file-position stream block-size)
	   (read-sequence octets stream :start (- fsize pos))))
	(make-log-message 
	 :magic magic
	 :id id
	 :lvl (int-level lvl)
	 :time time
	 :tag tag
	 :msg (babel:octets-to-string octets))))))
 
(defun write-log-message (stream id lvl tag msg block-size fsize)
  (declare (type stream stream)
	   (type integer id)
	   (type keyword lvl)
	   (type (vector (unsigned-byte 8) 4) tag)
	   (type string msg))
  (nibbles:write-ub32/be +msg-magic+ stream)
  (nibbles:write-ub32/be id stream)
  (nibbles:write-ub32/be (level-int lvl) stream)
  (nibbles:write-ub64/be (get-universal-time) stream)
  (write-sequence tag stream)
  (nibbles:write-ub32/be (length msg) stream)
  (let ((octets (babel:string-to-octets msg))
        (pos (file-position stream)))
    (cond
     ((<= (+ pos (length octets)) fsize)
      (write-sequence octets stream))
     (t 
      (write-sequence octets stream :end (- fsize pos))
      (file-position stream block-size)
      (write-sequence octets stream :start (- fsize pos))))))

(defstruct (plog (:constructor %make-plog)
		 (:copier %copy-plog))
  stream count size tag)

(defun advance-to-next-block (log)
  "Advance the stream position to the start of the next block."
  (declare (type plog log))
  (let ((offset (mapping-stream-position (plog-stream log))))
    ;; set the index to the start of the next block
    ;; recall that the first block is reserved for the header
    (let ((new-index
	   (multiple-value-bind (q r) (truncate offset (plog-size log))
	     (cond
	       ((= q (1- (plog-count log)))
		;; we are in the final block -- move back to the start
		1)
	       ((zerop r)
		;; we just happen to be at the start of a new block- -- do nothing
		q)
	       (t 
		;; next block position
		(1+ q))))))
      (file-position (plog-stream log)
		     (* (plog-size log) new-index))
      new-index)))

(defun advance-to-block (log index)
  "Set the stream position to the block index"
  (let ((offset (* (plog-size log) index)))
    (file-position (plog-stream log) offset)))

(defun advance-to-next (log)
  "Reset the log position of the next message."
  (declare (type plog log))
  (let ((init-index (header-index log))
	(stream (plog-stream log)))
    (advance-to-block log init-index)
    (do ((index init-index)
	 (end-index (mod (+ (plog-count log) 
			    (1- init-index))
			 (plog-count log)))
	 (magic (nibbles:read-ub32/be stream)
		(nibbles:read-ub32/be stream)))
	((or (= magic +msg-magic+)
	     (= index end-index))
	 (advance-to-block log index))
      (incf index)
      (when (= index (plog-count log))
	(setf index 0))
      (advance-to-block log index))))

;; ------------------------------------------------
    
(defun make-plog (mapping-stream size &key tag)
  "Make a plog instance from a mapping stream. 
SIZE should be the size of each block."
  (file-position mapping-stream 0)
  (let* ((map (mapping-stream-mapping mapping-stream))
	 (header (read-log-header mapping-stream))
	 (count (truncate (mapping-size map) size)))

    ;; validate the header information
    (cond
      ((zerop (log-header-size header))
       ;; zero size, must be a new header
       (setf (log-header-size header) size
	     (log-header-count header) count
	     (log-header-id header) 0
	     (log-header-index header) 1))
      ((not (= (log-header-size header) size))
       (error "log header size ~A does not match size ~A" 
	      (log-header-size header)
	      size)))

    ;; write the header back to the file
    (file-position mapping-stream 0)
    (write-log-header mapping-stream header)

    ;; set the initial stream position
    (file-position mapping-stream 
		   (* size (log-header-index header)))

    ;; make the log instance 
    (%make-plog :stream mapping-stream
		:size size
		:count count
		:tag (let ((octets (babel:string-to-octets (or tag "DEBG"))))
		       (assert (= (length octets) 4))
		       octets))))

(defun copy-log (log &key tag copy-stream)
  "Make a copy of the log stream, possibly changing the log tag"
  (let ((new-log
	 (%make-plog :stream (if copy-stream
				(make-instance 'mapping-stream
					       :mapping (mapping-stream-mapping 
							 (plog-stream log)))
				(plog-stream log))
		    :tag (if tag
			     (let ((octets (babel:string-to-octets tag)))
			       (assert (= (length octets) 4))
			       octets)
			     (plog-tag log))
		    :count (plog-count log)
		    :size (plog-size log))))
    (file-position (plog-stream new-log)
		   (* (plog-size new-log)
		      (header-index new-log)))
    new-log))
		 
(defun read-header (log)
  (let ((stream (plog-stream log)))
    (with-locked-mapping (stream)
      (let ((pos (file-position stream)))
	(file-position stream 0)
	(prog1 (read-log-header stream)
	  (file-position stream pos))))))

(defun write-header (header log)
  (let ((stream (plog-stream log)))
    (with-locked-mapping (stream)
      (let ((pos (file-position stream)))
	(file-position (plog-stream log) 0)
	(prog1 (write-log-header stream header)
	  (file-position stream pos))))))

(defun header-id (log)
  "Read the current message ID from the log header"
  (declare (type plog log))
  (let ((stream (plog-stream log)))
    (declare (type mapping-stream stream))
    (let ((pos (mapping-stream-position stream)))
      (setf (mapping-stream-position stream) 0)
      (prog1 (nibbles:read-ub32/be stream)
	(setf (mapping-stream-position stream) pos)))))

(defun header-index (log)
  "Read the current block index from the header"
  (declare (type plog log))
  (let ((stream (plog-stream log)))
    (declare (type mapping-stream stream))
    (let ((pos (mapping-stream-position stream)))
      (setf (mapping-stream-position stream) 4)
      (prog1 (nibbles:read-ub32/be stream)
	(setf (mapping-stream-position stream) pos)))))

(defun set-header-id-index (log id index)
  "Set the log header message id and block index"
  (let ((stream (plog-stream log)))
    (let ((pos (file-position stream)))
      (file-position stream 0)
      (nibbles:write-ub32/be id stream)
      (nibbles:write-ub32/be index stream)
      (file-position stream pos))))

(defun write-message (log lvl message &key tag)
  "Write a message to the log. Updates the log header information and advances the underlying mapping stream.

LVL should be a keyword namign a log level.

MESSAGE should be a string with the message to write.

TAG, if provided, will be the message tag, otherwise the default tag for the log will be used.
"
  (declare (type plog log)
	   (type string message))
  (let ((stream (plog-stream log)))
    (with-locked-mapping (stream)
      (let ((id (header-id log))
	    (index (header-index log)))
	(advance-to-block log index)
	(write-log-message stream
			   id
			   lvl
			   (or tag (plog-tag log))
			   message
                           (plog-size log)
                           (* (plog-size log) (plog-count log)))
	(let ((index (advance-to-next-block log)))
	  ;; increment the log id and set the new index
	  (set-header-id-index log (1+ id) index))
	id))))

(defun read-message (log)
  "Read the next message from the log"
  (declare (type plog log))
  (let ((stream (plog-stream log)))
    (flet ((rmsg ()
	     (with-locked-mapping (stream)
	       (let ((msg (read-log-message stream 
					    (plog-size log) 
					    (* (plog-size log) (plog-count log)))))
		 (cond
		   ((null msg)
		    (advance-to-next-block log)
                    nil)
		   ((= (log-message-magic msg) +msg-magic+)
		    (advance-to-next-block log)
		    msg)
		   (t 
		    (advance-to-next-block log)
		    nil))))))
      (do ((m (rmsg) (rmsg)))
	  (m m)))))


(defun write-message-to-stream (stream msg)
  "Format a message tothe stream"
  (multiple-value-bind (seconds min hour day month year i1 i2 i3)
      (decode-universal-time (log-message-time msg))
    (declare (ignore i1 i2 i3))
    (format stream
	    "~D-~D-~D ~2,'0D:~2,'0D:~2,'0D ~A:~A ~A"
	    year month day hour min seconds
	    (log-message-tag msg)
	    (log-message-lvl msg)
	    (log-message-msg msg))))

(defparameter *default-log-file* (merge-pathnames "pounds.log" (user-homedir-pathname)))
(defconstant +default-count+ (* 1024 16))
(defconstant +default-size+ 128)

(defun open-log (&key path count size tag)
  "Open a log file, creating it if it doesn't exist.

PATH should be a string representing a pathname to the log file to use. THe file will be created if it doesn't exist.
The pathname MUST be in the local system format.

COUNT, if provided, is the number of blocks to use in the log file. Default is 16k.

SIZE is the size of each block. Default is 128 bytes. The total filesize is (* SIZE COUNT).

TAG, if provided, should be a string of exactly 4 characters which is used to tag each message written to the log
file.

Returns a PLOG structure."
  (unless count (setf count +default-count+))
  (unless size (setf size +default-size+))
  (let ((map (open-mapping (or path (namestring *default-log-file*))
			   (* count size))))
    (handler-case 
	(make-plog (make-mapping-stream map)
		   size
		   :tag tag)
      (error (e)
	(close-mapping map)
	(error e)))))
  
(defun close-log (log)
  "Close the log and its associated file mapping."
  (close-mapping (mapping-stream-mapping (plog-stream log))))

;; -----------------------

(defvar *follower* nil)

(defstruct (follower (:constructor %make-follower))
  thread 
  exit-p 
  log 
  output 
  tag
  lvl)

;; followers need to use a different mapping stream because otherwise writing to the 
;; log would update the mapping stream position 
(defun make-follower (log &key (stream *standard-output*) tag levels)
  "Make a follower for the log streeam specified. Will output the messages to the stream provided."
  (%make-follower :log (copy-log log :copy-stream t)
		  :output stream
		  :tag tag
		  :lvl levels))

(defun follow-log (follower)
  "Print the log messages to the output stream until the exit-p flag is signalled."
  (do ((log (follower-log follower))
       (stream (plog-stream (follower-log follower)))
       (id 0)
       (output (follower-output follower)))
      ((follower-exit-p follower))
    (sleep 1)
    (let ((new-id (with-locked-mapping (stream)
		    (header-id log))))
      (when (> new-id id)
	(do ((done nil))
	    (done)
	  (let ((msg (read-message log)))
	    (when (and (if (follower-lvl follower)
			   (member (log-message-lvl msg)
				   (follower-lvl follower))
			   t)
		       (if (follower-tag follower)
			   (string-equal (follower-tag follower) (log-message-tag msg))
			   t))
	      (write-message-to-stream output msg)
	      (terpri output))
	    (when (>= (log-message-id msg) (1- new-id))
	      (setf done t))))
	(setf id new-id)))))

(defun start-following (log &key (stream *standard-output*) tag levels)
  "Start following the log. If TAG is provided, only those messages with a matching tag will be displayed."
  (when *follower*
    (error "Already following"))
  (setf *follower* (make-follower log 
				  :stream stream 
				  :tag tag 
				  :levels levels))
  (advance-to-next (follower-log *follower*))
  (setf (follower-exit-p *follower*)
	nil
	(follower-thread *follower*)
	(bt:make-thread (lambda ()
			  (follow-log *follower*))
			:name "follower-thread")))

(defun stop-following ()
  "Stop following the log."
  (setf (follower-exit-p *follower*) t)
  (bt:join-thread (follower-thread *follower*))
  (setf *follower* nil)
  nil)

(defun dump-log (log &key (stream *standard-output*) tag levels)
  "Dump the contents of the log to the stream. Filters the messages on tag and levels, if provided."
  (let ((lg (copy-log log :copy-stream t))
	(id (header-id log)))
    (advance-to-next lg)
    (do ((msg (read-message lg) (read-message lg)))
	((>= (log-message-id msg) (1- id)))
      (when (and (if levels 
		     (member (log-message-lvl msg) levels)
		     t)
		 (if tag
		     (string-equal (log-message-tag msg) tag)
		     t))
	(write-message-to-stream stream msg)
	(terpri stream)))))

       
    
