;;;; Copyright (c) Frank James 2015 <frank.a.james@gmail.com>
;;;; This code is licensed under the MIT license.

(defpackage #:pounds.log
  (:use #:cl #:pounds #:trivial-gray-streams)
  (:nicknames #:plog)
  (:export #:open-log
	   #:close-log
	   #:copy-log
	   #:start-following
	   #:stop-following
	   #:write-message 
	   #:read-message))

(in-package #:pounds.log)


;; we want to define a circular log stream from the buffer
;; the operations should be to read and write a log structure to/from it


(defstruct log-header 
  id ;; id of next msg to write
  index ;; index of next msg to write
  count
  size)

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


(defconstant +msg-magic+ #x3D5B612E)

(defstruct log-message
  magic
  id
  lvl
  time
  tag
  msg)

(defun read-log-message (stream)
  (declare (type stream stream))
  (let ((magic (nibbles:read-ub32/be stream))
	(id (nibbles:read-ub32/be stream))
	(lvl (nibbles:read-ub32/be stream))
	(time (nibbles:read-ub64/be stream))
	(tag (let ((octets (nibbles:make-octet-vector 4)))
	       (read-sequence octets stream)
	       (babel:octets-to-string octets)))
	(len (nibbles:read-ub32/be stream)))
    (assert (= magic +msg-magic+))
    (let ((octets (nibbles:make-octet-vector len)))
      (read-sequence octets stream)
      (make-log-message 
       :magic magic
       :id id
       :lvl (ecase lvl
	      (1 :info)
	      (2 :warning)
	      (4 :error))
       :time time
       :tag tag
       :msg (babel:octets-to-string octets)))))

(defun write-log-message (stream message)
  (declare (type stream stream)
	   (type log-message message))
  (let ((msg (log-message-msg message)))
    (declare (type string msg))
    (nibbles:write-ub32/be +msg-magic+ stream)
    (nibbles:write-ub32/be (log-message-id message) stream)
    (nibbles:write-ub32/be (ecase (log-message-lvl message)
			     (:info 1)
			     (:warning 2)
			     (:error 4))
			   stream)
    (nibbles:write-ub64/be (log-message-time message) stream)
    (write-sequence (log-message-tag message) stream)
    (nibbles:write-ub32/be (length msg) stream)
    (write-sequence (babel:string-to-octets msg)
		    stream)))

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
  (let ((stream (plog-stream log)))
    (let ((pos (file-position stream)))
      (file-position stream 0)
      (prog1 (nibbles:read-ub32/be stream)
	(file-position stream pos)))))

(defun header-index (log)
  (let ((stream (plog-stream log)))
    (let ((pos (file-position stream)))
      (file-position stream 4)
      (prog1 (nibbles:read-ub32/be stream)
	(file-position stream pos)))))

(defun set-header-id-index (log id index)
  (let ((stream (plog-stream log)))
    (let ((pos (file-position stream)))
      (file-position stream 0)
      (nibbles:write-ub32/be id stream)
      (nibbles:write-ub32/be index stream)
      (file-position stream pos))))

(defun write-message (log lvl message &key tag)
  "Write a log message"
  (declare (type plog log)
	   (type (member :info :warning :error) lvl)
	   (type string message))
  (let ((stream (plog-stream log)))
    (with-locked-mapping (stream)
      (let ((id (header-id log))
	    (index (header-index log)))
	(advance-to-block log index)
	(write-log-message stream
			   (make-log-message :id id
					     :lvl lvl
					     :time (get-universal-time)
					     :tag (or tag (plog-tag log))
					     :msg message))
	(let ((index (advance-to-next-block log)))
	  ;; increment the log id and set the new index
	  (set-header-id-index log (1+ id) index))))))

(defun read-message (log)
  "Read the next message from the log"
  (declare (type plog log))
  (let ((stream (plog-stream log)))
    (with-locked-mapping (stream)
      (let ((msg (read-log-message stream)))
	(assert (= (log-message-magic msg) +msg-magic+))
	(advance-to-next-block log)
	msg))))

(defun write-message-to-stream (stream msg)
  "Format a message tothe stream"
  (multiple-value-bind (seconds min hour day month year i1 i2 i3)
      (decode-universal-time (log-message-time msg))
    (declare (ignore i1 i2 i3))
    (format stream
	    "~D-~D-~D ~2,'0D:~2,'0D:~2,'0D ~A ~A ~A"
	    year month day hour min seconds
	    (log-message-tag msg)
	    (log-message-lvl msg)
	    (log-message-msg msg))))

(defparameter *default-log-file* "pounds.log")
(defparameter *default-count* (* 1024 16))
(defparameter *default-size* 128)

(defun open-log (&key path count size tag)
  (unless count (setf count *default-count*))
  (unless size (setf size *default-size*))
  (let ((map (open-mapping (or path *default-log-file*)
			   :size (* count size))))
    (handler-case 
	(make-plog (make-mapping-stream map)
		   size
		   :tag tag)
      (error (e)
	(close-mapping map)
	(error e)))))
  
(defun close-log (log)
  (close-mapping (mapping-stream-mapping (plog-stream log))))

;; -----------------------

(defparameter *follower* nil)

(defstruct (follower (:constructor %make-follower))
  thread 
  exit-p 
  log 
  output 
  tag
  (lvl '(:info :warning :error)))

(defun make-follower (log &key (stream *standard-output*) tag)
  "Make a follower for the log streeam specified. Will output the messages to the stream provided."
  (%make-follower :log (copy-log log :copy-stream t)
		  :output stream
		  :tag tag))

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
	    (when (and (member (log-message-lvl msg)
			       (follower-lvl follower))
		       (if (follower-tag follower)
			   (string-equal (follower-tag follower) (log-message-tag msg))
			   t))
	      (write-message-to-stream output msg)
	      (terpri output))
	    (when (>= (log-message-id msg) (1- new-id))
	      (setf done t))))
	(setf id new-id)))))

(defun start-following (log &key (stream *standard-output*) tag)
  "Start following the log."
  (setf *follower* (make-follower log :stream stream :tag tag))
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


  
