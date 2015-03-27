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
  id 
  count
  size
  index)

(defun read-log-header (stream)
  (make-log-header 
   :id (nibbles:read-ub32/be stream)
   :count (nibbles:read-ub32/be stream)
   :size (nibbles:read-ub32/be stream)
   :index (nibbles:read-ub32/be stream)))

(defun write-log-header (stream header)
  (with-slots (id count size index) header
    (nibbles:write-ub32/be id stream)
    (nibbles:write-ub32/be count stream)
    (nibbles:write-ub32/be size stream)
    (nibbles:write-ub32/be index stream)))

(defconstant +msg-magic+ #x3D5B612E)

(defstruct log-message
  magic
  id
  lvl
  time
  tag
  msg)

(defun read-log-message (stream)
  (let ((magic (nibbles:read-ub32/be stream))
	(id (nibbles:read-ub32/be stream))
	(lvl (nibbles:read-ub32/be stream))
	(time (nibbles:read-ub64/be stream))
	(tag (let ((octets (nibbles:make-octet-vector 4)))
	       (read-sequence octets stream)
	       (flexi-streams:octets-to-string octets)))
	(len (nibbles:read-ub32/be stream)))
    (let ((octets (nibbles:make-octet-vector len)))
      (read-sequence octets stream)
      (make-log-message 
       :magic magic
       :id id
       :lvl (ecase lvl
	      (0 :debug)
	      (1 :info)
	      (2 :warning)
	      (4 :error))
       :time time
       :tag tag
       :msg (flexi-streams:octets-to-string octets)))))

(defun write-log-message (stream message)
  (with-slots (id lvl time msg) message
    (nibbles:write-ub32/be +msg-magic+ stream)
    (nibbles:write-ub32/be id stream)
    (nibbles:write-ub32/be (ecase lvl
			     (:debug 0)
			     (:info 1)
			     (:warning 2)
			     (:error 4))
			   stream)
    (nibbles:write-ub64/be time stream)
    (write-sequence (log-stream-tag stream) stream)
    (nibbles:write-ub32/be (length msg) stream)
    (write-sequence (flexi-streams:string-to-octets msg)
		    stream)))

;; need a circular block stream type
;; it should keep writing to blocks, allocating the next block
;; as it goes. it should wrap around to the beginning if it hits 
;; the end of file. should use the underlying mapping-stream type
;; to do the IO. this stream should merely be controlling the 
;; current mapping/block offset.

(defclass log-stream (trivial-gray-stream-mixin 
			   fundamental-binary-input-stream 
			   fundamental-binary-output-stream)
  (#+cmu
   (open-p :initform t
           :accessor log-stream-open-p
           :documentation "For CMUCL we have to keep track of this manually.")
   (stream :initarg :stream
	   :reader log-stream-stream
	   :documentation "The underlying mapping-stream")
   (header :reader log-stream-header
	   :initarg :header
	   :documentation "The log header")
   (tag :reader log-stream-tag
	:initarg :tag
	:documentation "the tag")
   (count :initarg :count 
	  :reader log-stream-count
	  :documentation "The numnber of block")
   (size :initarg :size
	 :reader log-stream-size
	 :documentation "The size of each block")))

#+:cmu
(defmethod open-stream-p ((stream log-stream))
  "Returns a true value if STREAM is open.  See ANSI standard."
  (log-stream-open-p stream))

#+:cmu
(defmethod close ((stream log-stream) &key abort)
  "Closes the stream STREAM.  See ANSI standard."
  (declare (ignore abort))
  (prog1
      (log-stream-open-p stream)
    (setf (log-stream-open-p stream) nil)))

(defun check-if-open-log (stream)
  "Checks if STREAM is open and signals an error otherwise."
  (unless (open-stream-p stream)
    (error "stream closed")))

(defmethod stream-element-type ((stream log-stream))
  "The element type is always OCTET by definition."
  '(unsigned-byte 8))

;; use this to check if there are more bytes to read
(defmethod stream-listen ((stream log-stream))
  "checks whether there are bytes left to read -- there are always more bytes to be read because this is a cicular buffer"
  (check-if-open-log stream)
  nil)

(defmethod stream-file-position ((stream log-stream))
  "Simply returns the index into the underlying vector."
  ;; the position of the stream is the position of the underlying mapping stream
  (stream-file-position (log-stream-stream stream)))

(defmethod (setf stream-file-position) (position-spec (stream log-stream))
  "Sets the index into the underlying vector if POSITION-SPEC is acceptable."
  (setf (stream-file-position (log-stream-stream stream))
	position-spec))

(defmethod stream-read-sequence ((stream log-stream) sequence start end &key)
  "Returns the index of last byte read."
  (declare (fixnum start end))
  (let ((count (- end start)))
    (do ((i 0 (1+ i)))
	((= i count))
      (setf (elt sequence (+ start i))
	    (read-byte stream)))
    count))
        
(defmethod stream-write-sequence ((stream log-stream) sequence start end &key)
  "Returns the index of last byte written."
  (declare (fixnum start end))
  (let ((count (- end start)))
    (do ((i 0 (1+ i)))
	((= i count))
      (write-byte (elt sequence (+ start i)) stream))
    sequence))
  
(defmethod stream-read-byte ((stream log-stream))
  "Returns the byte or :EOF. We NEVER return eof because we always wrap around."
  (let ((b (read-byte (log-stream-stream stream))))
    (when (listen (log-stream-stream stream))
      ;; we reached the eof, rewind to the start
      (file-position (log-stream-stream stream) 
		     (log-stream-size stream)))
    b))
  
(defmethod stream-write-byte ((stream log-stream) byte)
  "write the byte to the local buffer, flush it if at the end of the buffer"
  (let ((s (log-stream-stream stream)))
    (write-byte byte s)
    (when (listen s)
      (file-position s (log-stream-size stream))))
  byte)

(defmethod stream-finish-output ((stream log-stream))
  (finish-output (log-stream-stream stream)))

(defmethod stream-force-output ((stream log-stream))
  (force-output (log-stream-stream stream)))

(defun advance-to-next-block (stream)
  (declare (type log-stream stream))
  (let ((index (mapping-stream-position (log-stream-stream stream))))
    ;; set the index to the start of the next block
    ;; recall that the first block is reserved for the header
    (let ((new-index 
	   (multiple-value-bind (q r) (truncate index (log-stream-size stream))
	     (* (log-stream-size stream)
		(cond
		  ((= q (1- (log-stream-count stream)))
		   ;; we are in the final block -- move back to the start
		   1)
		  ((zerop r)
		   ;; we just happen to be at the start of a new block- -- do nothing
		   q)
		  (t 
		   ;; next block position
		   (1+ q)))))))
      (file-position (log-stream-stream stream) new-index)
      new-index)))

(defun advance-to-block (stream index)
  "Set the stream position to the block index"
  (declare (type log-stream stream))
  (let ((offset (* (log-stream-size stream) (1+ index))))
    (file-position (log-stream-stream stream) offset)
    offset))

(defun advance-to-start (log)
  "Reset the log position to the first message"
  (let ((index 0))
    (advance-to-block log 0)
    (do ((magic (nibbles:read-ub32/be log)
		(nibbles:read-ub32/be log)))
	((or (= magic +msg-magic+)
	     (= magic 0)))
      (incf index)
      (advance-to-next-block log))
    (advance-to-block log index)))

;; ------------

    
(defun make-log-stream (mapping-stream &key (size 512) tag)
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
	     (log-header-index header) size))
      ((not (= (log-header-size header) size))
       (error "log header size ~A does not match size ~A" 
	      (log-header-size header)
	      size)))

    ;; set the initial stream position
    (file-position mapping-stream 
		   (log-header-index header))

    ;; make the instance
    (let ((log (make-instance 'log-stream
			      :stream mapping-stream
			      :header header
			      :tag (let ((octets (flexi-streams:string-to-octets (or tag "DEBG"))))
				     (assert (= (length octets) 4))
				     octets)
			      :count count
			      :size size)))
      ;; write the header back to the file
      ;; this ensures it's initially written when we crated the file
      (write-header header log)
      log)))

(defun copy-log (log &key tag)
  "Make a copy of the log stream, possibly changing the log tag"
  (let ((header (read-header log)))
    (make-instance 'log-stream
		   :stream (make-instance 'mapping-stream
					  :mapping (mapping-stream-mapping 
						    (log-stream-stream log)))
		   :header header
		   :tag (if tag
			    (let ((octets (flexi-streams:string-to-octets (or tag "DEBG"))))
			      (assert (= (length octets) 4))
			      octets)
			    (log-stream-tag log))
		   :count (log-header-count header)
		   :size (log-header-size header))))
		 

(defun read-header (log)
  (let ((pos (file-position (log-stream-stream log))))
    (file-position (log-stream-stream log) 0)
    (prog1 (read-log-header (log-stream-stream log))
      (file-position (log-stream-stream log) pos))))

(defun write-header (header log)
  (let ((pos (file-position (log-stream-stream log))))
    (file-position (log-stream-stream log) 0)
    (prog1 (write-log-header (log-stream-stream log) header)
      (file-position (log-stream-stream log) pos))))

(defun write-message (log lvl format-control &rest args)
  "Write a log message"
  (write-log-message log 
		     (make-log-message :id (log-header-id (log-stream-header log))
				       :lvl lvl
				       :time (get-universal-time)
				       :tag (log-stream-tag log)
				       :msg (apply #'format nil format-control args)))
  (let ((index (advance-to-next-block log)))
    (incf (log-header-id (log-stream-header log)))
    (setf (log-header-index (log-stream-header log))
	  index)
    (write-header (log-stream-header log) log))
  nil)

(defun read-message (log)
  "Read the next message from the log"
  (let ((msg (read-log-message log)))       
    (advance-to-next-block log)
    msg))

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
	(let ((log (make-log-stream (make-mapping-stream map)
				    :size size
				    :tag tag)))
	  log)
      (error (e)
	(close-mapping map)
	(error e)))))
  
(defun close-log (log)
  (close-mapping (mapping-stream-mapping (log-stream-stream log))))

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
  (%make-follower :log (copy-log log)
		  :output stream
		  :tag tag))

(defun follow-log (follower)
  "Print the log messages to the output stream until the exit-p flag is signalled."
  (do ((log (follower-log follower))
       (id 0)
       (output (follower-output follower)))
      ((follower-exit-p follower))
    (sleep 1)
    (let* ((header (read-header log))
	   (new-id (1- (log-header-id header))))
      (when (< id new-id)
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
	    (when (>= (log-message-id msg) new-id)
	      (setf done t))))
	(setf id new-id)))))

(defun start-following (log &key (stream *standard-output*) tag)
  "Start following the log."
  (setf *follower* (make-follower log :stream stream :tag tag))
  (advance-to-start (follower-log *follower*))
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


  
