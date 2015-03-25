;;;; Copyright (c) Frank James 2015 <frank.a.james@gmail.com>
;;;; This code is licensed under the MIT license.


(in-package #:pounds)


;; we want to define a circular log stream from the buffer
;; the operations should be to read and write a log structure to/from it

;; header that takes up the first block 
(defxstruct log-header ((:reader read-log-header) (:writer write-log-header))
  (id :uint32)
  (seqno :uint32)
  (count :uint32)
  (size :uint32)
  (index :uint32))

(defxenum log-level
  (:info 1)
  (:warning 2)
  (:error 4))

;; message that consumes a number of blocks 
(defxstruct log-message ((:reader read-log-message) (:writer write-log-message))
  (id :uint32) ;; msg id
  (lvl log-level :info)
  (time :uint64)
  (msg :string))

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
  (declare (type log-stream stream))
  (let ((offset (* (log-stream-size stream) (1+ index))))
    (file-position (log-stream-stream stream) offset)
    offset))

;; ------------

    
(defun make-log-stream (mapping-stream &key (size 512))
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
    (make-instance 'log-stream
		   :stream mapping-stream
		   :header header
		   :count count
		   :size size)))

(defun copy-log-stream (log)
  (let ((header (read-header log)))
    (make-instance 'log-stream
		   :stream (make-instance 'mapping-stream
					  :mapping (mapping-stream-mapping 
						    (log-stream-stream log)))
		   :header header
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
  (write-log-message log 
		     (make-log-message :id (log-header-id (log-stream-header log))
				       :lvl lvl
				       :time (get-universal-time)
				       :msg (apply #'format nil format-control args)))
  (let ((index (advance-to-next-block log)))
    (incf (log-header-id (log-stream-header log)))
    (incf (log-header-seqno (log-stream-header log)))
    (setf (log-header-index (log-stream-header log))
	  index)
    (write-header (log-stream-header log) log))
  nil)

(defun read-message (log)
  (prog1 (read-log-message log)
    (advance-to-next-block log)))

(defun write-message-to-stream (stream msg)
  (multiple-value-bind (seconds min hour day month year i1 i2 i3)
      (decode-universal-time (log-message-time msg))
    (declare (ignore i1 i2 i3))
    (format stream
	    "~A-~A-~A ~A:~A:~A ~A"
	    year month day hour min seconds
	    (log-message-msg msg))))


(defparameter *default-log-file* "pounds.log")
(defparameter *default-log* nil)

(defun open-log (&key path count size)
  (unless count (setf count (* 1024 16)))
  (unless size (setf size 128))
  (let ((map (open-mapping (or path *default-log-file*)
			   :size (* count size))))
    (make-log-stream (make-mapping-stream map)
		     :size size)))

(defun close-log (log)
  (close-mapping (mapping-stream-mapping (log-stream-stream log))))

(defmacro with-open-log ((var &key path count size) &body body)
  `(let ((,var (open-log :path ,path :count ,count :size ,size)))
     (unwind-protect (progn ,@body)
       (close-log ,var))))


;; -----------------------


(defstruct (follower (:constructor %make-follower))
  thread exit-p log output)

(defun make-follower (log &optional (output *standard-output*))
  (%make-follower :log log
		  :output output))

(defun follow-log (follower)
  (do ((seqno 0)
       (log (follower-log follower))
       (output (follower-output follower)))
      ((follower-exit-p follower))
    (sleep 1)
    (let* ((header (read-header log))
	   (id (1- (log-header-id header)))
	   (new-seqno (log-header-seqno header)))
      (when (< seqno new-seqno)
	(do ((msg (read-message log) (read-message log)))
	    ((or (zerop (log-message-id msg))
		 (>= (log-message-id msg) id)))
	  (write-message-to-stream output msg)
	  (terpri output))
	(setf seqno new-seqno)))))

(defun start-follower (follower)
  (setf (follower-exit-p follower)
	nil
	(follower-thread follower)
	(bt:make-thread (lambda ()
			  (follow-log follower)))))

(defun stop-follower (follower)
  (setf (follower-exit-p follower) t)
  (bt:join-thread (follower-thread follower))
  nil)


  
