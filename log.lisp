;;;; Copyright (c) Frank James 2015 <frank.a.james@gmail.com>
;;;; This code is licensed under the MIT license.


(in-package #:pounds)


;; we want to define a circular log stream from the buffer
;; the operations should be to read and write a log structure to/from it

;; header that takes up the first block 
(defxstruct log-header ((:reader read-log-header) (:writer write-log-header))
  (id :uint32)
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
	 :documentation "The size of each block")
   (index :initform 0 
	  :accessor log-stream-index
	  :documentation "Stores the current block index")
   (offset :initform 0
	   :accessor log-stream-offset
	   :documentation "Stores the current offset of the current block")))

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
      (file-position (log-stream-stream stream) new-index))))


;; ------------

    
(defun make-log-stream (mapping-stream &key (size 512))
  (file-position mapping-stream 0)
  (let ((map (mapping-stream-mapping mapping-stream))
	(header (read-log-header mapping-stream)))
    ;; validate the header information
    (cond
      ((zerop (log-header-size header))
       ;; zero size, must be a new header
       (setf (log-header-size header) size))
      ((= (log-header-size header) size)
       (error "log header size ~A does not match size ~A" 
	      (log-header-size header)
	      size)))
    ;; set the initial stream position
    (file-position mapping-stream 
		   (* (1+ (log-header-index header))
			  size))
    ;; make the instance
    (make-instance 'log-stream
		   :stream mapping-stream
		   :header header
		   :count (truncate (mapping-size map) size)
		   :size size)))

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
  (advance-to-next-block log)
  (incf (log-header-id (log-stream-header log)))
  (write-header (log-stream-header log) log)
  nil)

(defun read-message (log)
  (prog1 (read-log-message log)
    (advance-to-next-block log)))
