;;;; Copyright (c) Frank James 2015 <frank.a.james@gmail.com>
;;;; This code is licensed under the MIT license.

(in-package #:pounds)

;; implements a stream class to read from and write to the file mapping 

(defclass mapping-stream (trivial-gray-stream-mixin 
			   fundamental-binary-input-stream 
			   fundamental-binary-output-stream)
  (#+cmu
   (open-p :initform t
           :accessor mapping-stream-open-p
           :documentation "For CMUCL we have to keep track of this manually.")
   (mapping :initarg :mapping 
	    :reader mapping-stream-mapping)
   (position :initform 0 
	     :accessor mapping-stream-position
	     :documentation "Stores the current position in the remote file.")))

#+:cmu
(defmethod open-stream-p ((stream mapping-stream))
  "Returns a true value if STREAM is open.  See ANSI standard."
  (mapping-stream-open-p stream))

#+:cmu
(defmethod close ((stream mapping-stream) &key abort)
  "Closes the stream STREAM.  See ANSI standard."
  (declare (ignore abort))
  (prog1
      (mapping-stream-open-p stream)
    (setf (mapping-stream-open-p stream) nil)))

(defun check-if-open (stream)
  "Checks if STREAM is open and signals an error otherwise."
  (unless (open-stream-p stream)
    (error "stream closed")))

(defmethod stream-element-type ((stream mapping-stream))
  "The element type is always OCTET by definition."
;;  (declare #.*standard-optimize-settings*)
  '(unsigned-byte 8))

;; use this to check if there are more bytes to read
(defmethod stream-listen ((stream mapping-stream))
  "checks whether there are bytes left to read"
  (check-if-open stream)
  (>= (mapping-stream-position stream)
      (mapping-size (mapping-stream-mapping stream))))

(defmethod stream-file-position ((stream mapping-stream))
  "Simply returns the index into the underlying vector."
  (mapping-stream-position stream))

(defmethod (setf stream-file-position) (position-spec (stream mapping-stream))
  "Sets the index into the underlying vector if POSITION-SPEC is acceptable."
  (setf (mapping-stream-position stream)
	(case position-spec
	  (:start 0)
	  (:end (mapping-size (mapping-stream-mapping stream)))
	  (otherwise 
	   (unless (integerp position-spec) (error "Must be integer"))
	   position-spec)))
  (mapping-stream-position stream))

(defmethod stream-read-sequence ((stream mapping-stream) sequence start end &key)
  "Returns the index of last byte read."
  (declare (fixnum start end))
  (read-mapping-block sequence 
		      (mapping-stream-mapping stream) 
		      (mapping-stream-position stream)
		      :start start
		      :end end)
  (incf (mapping-stream-position stream) (- end start))
  (mapping-stream-position stream))
        
(defmethod stream-write-sequence ((stream mapping-stream) sequence start end &key)
  "Returns the index of last byte written."
  (declare (fixnum start end))
  (write-mapping-block sequence 
		       (mapping-stream-mapping stream) 
		       (mapping-stream-position stream)
		       :start start
		       :end end)
  (incf (mapping-stream-position stream) (- end start))
  (mapping-stream-position stream)
  sequence)
  
(defmethod stream-read-byte ((stream mapping-stream))
  "Returns the byte or :EOF"
  ;; if the buffer has been completely read then refill it 
  (cond
    ((= (mapping-stream-position stream)
	(mapping-size (mapping-stream-mapping stream)))
     :eof)
    (t
      (let ((b (mem-aref (mapping-ptr (mapping-stream-mapping stream)) :uint8 
			 (mapping-stream-position stream))))
	(incf (mapping-stream-position stream))
	b))))
  
(defmethod stream-write-byte ((stream mapping-stream) byte)
  "write the byte to the local buffer, flush it if at the end of the buffer"
  (setf (mem-aref (mapping-ptr (mapping-stream-mapping stream)) :uint8
		  (mapping-stream-position stream))
	byte)
  (incf (mapping-stream-position stream)))

(defmethod stream-finish-output ((stream mapping-stream))
  (flush-buffers (mapping-stream-mapping stream)))

(defmethod stream-force-output ((stream mapping-stream))
  (flush-buffers (mapping-stream-mapping stream)))

;; ------------------------------------------



(defun make-mapping-stream (mapping)
  (make-instance 'mapping-stream 
		 :mapping mapping))

(defmacro with-mapping-stream ((var mapping) &body body)
  `(let ((,var (make-mapping-stream ,mapping)))
     ,@body))

(defmacro with-mapping ((var filename size) &body body)
  `(let ((,var (open-mapping ,filename ,size)))
     (unwind-protect (progn ,@body)
       (close-mapping ,var))))

(defmacro with-open-mapping ((var filename size) &body body)
  "Evaluate the body in the contect of a mapping stream."
  (let ((gmapping (gensym "MAPPING")))
    `(with-mapping (,gmapping ,filename ,size)
       (with-mapping-stream (,var ,gmapping)
	 ,@body))))

(defmacro with-locked-mapping ((mapping-stream) &body body)
  "Evaluate the body with the mapping lock held."
  (let ((gstream (gensym)))
    `(let ((,gstream ,mapping-stream))
       (bt:with-lock-held ((mapping-lock (mapping-stream-mapping ,gstream)))
	 (lock-mapping (mapping-stream-mapping ,gstream))
	 (unwind-protect (progn ,@body)
	   (unlock-mapping (mapping-stream-mapping ,gstream)))))))


