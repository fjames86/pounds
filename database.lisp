;;;; Copyright (c) Frank James 2015 <frank.a.james@gmail.com>
;;;; This code is licensed under the MIT license.

;;; This file defines a common usage pattern of mmaped files. You want to be able to store a collection
;;; of entries with operations of searching, adding and deleting them. This is implemented using a pounds file mapping. 


(defpackage #:pounds.db
  (:use #:cl #:pounds)
  (:export #:open-db
           #:close-db
           #:db-seqno

           ;; generic lookup/mapping functions 
           #:find-entry
	   #:remove-entry 
           #:find-entry-if
           #:mapentries))

(in-package #:pounds.db)


(defconstant +default-count+ 32)
(defconstant +default-block-size+ 128)

(defstruct db 
  mapping stream
  header
  bsize
  reader writer)

(defun read-header (stream)
  (declare (type stream stream))
  (file-position stream 0)
  (list :count (nibbles:read-ub32/be stream)
	:fill (nibbles:read-ub32/be stream)
	:seqno (nibbles:read-ub32/be stream)))

(defun write-header (stream header)
  (declare (type stream stream))
  (file-position stream 0)
  (nibbles:write-ub32/be (getf header :count) stream)
  (nibbles:write-ub32/be (getf header :fill) stream)
  (nibbles:write-ub32/be (getf header :seqno) stream))

(defun read-entry (stream reader)
  (declare (type stream stream)
	   (type function reader))
  (let ((active (nibbles:read-ub32/be stream)))
    (unless (zerop active)
      (funcall reader stream))))

(defun write-entry (stream writer obj)
  (declare (type stream stream)
	   (type function writer))
  (nibbles:write-ub32/be 1 stream)
  (funcall writer stream obj))

(defun open-db (pathspec reader writer &key (count +default-count+) (block-size +default-block-size+))
  "Open the database.
PATHSPEC ::= pathspec to the file.
READER ::= function \(stream\) which reads an entry.
WRITER ::= function \(stream entry\) which writes the entry to the stream.
COUNT ::= default initial number of entries.
BLOCK-SIZE ::= agreed block size. Must be at least 16.

Returns the database."
  (let* ((mapping (open-mapping pathspec (* count block-size)))
	 (stream (make-mapping-stream mapping)))
    (with-locked-mapping (stream)
      ;; read the header 
      (let ((header (read-header stream)))
	(cond
	  ((zerop (getf header :count))
	   ;; write the new header
	   (setf (getf header :count) count)
	   (write-header stream header))
	  ((< (getf header :count) count)
	   (setf (getf header :count) count)
	   (write-header stream header))
	  ((> (getf header :count) count)
	   (setf count (getf header :count))
	   (remap mapping (* count block-size))))
	(make-db :mapping mapping
		 :stream stream
		 :header header
		 :bsize block-size
		 :reader reader
		 :writer writer)))))

(defun close-db (db)
  "Close the database."
  (declare (type db db))
  (close-mapping (db-mapping db)))

(defun maybe-remap (db)
  (declare (type db db))
  (let ((header (read-header (db-stream db))))
    (unless (= (getf header :count) (getf (db-header db) :count))
      (remap (db-mapping db) (* (getf header :count) (db-bsize db))))
    (setf (db-header db) header)
    header))

(defmacro with-locked-db ((db) &body body)
  `(with-locked-mapping ((db-stream ,db))
     (maybe-remap ,db)
     ,@body))

(defun db-count (db)
  (declare (type db db))
  (getf (db-header db) :count))

(defun db-seqno (db)
  "Read the database sequence number."
  (declare (type db db))
  (with-locked-db (db)
    (getf (db-header db) :seqno)))

(defun find-entry (item db &key key test)
  "Search for the item in the database."
  (declare (type db db))
  (with-locked-db (db)
    (do ((i 1 (1+ i))
	 (stream (db-stream db)))
	((= i (db-count db)))
      (file-position stream (* i (db-bsize db)))
      (let ((entry (read-entry stream (db-reader db))))
	(when entry 
	  (when (funcall (or test #'eql)
			 item
			 (if key 
			     (funcall key entry)
			     entry))
	    (return-from find-entry entry)))))))

(defun (setf find-entry) (value item db &key key test)
  (declare (type db db))
  (with-locked-db (db)
    (do ((i 1 (1+ i))
	 (stream (db-stream db)))
	((= i (db-count db)))
      (file-position stream (* i (db-bsize db)))
      (let ((entry (read-entry stream (db-reader db))))
	(when (or (not entry)
		  (funcall (or test #'eql)
			   item
			   (if key 
			       (funcall key entry)
			       entry)))
	  ;; write the entry 
	  (file-position stream (* i (db-bsize db)))
	  (write-entry stream (db-writer db) value)
	  ;; update the header 
	  (incf (getf (db-header db) :fill))
	  (incf (getf (db-header db) :seqno))
	  (file-position stream 0)
	  (write-header stream (db-header db))
	  (return-from find-entry value))))
    ;; if we get here then there are no free entries. we need to grow and remap
    (let ((count (db-count db)))
      (setf (getf (db-header db) :count) (* count 2))
      (remap (db-mapping db) (* count (db-bsize db)))
      ;; write the entry 
      (file-position (db-stream db) (* count (db-bsize db)))
      (write-entry (db-stream db) (db-writer db) value)
      ;; update the header 
      (incf (getf (db-header db) :fill))
      (incf (getf (db-header db) :seqno))
      (file-position (db-stream db) 0)
      (write-header (db-stream db) (db-header db))
      value)))

(defun remove-entry (item db &key key test)
  "Delete the item from the database."
  (declare (type db db))
  (with-locked-db (db)
    (do ((i 1 (1+ i))
	 (stream (db-stream db)))
	((= i (db-count db)))
      (file-position stream (* i (db-bsize db)))
      (let ((entry (read-entry stream (db-reader db))))
	(when entry 
	  (when (funcall (or test #'eql)
			 item
			 (if key 
			     (funcall key entry)
			     entry))
	    (file-position stream (* i (db-bsize db)))
	    (nibbles:write-ub32/be 0 stream)
	    ;; update the header 
	    (decf (getf (db-header db) :fill))
	    (incf (getf (db-header db) :seqno))
	    (file-position (db-stream db) 0)
	    (write-header (db-stream db) (db-header db))      
	    (return-from remove-entry nil)))))))
  
(defun find-entry-if (predicate db &key key)
  "Find the item in the database."
  (declare (type db db)
	   (type function predicate))
  (with-locked-db (db)
    (do ((i 1 (1+ i))
	 (stream (db-stream db)))
	((= i (db-count db)))
      (file-position stream (* i (db-bsize db)))
      (let ((entry (read-entry stream (db-reader db))))
	(when entry 
	  (when (funcall predicate 
			 (if key 
			     (funcall key entry)
			     entry))
	    (return-from find-entry-if entry)))))))


(defun mapentries (function db)
  "Map over the entries in the database."
  (declare (type db db)
	   (type function function))
  (with-locked-db (db)
    (do ((i 1 (1+ i))
	 (stream (db-stream db))
	 (vals nil))
	((= i (db-count db)) (nreverse vals))
      (file-position stream (* i (db-bsize db)))
      (let ((entry (read-entry stream (db-reader db))))
	(when entry 
	  (push (funcall function entry) vals))))))



