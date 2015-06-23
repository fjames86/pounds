;;;; Copyright (c) Frank James 2015 <frank.a.james@gmail.com>
;;;; This code is licensed under the MIT license.

;;; This file defines a simple open-addressed hash table, shared using
;;; a file mapping. It must be agreed beforehand what the block size and 
;;; count of entries is. It does no rehashing, so once it's getting
;;; full you are out of luck. Make sure you know how large you need it
;;; to be first.

(defpackage #:pounds.ht
  (:use #:cl #:pounds)
  (:export #:open-ht
	   #:close-ht 
	   #:find-hash
	   #:map-ht))

(in-package #:pounds.ht)

(defparameter *primes*
  '(23 59 83 163 191 227 293 349 487 599 677 773 859 967 1283 1571 1877 2297 2677 2909
    3709 4079 4513 4813 5237 5689 6659 7621 7919))

(defun choose-prime (n)
  (when (<= n (car *primes*))
    (return-from choose-prime (car *primes*)))
  (do ((primes *primes* (cdr primes)))
      ((null primes) nil)
    (when (and (> n (car primes)) (cadr primes) (<= n (cadr primes)))
      (return-from choose-prime (cadr primes)))))

(defun make-entry (key data) 
  (list :key key :data data))
(defun entry-key (entry)
  (getf entry :key))
(defun entry-data (entry)
  (getf entry :data))

(defstruct ht 
  mapping stream 
  count bsize
  reader writer
  key-reader key-writer test
  quadratic-p)

(defun read-entry (stream ht)
  (declare (type stream stream)
	   (type ht ht))
  (let ((active (nibbles:read-ub32/be stream)))
    (when (zerop active) (return-from read-entry nil)))
  (make-entry (funcall (ht-key-reader ht) stream)
	      (funcall (ht-reader ht) stream)))

(defun write-entry (stream ht entry)
  (declare (type stream stream)
	   (type ht ht))
  (nibbles:write-ub32/be 1 stream)
  (funcall (ht-key-writer ht) stream (entry-key entry))
  (funcall (ht-writer ht) stream (entry-data entry)))

(defun clear-entry (stream)
  (declare (type stream stream))
  (nibbles:write-ub32/be 0 stream))

(defconstant +default-block-size+ 128)


(defun default-key-reader (stream)
  (let* ((len (nibbles:read-ub32/be stream))
	 (key (nibbles:make-octet-vector len)))
    (read-sequence key stream)))

(defun default-key-writer (stream key)
  (declare (type (vector (unsigned-byte 8)) key))
  (nibbles:write-ub32/be (length key) stream)
  (write-sequence key stream))

(defun open-ht (path count reader writer &key block-size key-reader key-writer test quadratic-p)
  "Open the hash table.
PATH ::= pathspec to the file.
COUNT ::= number of entries in the hash table.
READER, WRITER ::= functions to read/write the entries to the mapping.

Optional parameters:
BLOCK-SIZE ::= number of bytes each entry must fit within.
KEY-READER, KEY-WRITER ::= functions to read/write the key to the file. By default the key is assumed to be a vector of unsigned bytes.
TEST ::= function to compare two keys. Default is equalp.
QUADRATIC-P ::= if true, will search using quadratic probing, default it to use linear probing. Quadratic can improve search/insertion times and reduce clustering. Linear may improve performance.
"
  (let ((prime (or (choose-prime count) (error "No prime for ~A" count))))
    (let ((mapping (open-mapping path (* prime (or block-size +default-block-size+)))))
      (make-ht :mapping mapping
	       :stream (make-mapping-stream mapping)
	       :count prime
	       :bsize (or block-size +default-block-size+)
	       :quadratic-p quadratic-p
	       :reader reader
	       :writer writer
	       :key-reader (or key-reader #'default-key-reader)
	       :key-writer (or key-writer #'default-key-writer)
	       :test (or test #'equalp)))))

(defun close-ht (ht)
  "Close the hash table."
  (declare (type ht ht))
  (close-mapping (ht-mapping ht)))

;; -------------------------------------

(defun mod32 (x)
  (mod x (expt 2 32)))

(defun hash32 (octets)
  (do ((hash 0)
       (i 0 (1+ i)))
      ((= i (length octets)) 
       (progn
	 (setf hash (mod32 (+ hash (ash hash 3))))
	 (setf hash (mod32 (logxor hash (ash hash -11))))
	 (setf hash (mod32 (+ hash (ash hash 15))))
	 hash))
    (setf hash (mod32 (+ hash (aref octets i))))
    (setf hash (mod32 (+ hash (ash hash 10))))
    (setf hash (mod32 (logxor hash (ash hash -6))))))
	
(defun key-octets (ht key)
  (flexi-streams:with-output-to-sequence (s) 
    (funcall (ht-key-writer ht) s key)))

(defun find-hash (ht key)
  "Find the entry named by the KEY. Returns the value stored for this key, or nil if not found."
  (declare (type ht ht))	   
  (let ((hash (hash32 (key-octets ht key))))
    ;; go to that entry and iterate until we find either a free entry or the entry with the key
    (with-locked-mapping ((ht-stream ht))
      (do ((start nil t)
	   (diff 1 (* diff 2))
	   (offset (* (ht-bsize ht) (mod hash (ht-count ht)))
		   (* (ht-bsize ht)
		      (mod (if (ht-quadratic-p ht)
			       (+ offset diff)
			       (1+ offset))
			   (ht-count ht)))))
	  ((and start (= (* (ht-bsize ht) (mod hash (ht-count ht))) offset)))
	(file-position (ht-stream ht) offset)
	(let ((entry (read-entry (ht-stream ht) ht)))
	  (unless entry (return-from find-hash nil))
	  ;; compare the key, if equal then done
	  (when (funcall (ht-test ht) (entry-key entry) key)
	    (return-from find-hash (entry-data entry))))))))

(defun (setf find-hash) (value ht key)
  "Store the value in the entry for this KEY. Use a value of NIL to remove this entry."
  (declare (type ht ht))
  (let ((hash (hash32 (key-octets ht key))))
    (with-locked-mapping ((ht-stream ht))
      (do ((start nil t)
	   (diff 1 (* diff 2))
	   (offset (* (ht-bsize ht) 
		      (mod hash (ht-count ht)))
		   (* (ht-bsize ht)
		      (mod (if (ht-quadratic-p ht)
			       (+ offset diff)
			       (1+ offset))
			   (ht-count ht)))))
	  ((and start (= (* (ht-bsize ht) (mod hash (ht-count ht))) offset))
	   (error "No free entries"))
	(file-position (ht-stream ht) offset)
	(let ((entry (read-entry (ht-stream ht) ht)))
	  ;; check this entry has not been used, if not then write here and we're done
	  ;; otherwise check the key, if it's equal then overwrite the data
	  (unless entry 
	    (file-position (ht-stream ht) offset)
	    (write-entry (ht-stream ht) ht (make-entry key value))
	    (return-from find-hash value))
	  ;; check the key 
	  (when (funcall (ht-test ht) key (entry-key entry))
	    ;; a match, overwrite the data and return
	    (cond
	      (value
	       (file-position (ht-stream ht) offset)
	       (write-entry (ht-stream ht) ht 
			    (make-entry key value)))
	      (t 
	       (file-position (ht-stream ht) offset)
	       (clear-entry (ht-stream ht))))
	    (return-from find-hash value)))))))

(defun map-ht (function ht)
  "Like MAPHASH. Iterates over each entry in the hash table, executing the function on each (key value). Returns NIL."
  (declare (type function function)
	   (type ht ht))
  (with-locked-mapping ((ht-stream ht))
    (do ((offset 0 (+ offset (ht-bsize ht))))
	((= offset (* (ht-count ht) (ht-bsize ht))))
      (file-position (ht-stream ht) offset)
      (let ((entry (read-entry (ht-stream ht) ht)))
	(when entry 
	  (funcall function (entry-key entry) (entry-data entry)))))))
