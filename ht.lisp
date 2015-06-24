;;;; Copyright (c) Frank James 2015 <frank.a.james@gmail.com>
;;;; This code is licensed under the MIT license.

;;; This file defines a simple open-addressed hash table, shared using
;;; a file mapping. It must be agreed beforehand what the block size is.
;;; It does no rehashing, so once it's getting
;;; full you are out of luck. Make sure you know how large you need it
;;; to be first.

;;; TODO: 
;;; * rehash when getting full. (how?)
;;; * need to cope with deletions, so that probing for items does not require walking over previouslt deleted entries. Essentially this means copying the block to the first deleted entry found on the probing walk.

(defpackage #:pounds.ht
  (:use #:cl #:pounds)
  (:export #:open-ht
	   #:close-ht 
	   #:find-hash
	   #:map-ht))

(in-package #:pounds.ht)

;; make them always a power of 2 
(defun choose-size (n)
  (expt 2 (truncate (log n 2))))

(defun read-header (stream)
  (list :count (nibbles:read-ub32/be stream)
        :fill (nibbles:read-ub32/be stream)
        :seqno (nibbles:read-ub32/be stream)))

(defun write-header (stream header)
  (nibbles:write-ub32/be (getf header :count) stream)
  (nibbles:write-ub32/be (getf header :fill) stream)
  (nibbles:write-ub32/be (getf header :seqno) stream))

(defun make-entry (key data) 
  (list :key key :data data))
(defun entry-key (entry)
  (getf entry :key))
(defun entry-data (entry)
  (getf entry :data))

(defstruct ht 
  mapping stream 
  count bsize fill seqno
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

(defconstant +default-count+ 64)
(defconstant +default-block-size+ 128)

(defun default-key-reader (stream)
  (let* ((len (nibbles:read-ub32/be stream))
	 (key (nibbles:make-octet-vector len)))
    (read-sequence key stream)))

(defun default-key-writer (stream key)
  (declare (type (vector (unsigned-byte 8)) key))
  (nibbles:write-ub32/be (length key) stream)
  (write-sequence key stream))

(defun open-ht (path reader writer &key count block-size key-reader key-writer test (quadratic-p t))
  "Open the hash table.
PATH ::= pathspec to the file.
READER, WRITER ::= functions to read/write the entries to the mapping.

Optional parameters:
COUNT ::= number of entries in the hash table.
BLOCK-SIZE ::= number of bytes each entry must fit within.
KEY-READER, KEY-WRITER ::= functions to read/write the key to the file. By default the key is assumed to be a vector of unsigned bytes.
TEST ::= function to compare two keys. Default is equalp.
QUADRATIC-P ::= if true, will search using quadratic probing, default it to use linear probing. Quadratic can improve search/insertion times and reduce clustering. Linear may improve performance.
"
  (unless count (setf count +default-count+))
  (setf count (choose-size count))
  (unless block-size (setf block-size +default-block-size+))

  (let* ((mapping (open-mapping path (* (1+ count) (or block-size +default-block-size+))))
         (stream (make-mapping-stream mapping)))
    ;; read the header and remap if needed
    (with-locked-mapping (stream)
      (file-position stream 0)
      (let ((header (read-header stream)))
        (cond
          ((zerop (getf header :count))
           ;; must be a new file 
           (file-position stream 0)
           (write-header stream (list :count count :seqno 0 :fill 0)))
          ((> count (getf header :count))
           ;; write the new header
           (setf (getf header :count) count)
           (file-position stream 0)
           (write-header stream header))
          ((< count (getf header :count))
           ;; remap
           (setf count (getf header :count))           
           (remap mapping (* block-size count))))
        (make-ht :mapping mapping
                 :stream stream
                 :count count 
                 :fill (getf header :fill)
                 :seqno (getf header :seqno)
                 :bsize (or block-size +default-block-size+)
                 :quadratic-p quadratic-p
                 :reader reader
                 :writer writer
                 :key-reader (or key-reader #'default-key-reader)
                 :key-writer (or key-writer #'default-key-writer)
                 :test (or test #'equalp))))))

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

(defun qprobe (hash i n &optional (c0 0) (c1 1/2) (c2 1/2))
  (mod (+ hash c0 (* c1 i) (* c2 i i)) n))
(defun lprobe (hash i n)
  (mod (+ hash i 1) n))
(defun probe (ht hash i)
  (declare (type ht ht))
  (if (ht-quadratic-p ht)
      (qprobe hash i (ht-count ht))
      (lprobe hash i (ht-count ht))))

(defun maybe-remap (ht)
  (declare (type ht ht))
  (file-position (ht-stream ht) 0)
  (let ((header (read-header (ht-stream ht))))
    (unless (= (getf header :count) (ht-count ht))
      (remap (ht-mapping ht) (* (getf header :count) (ht-bsize ht))))
    (setf (ht-count ht) (getf header :count)
          (ht-fill ht) (getf header :fill)
          (ht-seqno ht) (getf header :seqno))
    header))

(defun find-hash (ht key)
  "Find the entry named by the KEY. Returns the value stored for this key, or nil if not found."
  (declare (type ht ht))	   
  (let ((hash (hash32 (key-octets ht key))))
    ;; go to that entry and iterate until we find either a free entry or the entry with the key
    (with-locked-mapping ((ht-stream ht))
      ;; read the header to make sure it's not been remapped out-of-process
      (maybe-remap ht)
      (do ((i 1 (1+ i)))
          ((= i (ht-count ht)))
        (let* ((index (probe ht hash i))
               (offset (* (1+ index) (ht-bsize ht))))
          (file-position (ht-stream ht) offset)
          (let ((entry (read-entry (ht-stream ht) ht)))
            (unless entry (return-from find-hash nil))
            ;; compare the key, if equal then done
            (when (funcall (ht-test ht) (entry-key entry) key)
              (return-from find-hash (entry-data entry)))))))))

(defun (setf find-hash) (value ht key)
  "Store the value in the entry for this KEY. Use a value of NIL to remove this entry."
  (declare (type ht ht))
  (let ((hash (hash32 (key-octets ht key))))
    (with-locked-mapping ((ht-stream ht))
      (maybe-remap ht)
      (do ((i 1 (1+ i)))
          ((= i (ht-count ht)))
        (let* ((index (probe ht hash i))
               (offset (* (1+ index) (ht-bsize ht))))
          (file-position (ht-stream ht) offset)
          (let ((entry (read-entry (ht-stream ht) ht)))
            ;; check this entry has not been used, if not then write here and we're done
            ;; otherwise check the key, if it's equal then overwrite the data
            (when (and (null entry) value)
              ;; increment the seqno
              (incf (ht-seqno ht))
              (incf (ht-fill ht))
              (file-position (ht-stream ht) 0)
              (write-header (ht-stream ht) (list :count (ht-count ht) :fill (ht-fill ht) :seqno (ht-seqno ht)))

              (file-position (ht-stream ht) offset)
              (write-entry (ht-stream ht) ht (make-entry key value))
              (return-from find-hash value))
            ;; check the key 
            (when (funcall (ht-test ht) key (entry-key entry))
              ;; a match, overwrite the data and return
              (cond
                (value
                 ;; increment the seqno
                 (incf (ht-seqno ht))
                 (file-position (ht-stream ht) 0)
                 (write-header (ht-stream ht) (list :count (ht-count ht) :fill (ht-fill ht) :seqno (ht-seqno ht)))

                 (file-position (ht-stream ht) offset)
                 (write-entry (ht-stream ht) ht 
                              (make-entry key value)))
                (t 
                 ;; increment the seqno
                 (incf (ht-seqno ht))
                 (decf (ht-fill ht))
                 (file-position (ht-stream ht) 0)
                 (write-header (ht-stream ht) (list :count (ht-count ht) :fill (ht-fill ht) :seqno (ht-seqno ht)))

                 (file-position (ht-stream ht) offset)
                 (clear-entry (ht-stream ht))))
              (return-from find-hash value))))))))

(defun map-ht (function ht)
  "Like MAPHASH. Iterates over each entry in the hash table, executing the function on each (key value). Returns NIL."
  (declare (type function function)
           (type ht ht))
  (with-locked-mapping ((ht-stream ht))
    (maybe-remap ht)
    (do ((i 0 (1+ i)))
        ((= i (ht-count ht)))
      (let ((offset (* (1+ i) (ht-bsize ht))))
        (file-position (ht-stream ht) offset)
        (let ((entry (read-entry (ht-stream ht) ht)))
          (when entry 
            (funcall function (entry-key entry) (entry-data entry))))))))
