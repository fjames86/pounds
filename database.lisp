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
           #:find-entry-if
           #:mapentries

           ;; macro support for advanced usage
           #:doentries
           #:clear-entry
           #:store-entry))


(in-package #:pounds.db)

;; header: 
;; count 
;; seqno
;; ... user data ...

;; entry:
;; active
;; next 
;; ... user data ...

(defstruct db 
  mapping stream 
  count bsize
  reader writer)
;;  header-reader header-writer)

(defconstant +default-count+ 32)
(defconstant +default-block-size+ 128)

(defun close-db (db)
  "Close the database."
  (close-mapping (db-mapping db)))

(defun read-header (stream)
  (file-position stream 0)
  (list :count (nibbles:read-ub32/be stream)
        :seqno (nibbles:read-ub32/be stream)))

(defun inc-seqno (db)
  (let ((stream (db-stream db)))
    (file-position stream 0)
    (let ((header (read-header stream)))
      (file-position stream 4)
      (nibbles:write-ub32/be (1+ (getf header :seqno)) stream))))

(defun open-db (path reader writer &key count block-size)
  "Open the database.

PATH ::= pathspec

READER ::= function accepting argument (stream) that should read an entry from the database.
WRITER ::= function accepting argument (stream obj) that should write an entry to the database.

COUNT ::= default initial count of blocks.
BLOCK-SIZE ::= the block size. All entries MUST fit within this.

Returns the database object. This should be closed with a call to CLOSE-DB when finished with."
  (unless count (setf count +default-count+))
  (unless block-size (setf block-size +default-block-size+))
  (let ((mapping (open-mapping path (* count block-size))))
    (let ((db (make-db :mapping mapping
                       :stream (make-mapping-stream mapping)
                       :count count
                       :bsize block-size
                       :reader reader
                       :writer writer)))
      ;; ensure the count is correct 
      (let ((header (read-header (db-stream db))))
        (cond
          ((zerop (getf header :count))
           ;; new file, write initial header
           (file-position (db-stream db) 0)
           (nibbles:write-ub32/be count (db-stream db))
           (nibbles:write-ub32/be 0 (db-stream db)))
          ((< (getf header :count) count)
           ;; write the new count to the file
           (file-position (db-stream db) 0)
           (nibbles:write-ub32/be count (db-stream db)))
          ((> (getf header :count) count)
           ;; really should be mapped larger than this, remap
           (close-db db)
           (open-db path reader writer 
                    :count (getf header :count)
                    :block-size block-size))))
      db)))

(defun db-seqno (db)
  "Read the current sequence number from the database."
  (declare (type db db))
  (let ((header 
         (pounds:with-locked-mapping ((db-stream db))
           (read-header (db-stream db)))))
    (getf header :seqno)))

(defun maybe-remap (db)
  ;; read the header, remap if necessary 
  (pounds:with-locked-mapping ((db-stream db))
    (let ((header (read-header (db-stream db))))
      (unless (= (getf header :count) (db-count db))
        (remap (db-mapping db) (* (db-bsize db) (getf header :count)))
        (setf (db-count db) (getf header :count))))))

(defun read-entry (stream reader)
  (let ((active (nibbles:read-ub32/be stream)))
    (if (zerop active)
        nil
        (funcall reader stream))))

(defun write-entry (stream writer obj)
  (nibbles:write-ub32/be 1 stream)
  (funcall writer stream obj))

(defmacro doentries ((var db &optional return-value) &body body)
  "Iterate over each entry. VAR will be bound to the value of decoding the entry, or nil if no entry is in this block. The body is executed in a context where you can call

CLEAR-ENTRY, which deletes the current entry
STORE-ENTRY entry, which writes the new entry in this location.
"
  (alexandria:with-gensyms (gdb gi gstream)
    `(let ((,gdb ,db))
       (maybe-remap ,gdb)
       (pounds:with-locked-mapping ((db-stream ,gdb))
         (do ((,gi 1 (1+ ,gi))
              (,gstream (db-stream ,gdb)))
             ((= ,gi (db-count ,gdb)) ,return-value)
           (macrolet ((clear-entry ()
                        `(progn
                           (file-position ,',gstream (* ,',gi (db-bsize ,',gdb)))
                           (nibbles:write-ub32/be 0 ,',gstream)
                           (inc-seqno ,',gdb)))
                      (store-entry (entry)
                        `(progn
                           (file-position ,',gstream (* ,',gi (db-bsize ,',gdb)))
                           (write-entry ,',gstream (db-writer ,',gdb) ,entry)
                           (inc-seqno ,',gdb))))
             (file-position ,gstream (* ,gi (db-bsize ,gdb)))
             (let ((,var (read-entry ,gstream (db-reader ,gdb))))
               ,@body)))))))

(defun mapentries (function db)
  "Map over the entries in the database, applying FUNCTION to each entry."
  (declare (type db db))
  (let (vals)
    (doentries (entry db vals)
      (when entry 
        (push (funcall function entry) vals)))))

(defun find-entry (value db &key test key)
  "Lookup the entry named by VALUE."
  (declare (type db db))
  (doentries (entry db)
    (when entry 
      (if (funcall (or test #'eql)
                   (if key 
                       (funcall key entry)
                       entry)
                   value)
          (return-from find-entry entry))))
  nil)


(defun (setf find-entry) (new-entry value db &key test key)
  "Set the entry. Use a value of NIL to clear the entry."
  (declare (type db db))
  (doentries (entry db)
    (cond
      (entry 
       ;; overwrite if necessary 
       (if new-entry 
           (when (funcall (or test #'eql)
                          (if key 
                              (funcall key entry)
                              entry)
                          value)
             (store-entry new-entry)
             (return-from find-entry))
           (progn (clear-entry)
                  (return-from find-entry))))
      (new-entry
       ;; unused entry, store here
       (store-entry new-entry)
       (return-from find-entry))))
  ;; no free entries, remap to larger size if adding
  (when new-entry
    (pounds:with-locked-mapping ((db-stream db))
      (remap (db-mapping db) (* (db-count db) 2))
      ;; store new entry
      (file-position (db-stream db) (db-count db))
      (write-entry (db-stream db) (db-writer db) new-entry)
      ;; update the count
      (setf (db-count db) (* (db-count db) 2))
      nil)))
     

(defun find-entry-if (predicate db &key key)
  (declare (type db db))
  (doentries (entry db)
    (when entry
      (if (funcall predicate
                   (if key 
                       (funcall key entry)
                       entry))
          (return-from find-entry-if entry))))
  nil)

(defun (setf find-entry-if) (new-value predicate db &key key)
  (declare (type db db))
  (doentries (entry db)
    (when entry 
      (when (funcall predicate (if key 
                                   (funcall key entry)
                                   entry))
        (cond
          (new-value 
           (store-entry new-value)
           (return-from find-entry-if))
          (t 
           (clear-entry)
           (return-from find-entry-if))))))
  ;; no free entries, remap to larger size if adding
  (when new-value
    (pounds:with-locked-mapping ((db-stream db))
      (remap (db-mapping db) (* (db-count db) 2))
      ;; store new entry
      (file-position (db-stream db) (db-count db))
      (write-entry (db-stream db) (db-writer db) new-value)
      ;; update the count
      (setf (db-count db) (* (db-count db) 2))))
  nil)
