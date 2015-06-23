;;;; Copyright (c) Frank James 2015 <frank.a.james@gmail.com>
;;;; This code is licensed under the MIT license.

;;; This shows how to use the open-addressed hash table in POUNDS.HT.
;;; Basically the same as the example in db-test.lisp.

(defpackage #:ht-test  
  (:use #:cl #:pounds.ht))

(in-package #:ht-test)

(defstruct entry 
  age colour)

(defun encode-entry (stream entry)
  (nibbles:write-ub32/be (entry-age entry) stream)
  (nibbles:write-ub32/be 
   (ecase (entry-colour entry)
     (:blue 0)
     (:green 1)
     (:red 2))
   stream))

(defun decode-entry (stream)
  (let ((entry (make-entry)))
    (setf (entry-age entry) (nibbles:read-ub32/be stream))
    (let ((c (nibbles:read-ub32/be stream)))
      (setf (entry-colour entry)
            (ecase c 
              (0 :blue)
              (1 :green)
              (2 :red))))
    entry))

(defvar *the-path* (merge-pathnames "test.ht" (user-homedir-pathname)))
(defvar *the-ht* nil)


(defun key-reader (stream)
  (let ((len (nibbles:read-ub32/be stream)))
    (let ((octets (nibbles:make-octet-vector len)))
      (read-sequence octets stream)
      (babel:octets-to-string octets))))

(defun key-writer (stream string)
  (let ((octets (babel:string-to-octets string)))
    (nibbles:write-ub32/be (length octets) stream)
    (write-sequence octets stream)))

	
(defun test-open ()
  (setf *the-ht* 
        (open-ht *the-path* 32 
		 #'decode-entry #'encode-entry
		 :key-reader #'key-reader
		 :key-writer #'key-writer
		 :test #'string=)))


(defun find-person (name)
  (find-hash *the-ht* name))

(defun add-person (name age colour)
  (setf (find-hash *the-ht* name)
        (make-entry :age age :colour colour)))

(defun remove-person (name)
  (setf (find-hash *the-ht* name) nil))
                    
(defun list-persons ()
  (let (vals)
    (map-ht (lambda (k v)
	      (push (cons k v) vals))
	    *the-ht*)
    vals))



        


           
  
