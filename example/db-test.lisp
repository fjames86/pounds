

(defpackage #:db-test  
  (:use #:cl #:pounds.db))

(in-package #:db-test)

(defstruct entry 
  name age colour)

(defun encode-entry (stream entry)
  (let ((octets (babel:string-to-octets (entry-name entry))))
    (nibbles:write-ub32/be (length octets) stream)
    (write-sequence octets stream))
  (nibbles:write-ub32/be (entry-age entry) stream)
  (nibbles:write-ub32/be 
   (ecase (entry-colour entry)
     (:blue 0)
     (:green 1)
     (:red 2))
   stream))

(defun decode-entry (stream)
  (let ((entry (make-entry)))
    (let ((len (nibbles:Read-ub32/be stream)))
      (let ((v (nibbles:make-octet-vector len)))
        (read-sequence v stream)
        (setf (entry-name entry) (babel:octets-to-string v))))
    (setf (entry-age entry) (nibbles:read-ub32/be stream))
    (let ((c (nibbles:read-ub32/be stream)))
      (setf (entry-colour entry)
            (ecase c 
              (0 :blue)
              (1 :green)
              (2 :red))))
    entry))

(defvar *the-path* (merge-pathnames "test.db" (user-homedir-pathname)))
(defvar *the-db* nil)

(defun test-open ()
  (setf *the-db* 
        (open-db *the-path* #'decode-entry #'encode-entry)))


(defun find-person (name)
  (find-entry name *the-db*
              :test #'string-equal :key #'entry-name))

(defun add-person (name age colour)
  (setf (find-entry name *the-db*
                    :test #'string-equal :key #'entry-name)
        (make-entry :name name :age age :colour colour)))

(defun remove-person (name)
  (setf (find-entry name *the-db* 
                    :test #'string-equal :key #'entry-name)
        nil))
                    
(defun list-persons ()
  (mapentries #'identity *the-db*))


        


           
  
