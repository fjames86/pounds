;;;; Copyright (c) Frank James 2016 <frank.a.james@gmail.com>
;;;; This code is licensed under the MIT license.

;;; Example of how to use the binary log (blog).
;;; Usage:
;;; 1. Open the log, (OPEN-BLOG)
;;; 2. Start a thread which periodically polls the blog seqno and prints any new message to stream
;;; (START-FOLLOWING)
;;; 3. From the REPL, wite some messages to the log, (DOTIMES (I 100) (WRITE-ENTRY "Hello"))
;;; 4. Observe that the follower gets them and prints them out.
;;; 5. Stop following (STOP-FOLLOWING)
;;; 6. Close the blog (CLOSE-BLOG)

;;; Note: this requires the DrX package for serialization.

(defpackage #:blog-test
  (:use #:cl #:drx))

(in-package #:blog-test)


(defvar *blog* nil)

(defun open-blog ()
  (unless *blog*
    (setf *blog*
	  (pounds.blog:open-blog "blog.log"
				 :header-size 512
				 :block-size 64
				 :nblocks (* 16 1024)))))


(defun close-blog ()
  (when *blog*
    (pounds.blog:close-blog *blog*)
    (setf *blog* nil)))

;; define the entry structures
(defxstruct entry ()
  (name :string)
  (timestamp :uint64))

(defun read-entry (blk) 
  (reset-xdr-block blk)
  (multiple-value-bind (count id)
      (pounds.blog:read-entry *blog* 
			      (xdr-block-buffer blk) 
			      :start 0 
			      :end (xdr-block-count blk))
    (setf (xdr-block-count blk) count)
    (let ((e (decode-entry blk)))
      (multiple-value-bind (sec min hour date month year) (decode-universal-time (entry-timestamp e))
	(format t "~A-~A-~A ~A:~A:~A ~A ~A~%" 
		year month date hour min sec
		id 
		(entry-name e))))))

(defun write-entry (name)
  (let ((e (make-entry :name name 
		       :timestamp (get-universal-time)))
	(blk (xdr-block 512)))
    (encode-entry blk e)
    (pounds.blog:write-entry *blog*
			     (xdr-block-buffer blk)
			     :start 0 :end (xdr-block-offset blk))))

(defvar *exiting* nil)
(defvar *thread* nil)

(defun follow (stream)
  (do ((blk (xdr-block 512))
       (props (pounds.blog:blog-properties *blog*)))
      (*exiting*)
    (sleep 1)
    (let ((p (pounds.blog:blog-properties *blog*)))
      (unless (= (getf p :seqno) (getf props :seqno))
	;; messages to read 
	(do ((id (getf props :id) (1+ id)))
	    ((= id (getf p :id)))
	  (let ((*standard-output* stream))
	    (read-entry blk)))
	(setf props p)))))

(defun start-following (&optional (stream *standard-output*))
  (setf *exiting* nil)
  (setf *thread* (bt:make-thread (lambda () (follow stream)))))

(defun stop-following ()
  (setf *exiting* t)
  (bt:join-thread *thread*)
  (setf *thread* nil))



    
