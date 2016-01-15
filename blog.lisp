;;;; Copyright (c) Frank James 2016 <frank.a.james@gmail.com>
;;;; This code is licensed under the MIT license.

;;; This files defines a circular log, much like the one defined in log.lisp
;;; The difference here is that the entries are not debug logging strings
;;; but binary messages.
;;; The file is divided up into three sections
;;; 1. a fixed-sized header containing the general log properties. This consumes
;;; a region of +block+ bytes (512 bytes).
;;; 2. a variable sized region of memory the user may access for any purpose.
;;; 3. an array of n blocks of size +block+ (512 bytes) each.
;;;
;;; When the log is initialized it is assigned a unique tag (current time) and
;;; a sequence number. Any write into the log increments the sequence number.
;;; When a user writes a message into the log, it will consume as many blocks
;;; as required to fit it. The new message is always written at the end,
;;; possibly overwriting an old message in that location. 
;;; When a user reads a message from the log, it is read from the current
;;; location (block index) stored in the currently stored blog properties.
;;; 
;;; Typically one process will be writing into the log and another reading
;;; from it.
;;; PROC1:
;;; (write-entry *blog* buffer :start 0 :end 128)
;;; -> 123
;;; PROC2:
;;; (read-entry *blog* buffer)
;;; -> (128 123 3661763585)
;;; READ-ENTRY returns (values count id timestamp)
;;;
;;; There is no way of blocking readers until new messages arrive,
;;; the only option is to periodically poll for seqno changes.

;;; Requires the DrX system which provides XDR serialization.

;;; TODO: allow the block size to be a user-specified size. We might need much larger blocks e.g. 4MB 
;;; We currently use a lot of space for entry headers (16 bytes out of 512) which would be reduced
;;; if the user needs larger blocks.


(defpackage #:pounds.blog
  (:use #:cl #:pounds #:drx)
  (:export #:open-blog
	   #:close-blog
	   #:reset-blog
	   #:sync-blog 

	   ;; reading 
	   #:read-entry
	   #:read-entry-details
	   #:read-entries

	   ;; writing 
	   #:write-entry 
	   
	   ;; user accessible header 
	   #:read-header
	   #:write-header
	   
	   ;; read properties 
	   #:blog-properties))
	   
(in-package #:pounds.blog)

;; defines a binary log i.e. very similar to the pounds.log
;; but for arbitrary octet vectors

;; layout:
;; fixed-size header properties (sized to 1 block)
;; variable size user header
;; data block 0
;; data block 1
;; ...
;; data block n - 1

;; divided up into 512 byte blocks

(defconstant +block+ 512)
(defconstant +block-data+ 496) ;; 16 byte header

(defconstant +blog-version+ 1)

(defxstruct props ()
  (version :uint32)
  (nblocks :uint32)
  (id :uint32)
  (index :uint32)
  (tag :uint32)
  (seqno :uint32)
  (header-size :uint32))

(defun read-blog-props (stream)
  (let ((blk (xdr-block +block+)))
    (read-sequence (xdr-block-buffer blk) stream)
    (decode-props blk)))

(defun write-blog-props (stream props)
  (let ((blk (xdr-block +block+)))
    (encode-props blk props)
    (write-sequence (xdr-block-buffer blk) stream
		    :end (xdr-block-offset blk))))

(defstruct blog
  mapping
  stream
  props)

(defun open-blog (pathspec &key (nblocks 1024) (header-size 0))
  "Open the binary log.
PATHSPEC ::= path to the file.
COUNT ::= number of blocks in the log."

  ;; total file size is properties (1 block), user header and count blocks
  (let ((mapping (open-mapping pathspec
			       (+ +block+ header-size (* nblocks +block+)))))
    (handler-bind ((error (lambda (e)
			    (declare (ignore e))
			    (close-mapping mapping))))
      (let ((blog (make-blog :mapping mapping
			     :stream (make-mapping-stream mapping))))
	;; start by reading the properties
	(file-position (blog-stream blog) 0)
	(setf (blog-props blog) (read-blog-props (blog-stream blog)))

	;; if the props are empty then assume just created
	(let ((props (blog-props blog)))
	  (cond
	    ((zerop (props-nblocks props))
	     (setf (props-version props) +blog-version+
		   (props-nblocks props) nblocks
		   (props-id props) 0
		   (props-tag props) (logand (get-universal-time) #xffffffff)
		   (props-seqno props) 0
		   (props-index props) 0
		   (props-header-size props) header-size)
	     (file-position (blog-stream blog) 0)
	     (write-blog-props (blog-stream blog) (blog-props blog)))
	    ((not (= (props-version props) +blog-version+))
	     (error "file version ~A mismatch (expected ~A)" 
		    (props-version props) +blog-version+))
	    ((not (= (props-nblocks props) nblocks))
	     (error "nblocks mismatch (expected ~A)" (props-nblocks props)))
	    ((not (= (props-header-size props) header-size))
	     (error "Header size mismatch (expected ~A)" (props-header-size props)))))
	
	blog))))
    
(defun close-blog (blog)
  "Close the binary log and free all resources."
  (close-mapping (blog-mapping blog))
  (setf (blog-mapping blog) nil
	(blog-stream blog) nil)
  nil)

(declaim (ftype (function (blog (vector (unsigned-byte 8)) &key (:start integer) (:end (or null integer))) *)
		read-header))
(defun read-header (blog sequence &key (start 0) end)
  "Read the user header data.
BLOG ::= binary log
SEQUENCE ::= octet vector
START, END ::= region of sequence to read into."
  (declare (type blog blog)
	   (type (vector (unsigned-byte 8)) sequence)
	   (type integer start)
	   (type (or null integer) end)) 

  ;; adujust the end pointer so that it is always within the header-size bounds 
  (let ((hs (props-header-size (blog-props blog))))
    (unless end (setf end (length sequence)))

    (when (> end (+ start hs))
      (setf end (+ start hs)))

    (let ((stream (blog-stream blog)))
      (with-locked-mapping (stream)
	(file-position stream +block+)
	(read-sequence sequence stream :start start :end end)))))

(declaim (ftype (function (blog (vector (unsigned-byte 8)) &key (:start integer) (:end (or null integer))) *)
		write-header))
(defun write-header (blog sequence &key (start 0) end)
  "Write the user header data. 
BLOG ::= binary log
SEQUENCE ::= octet vector
START, END ::= region of sequence to write.

Note that the header is a free-access region with no record of 
how much of the allocated space (if any) is actually in use."
  (declare (type blog blog)
	   (type (vector (unsigned-byte 8)) sequence)
	   (type integer start)
	   (type (or null integer) end))

  ;; check the header size 
  (unless end (setf end (length sequence)))
  (unless (<= (- end start) (props-header-size (blog-props blog)))
    (error "Attempt to write ~A bytes into header of size ~A" 
	   (- end start)
	   (props-header-size (blog-props blog))))

  (let ((stream (blog-stream blog)))
    (with-locked-mapping (stream)
      (file-position stream +block+)
      (write-sequence sequence stream :start start :end end)

      ;; increment the seqno
      (file-position stream 0)
      (let ((props (read-blog-props stream)))
	(incf (props-seqno props))
	(file-position stream 0)
	(write-blog-props stream props))))

  (- end start))

(defun blog-properties (blog)
  "Read the current blog properties."
  (declare (type blog blog))
  (with-locked-mapping ((blog-stream blog))
    (file-position (blog-stream blog) 0)
    (let ((props (read-blog-props (blog-stream blog))))
      (list :nblocks (props-nblocks props)
	    :id (props-id props)
	    :index (props-index props)
	    :tag (props-tag props)
	    :seqno (props-seqno props)
	    :header-size (props-header-size props)
	    :version (props-version props)))))

(defun sync-blog (blog)
  "Synchronize the binary log. Flushes the file mapping, reads and sets 
current properties."
  (declare (type blog blog))
  (with-locked-mapping ((blog-stream blog))
    (pounds::flush-buffers (blog-mapping blog))
    
    (file-position (blog-stream blog) 0)
    (setf (blog-props blog)
	  (read-blog-props (blog-stream blog))))
  nil)

(defun blog-index (blog)
  (props-index (blog-props blog)))

(defun (setf blog-index) (index blog)
  (unless (and (>= index 0) (< index (props-nblocks (blog-props blog))))
    (error "Index must be between 0 and ~A" (props-nblocks (blog-props blog))))
  
  (setf (props-index (blog-props blog)) index))


;; each block has a 16 byte header
(defxstruct entry ()
  (timestamp :uint64)
  (id :uint32)
  (count :uint32))

(defun read-blog-entry-props (stream)
  (let ((eblk (xdr-block 16)))
    (read-sequence (xdr-block-buffer eblk) stream)
    (decode-entry eblk)))

(defun write-blog-entry-props (stream e)
  (let ((eblk (xdr-block 16)))
    (encode-entry eblk e)
    (write-sequence (xdr-block-buffer eblk) stream)))

(defun write-blog-entry (stream e blk)
  (let ((eblk (xdr-block 16)))
    (encode-entry eblk e)
    (write-sequence (xdr-block-buffer eblk) stream)
    (write-sequence (xdr-block-buffer blk) stream
		    :start (xdr-block-offset blk)
		    :end (+ (xdr-block-offset blk) (entry-count e)))
    (incf (xdr-block-offset blk) (entry-count e))
    e))

(defun next-index (i nblocks)
  (let ((ni (1+ i)))
    (if (= ni nblocks)
	0
	ni)))

(defun prev-index (i nblocks)
  (if (zerop i)
      (1- nblocks)
      (1- i)))

;; when there are more blocks to read the count has the high bit set 
(defconstant +flag-more+ #x80000000)


(defun read-entry-locked (props stream blk set-props-p)
  (do ((i (props-index props) (next-index i (props-nblocks props)))
       (cnt 0)
       (id 0)
       (timestamp nil)
       (done nil))
      (done
       (progn 
	 ;; update the log props so that we advance
	 ;; but don't write it to the log because only writers do that 
	 (when set-props-p 
	   (setf (props-index props) i
		 (props-id props) id))
	 
	 (values cnt id timestamp)))
    (file-position stream
		   (+ +block+ (props-header-size props) (* +block+ i)))
    (let* ((e (read-blog-entry-props stream))
	   (rc (logand (entry-count e) (lognot +flag-more+))))
      (when blk 
	(read-sequence (xdr-block-buffer blk) stream
		       :start (xdr-block-offset blk)
		       :end (min (+ (xdr-block-offset blk) rc)
				 (xdr-block-count blk)))
	(setf (xdr-block-offset blk) 
	      (min (+ (xdr-block-offset blk) rc)
		   (xdr-block-count blk))))

      (incf cnt rc)
      (when (zerop (logand (entry-count e) +flag-more+))
	(setf done t))

      ;; TODO: check the id doesn't change
      (setf id (entry-id e)
	    timestamp (entry-timestamp e)))))

(declaim (ftype (function (blog (vector (unsigned-byte 8)) &key (:start integer) (:end (or null integer))) *)
		read-entry))
(defun read-entry (blog sequence &key (start 0) end)
  "Read the entry starting at the index currently pointed to by the log properties.

BLOG ::= binary log 
SEQUENCE ::= octet vector
START, END ::= region of sequence to read into.

If the message is larger than the sequence provided will read as much as it can,
returning the truncated message in the sequence. You can use READ-ENTRY-DETAILS
before calling this to ensure you have allocated sufficient space.

Updates the blog properties (but does not persist the updated properties) 
to the index of the next message to read. 

Returns (values count id timestamp) where 
COUNT ::= the number of bytes in the message.
ID ::= message ID
TIMESTAMP ::= universal time when the message was written.
"
  (declare (type blog blog)
	   (type (vector (unsigned-byte 8)) sequence)
	   (type integer start)
	   (type (or null integer) end))
  (let* ((count (- (or end (length sequence)) start))
	 (stream (blog-stream blog))
	 (blk (make-xdr-block :buffer sequence
			      :offset start
			      :count (+ start count))))
    (with-locked-mapping (stream)
      (let ((props (blog-props blog)))
	(read-entry-locked props stream blk t)))))

(declaim (ftype (function (blog) *) read-entry-details))
(defun read-entry-details (blog)
  "Read the properties of the next entry.
BLOG ::= the binary log.
Returns (values count id timestamp) where
COUTN ::= length of the message
ID ::= the ID of the message
TIMESTAMP ::= universal timestamp of when the message was written."
  (declare (type blog blog))
  (let ((stream (blog-stream blog)))
    (with-locked-mapping (stream)
      (let ((props (blog-props blog)))
	(read-entry-locked props stream nil nil)))))


;; advance the props index to the point where this ID is first found. returns true if found false otherwise 
(defun advance-to-id (stream props id)
  (do ((i (props-index props) (next-index i (props-nblocks props)))
       (started nil t))
      ((and started (= i (props-index props))) nil)
    (file-position stream
		   (+ +block+ (props-header-size props) (* +block+ i)))
    (let ((e (read-blog-entry-props stream)))
      (when (= (entry-id e) id)
	;; found the start of the entry
	(setf (props-index props) i)
	(return-from advance-to-id t)))))
	

(declaim (ftype (function (blog integer integer (vector (unsigned-byte 8)) &key (:start integer) (:end (or null integer))) *)
		read-entries))
(defun read-entries (blog id nmsgs sequence &key (start 0) end)
  "Read a set of messages starting from message ID. 

BLOG ::= the binary log
ID ::= starting ID.
NMSGS ::= number of messages to read.
SEQUENCE ::= octet vector to receive the messages.
START, END ::= region of sequence to read into.

Returns a list of (count id timestamp start end) for each message.
COUNT ::= the length of the message, even if it couldn't fit into the buffer.
ID ::= message ID.
TIMESTAMP ::= message universal timestamp
START, END ::= region of SEQUENCE that the message was written into. If the message
may have only been partially read into SEQUENCE if insufficient space was provided. 

Note that this function does not update the internal properties and therefore does 
not affect subsequent calls to READ-ENTRY.
"
  (declare (type blog blog)
	   (type integer id nmsgs start)
	   (type (vector (unsigned-byte 8)) sequence)
	   (type (or null integer) end))
  (unless end (setf end (length sequence)))

  (let ((stream (blog-stream blog))
	(blk (make-xdr-block :buffer sequence
			     :offset start
			     :count end)))
    
    (with-locked-mapping (stream)
      (file-position stream 0)
      (let ((props (read-blog-props stream)))
	;; advance the props to point to this ID or fail
	(unless (advance-to-id stream props id)
	  (return-from read-entries nil))

	;; keep reading entries until count have been read 
	(do ((n nmsgs (1- n))
	     (offset start)
	     (msgs nil))
	    ((zerop n) (nreverse msgs))
	  (setf (xdr-block-offset blk) offset)
	  (multiple-value-bind (cnt id timestamp) (read-entry-locked props stream blk t)
	    (push (list cnt id timestamp offset (xdr-block-offset blk))
		  msgs)
	    (setf offset (xdr-block-offset blk))))))))
  
;; writing is much simpler because we always just write at the end.
(declaim (ftype (function (blog (vector (unsigned-byte 8)) &key (:start integer) (:end (or null integer))) *)
		write-entry))
(defun write-entry (blog sequence &key (start 0) end)
  "Write a new message into the log.
BLOG ::= binary log
SEQUENCE ::= octet vector
START, END ::= region of sequence to write.

Writes the data into the log starting at the currently persisted index.
Updates the persisted properties to the next index, ID and increments the seqno.

Returns the ID of the message that was written."
  (declare (type blog blog)
	   (type (vector (unsigned-byte 8)) sequence)
	   (type integer start)
	   (type (or null integer) end))
  (let* ((count (- (or end (length sequence)) start))
	 (stream (blog-stream blog))
	 (blk (make-xdr-block :buffer sequence
			      :offset start
			      :count (+ start count))))
    (with-locked-mapping (stream)
      ;; we always read the current props from the file to ensure we are
      ;; consistent incase other processes are writing to it as well
      (file-position stream 0)
      (let ((props (read-blog-props stream)))
	;; adjust props
	(incf (props-id props))
	
	(do ((i (props-index props)
		(let ((ni (next-index i (props-nblocks props))))
		  (setf (props-index props) ni)
		  ni))
	     (e (make-entry :id (props-id props)
			    :timestamp (get-universal-time)))
	     (cnt count))
	    ((zerop cnt))
	  (file-position stream
			 (+ +block+ (props-header-size props) (* +block+ i)))
	  ;; set the entry count 
	  (cond
	    ;; if the is more data then set the flag 
	    ((> cnt +block-data+)
	     (decf cnt +block-data+)
	     (setf (entry-count e) (logior +block-data+ +flag-more+)))
	    (t
	     ;; this is the final block
	     (setf (entry-count e) cnt
		   cnt 0)))

	  (write-blog-entry-props stream e)
	  (write-sequence sequence stream
			  :start (xdr-block-offset blk)
			  :end (+ (xdr-block-offset blk)
				  (logand (entry-count e) (lognot +flag-more+))))
	  (incf (xdr-block-offset blk)
		(logand (entry-count e) (lognot +flag-more+))))
	
	;; write back the updated props 
	(incf (props-seqno props))	  
	(file-position (blog-stream blog) 0)
	(write-blog-props (blog-stream blog) props)
	
	(props-id props)))))

(defun reset-blog (blog)
  "Reset the binary log. Clears all data blocks and assigns new tag and seqno."
  (declare (type blog blog))
  ;; reset the props and clear contents 
  (let ((stream (blog-stream blog)))
    (with-locked-mapping (stream)
      (let ((props (blog-props blog)))
	(setf (props-tag props) (logand (get-universal-time) #xffffffff)
	      (props-seqno props) 0
	      (props-id props) 0
	      (props-index props) 0)
	(file-position stream 0)
	(write-blog-props stream props)
	
	(do ((i 1 (1+ i))
	     (buff (make-array +block+
			       :element-type '(unsigned-byte 8)
			       :initial-element 0)))
	    ((= i (props-nblocks props)) props)
	  (file-position stream
			 (+ +block+ (props-header-size props) (* +block+ i)))
	  (write-sequence buff stream))))))


      

  
