;;;; Copyright (c) Frank James 2015 <frank.a.james@gmail.com>
;;;; This code is licensed under the MIT license.

;;; this file defines the ffi necessary to mmap a file and read/write blocks

(in-package #:pounds)

#+(or win32 windows)
(progn

;; for errors
(defcfun (%format-message "FormatMessageA" :convention :stdcall)
    :uint32
  (flags :uint32)
  (source :pointer)
  (msg-id :uint32)
  (lang-id :uint32)
  (buffer :pointer)
  (size :uint32)
  (args :pointer))

(defun format-message (code)
  "Use FormatMessage to convert the error code into a system-defined string."
  (with-foreign-object (buffer :char 1024)
    (let ((n (%format-message #x00001000
			      (null-pointer)
			      code
			      0
			      buffer
			      1024
			      (null-pointer))))
      (if (= n 0)
	  (error "Failed to format message")
	  (foreign-string-to-lisp buffer :count (- n 2))))))

(define-condition win-error (error)
  ((code :initform 0 :initarg :code :reader win-error-code))
  (:report (lambda (condition stream)
	     (format stream "ERROR ~A: ~A" 
		     (win-error-code condition)
		     (format-message (win-error-code condition))))))
	   
(defcfun (%get-last-error "GetLastError" :convention :stdcall) :long)

(defun get-last-error ()
  (let ((code (%get-last-error)))
    (unless (zerop code)
      (error 'win-error :code code))))


;; -----------------------



(defctype handle :pointer)
(defctype size-t 
    #+(or x86-64 x64 amd64):uint64 
    #-(or x86-64 x64 amd64):uint32)

(defcfun (%create-file-mapping "CreateFileMappingA" :convention :stdcall)
    handle
  (h handle)
  (attrs :pointer)
  (protect :uint32)
  (size-high :uint32)
  (size-low :uint32)
  (name :string))

(defcfun (%map-view-of-file "MapViewOfFile" :convention :stdcall)
    :pointer
  (h handle)
  (access :uint32)
  (offset-high :uint32)
  (offset-low :uint32)
  (nbytes size-t))

(defcfun (%unmap-view-of-file "UnmapViewOfFile" :Convention :stdcall)
    :boolean
  (h handle))


;;#define GENERIC_READ                     0x80000000
;;#define GENERIC_WRITE                    0x40000000
;;#define GENERIC_EXECUTE                  0x20000000
;;#define GENERIC_ALL                      0x10000000
;;
;;#define FILE_SHARE_READ                  0x00000001
;;#define FILE_SHARE_WRITE                 0x00000002
;;#define FILE_SHARE_DELETE                0x00000004

(defcfun (%create-file "CreateFileA" :convention :stdcall)
    handle
  (filename :string)
  (access :uint32)
  (mode :uint32)
  (attrs :pointer)
  (disposition :uint32)
  (flags :uint32)
  (template :pointer))

(defcfun (%close-handle "CloseHandle" :convention :stdcall)
    :boolean
  (h handle))

(defcfun (%flush-file-buffers "FlushFileBuffers" :convention :stdcall)
    :boolean
  (h handle))

(defcfun (%get-file-size "GetFileSize" :convention :stdcall)
    :uint32
  (h handle)
  (high :pointer))

(defcstruct overlapped 
  (internal :pointer)
  (internal-high :pointer)
  (offset :uint32)
  (offset-high :uint32)
  (handle :pointer))

(defcfun (%lock-file-ex "LockFileEx" :convention :stdcall)
    :boolean
  (h handle)
  (flags :uint32) ;; 2 == exclusive, 1 == fail immediately
  (reserved :uint32)
  (low :uint32)
  (high :uint32)
  (overlapped :pointer))

(defcfun (%unlock-file-ex "UnlockFileEx" :convention :stdcall)
    :boolean
  (h handle)
  (reserved :uint32)
  (low :uint32)
  (high :uint32)
  (overlapped :pointer))

(defcfun (%write-file "WriteFile" :convention :stdcall)
    :boolean
  (handle :pointer)
  (buffer :pointer)
  (count :uint32)
  (bytes :pointer)
  (overlapped :pointer))

(defun split-offset (offset)
  (values (logand offset #xffffffff)
	  (ash offset -32)))

(defun write-file (handle offset sequence &key (start 0) end)
  (let* ((length (length sequence))
	 (count (- (or end length) start)))
    (with-foreign-objects ((buffer :uint8 length)
			   (nbytes :uint32)
			   (overlapped '(:struct overlapped)))
      (do ((i 0 (1+ i)))
	  ((= i count))
	(setf (mem-aref buffer :uint8 i)
	      (elt sequence (+ start i))))
      (multiple-value-bind (offset-low offset-high) (split-offset offset)
	(setf (foreign-slot-value overlapped '(:struct overlapped)
				  'offset)
	      offset-low
	      (foreign-slot-value overlapped '(:struct overlapped)
				  'offset-high)
	      offset-high))
      (let ((res (%write-file handle 
			      buffer
			      length
			      nbytes
			      overlapped)))
	(if res
	    nil
	    (get-last-error))))))

(defstruct mapping 
  fhandle 
  mhandle 
  ptr 
  size 
  (lock (bt:make-lock)))

(defun invalid-handle-p (handle)
  (pointer-eq handle 
	      (make-pointer #+(or x86-64 x64 amd64)#xffffffffffffffff
			    #-(or x86-64 x64 amd64)#xffffffff)))

(defun open-mapping (path &key size)
  "Opens a file named by PATHSPEC and maps it into memory. If the file is too small it is extended. 
Returns a MAPPING structure."
  ;; open, extend and close the file using regular CL functions first 
  (with-open-file (f path 
		     :direction :io 
		     :if-exists :overwrite 
		     :if-does-not-exist :create
		     :element-type '(unsigned-byte 8))
    (let ((length (file-length f)))
      (cond
	((zerop length)
	 (unless size (error "Must provide a size when creating mapping file"))
	 (file-position f size)
	 (write-byte 0 f))
	((and size (> size length))
	 (file-position f size)
	 (write-byte 0 f))
	((not size) 
	 (setf size length)))))

  (let ((fhandle (with-foreign-string (s path)
		   (%create-file s 
				 #xC0000000 ;; access == generic_read|generic_write
				 3 ;; mode == share_read|share_write
				 (null-pointer) ;; attrs
				 4 ;; disposition == open always
				 128 ;; flags == file attribute normal
				 (null-pointer)))))
    (when (invalid-handle-p fhandle)
      (get-last-error))
    ;; extend the file if necessary 
    (cond
      (size 
       (when (> size (%get-file-size fhandle (null-pointer)))
	 (handler-case (write-file fhandle size #(0))
	   (error (e)
	     (%close-handle fhandle)
	     (error e)))
	 (%close-handle fhandle)
	 (open-mapping path :size size)))
      ((and (not size) 
	    (zerop (%get-file-size fhandle (null-pointer))))
       (%close-handle fhandle)
       (error "Must provide initial size when creating file"))
      (t 
       (setf size (%get-file-size fhandle (null-pointer)))))
    
    (let ((mhandle (%create-file-mapping fhandle
					 (null-pointer) ;; attrs
					 4 ;; protect == page_readwrite
					 0 ;; high
					 0 ;; low
					 (null-pointer))))
      (when (null-pointer-p mhandle)
	(%close-handle fhandle)
	(get-last-error))
      (let ((ptr (%map-view-of-file mhandle
				    #x001f ;; access == filemapallaccess
				    0 ;; low
				    0 ;; high
				    size)))
	(when (null-pointer-p ptr)
	  (%close-handle mhandle)
	  (%close-handle fhandle)
	  (get-last-error))
	(make-mapping :fhandle fhandle
		      :mhandle mhandle
		      :ptr ptr
		      :size size)))))

(defun close-mapping (mapping)
  "Closes the mapping structure."
  (declare (type mapping mapping))
  (with-slots (fhandle mhandle ptr) mapping
    (%unmap-view-of-file ptr)
    (%close-handle mhandle)
    (%close-handle fhandle)))

(defun remap (mapping size)
  "Remaps the file mapping to the new size."
  (let ((fhandle (mapping-fhandle mapping)))
    ;; close the file mapping 
    (with-slots (mhandle ptr) mapping
	(%unmap-view-of-file ptr)
      (%close-handle mhandle))

    ;; extend the file if we need to 
    (when (> size (mapping-size mapping))
      (write-file fhandle size #(0)))

    ;; remap
    (let ((mhandle (%create-file-mapping fhandle
					 (null-pointer) ;; attrs
					 4 ;; protect == page_readwrite
					 0 ;; high
					 0 ;; low
					 (null-pointer))))
      (when (invalid-handle-p mhandle)
	(get-last-error))
      (let ((ptr (%map-view-of-file mhandle
				    #x001f ;; access == filemapallaccess
				    0 ;; low
				    0 ;; high
				    0)))
	(when (null-pointer-p ptr)
	  (get-last-error))
	(setf (mapping-mhandle mapping) mhandle
	      (mapping-ptr mapping) ptr
	      (mapping-size mapping) size))))
  mapping)

(defun flush-buffers (mapping)
  "Ensure changes to the file mapping are written to disk."
  (%flush-file-buffers (mapping-fhandle mapping)))

;; we lock the first byte and rely on co-operative locking
(defun lock-mapping (map)
  (with-foreign-object (overlapped '(:struct overlapped))
    (setf (foreign-slot-value overlapped '(:struct overlapped)
			      'offset)
	  0
	  (foreign-slot-value overlapped '(:struct overlapped)
			      'offset-high)
	  0
	  (foreign-slot-value overlapped '(:struct overlapped)
			      'handle)
	  (null-pointer))
    (%lock-file-ex (mapping-fhandle map)
		   2
		   0
		   1
		   0
		   overlapped)))

(defun unlock-mapping (map)
  (with-foreign-object (overlapped '(:struct overlapped))
    (setf (foreign-slot-value overlapped '(:struct overlapped)
			      'offset)
	  0
	  (foreign-slot-value overlapped '(:struct overlapped)
			      'offset-high)
	  0
	  (foreign-slot-value overlapped '(:struct overlapped)
			      'handle)
	  (null-pointer))
    (%unlock-file-ex (mapping-fhandle map)
		     0
		     1
		     0
		     overlapped)))



) 


#-(or win32 windows)
(progn

(defctype size-t 
    #+(or x86-64 x64 amd64):uint64 
    #-(or x86-64 x64 amd64):uint32)
(defctype ssize-t 
    #+(or x86-64 x64 amd64):int64 
    #-(or x86-64 x64 amd64):int32)

(defcfun (%mmap "mmap")
    :pointer
  (addr :pointer)
  (length size-t)
  (prot :int32) ;; prot_read == 1, prot_write == 2
  (flags :int32) ;; map_shared == 1
  (fd :int32)
  (offset size-t))

(defcfun (%munmap "munmap")
    :int32
  (p :pointer)
  (len size-t))

(defcfun (%open "open")
    :int32
  (path :string)
  (flags :int32) ;; 64 == o_creat|o_rdwr
  (mode :int32)) ;; 438 == rw|rw|rw

(defcfun (%close "close")
    :int32
  (fd :int32))

(defcfun (%fsync "fsync")
    :int32
  (fd :int32))

(defcfun (%lseek "lseek")
    size-t
  (fd :int32)
  (offset :uint32)
  (whence :int32)) ;; 0 == set, 1 == cur, 2 == end

(defcfun (%write "write") 
    ssize-t
  (fd :int32)
  (buffer :pointer)
  (count size-t))
(defun write-zero-at (fd offset)
  (%lseek fd offset 0)
  (with-foreign-object (b :uint8)
    (setf (mem-ref b :uint8) 0)
    (%write fd b 1)))

(defcfun (%flock "flock")
    :int32
  (fd :int32)
  (op :int32))

(defcvar "errno" :int)

(defcfun (%strerror "strerror") :string     
  (code :int32))
(defun get-last-error ()
  (error "failed: ~A" (%strerror *errno*)))


(defstruct mapping 
  fd 
  ptr
  size
  (lock (bt:make-lock)))

(defun invalid-pointer-p (handle)
  (pointer-eq handle 
	      (make-pointer #+(or x86-64 x64 amd64)#xffffffffffffffff
			    #-(or x86-64 x64 amd64)#xffffffff)))

(defun open-mapping (path &key size)
  "Opens the file named by PATH and maps it into memory. 

If SIZE is provided, the file is first extended to be SIZE bytes. SIZE must be provided when 
creating the file."
  ;; use regular CL functions to create the file and check its length
  (with-open-file (f path 
		     :direction :io 
		     :if-exists :overwrite 
		     :if-does-not-exist :create
		     :element-type '(unsigned-byte 8))
    (let ((length (file-length f)))
      (cond
	((zerop length)
	 (unless size (error "Must provide a size when creating mapping file"))
	 (file-position f size)
	 (write-byte 0 f))
	((and size (> size length))
	 (file-position f size)
	 (write-byte 0 f))
	((not size) 
	 (setf size length)))))
  ;; the file is now created and the correct size 
  (let ((fd (with-foreign-string (s path)
	      (%open s 
		     2 ;; o_rdwr
		     600)))) ;; rw
    (when (< fd 0)
      (get-last-error))
    (let ((ptr (%mmap (null-pointer) 
		      size 
		      3 ;; prot_read | prot_write
		      1 ;; map_shared
		      fd 
		      0)))
      (when (invalid-pointer-p ptr)
	(%close fd)
	(get-last-error))
      (make-mapping :fd fd
		    :ptr ptr
		    :size size))))
  
(defun close-mapping (mapping)
  "Close the file mapping."
  (declare (type mapping mapping))
  (%munmap (mapping-ptr mapping) 
	   (mapping-size mapping))
  (%close (mapping-fd mapping)))

(defun remap (mapping size)
  "Remap the file. SIZE should be the new size."
  (%munmap (mapping-ptr mapping)
	   (mapping-size mapping))
  ;; write a byte at size
  (when (> size (mapping-size mapping))
    (write-zero-at (mapping-fd mapping) size))    
  ;; remap 
  (let ((ptr (%mmap (null-pointer) 
		    size 
		    3 ;; prot_read|prot_write
		    1 ;; map_shared
		    (mapping-fd mapping)
		    0)))
    (when (invalid-pointer-p ptr)
      (get-last-error))
    (setf (mapping-ptr mapping) ptr
	  (mapping-size mapping) size)
    mapping))
  
(defun flush-buffers (mapping)
  "Ensure changes to the file mappign are written to disk"
  (%fsync (mapping-fd mapping)))

(defun lock-mapping (map)
  (%flock (mapping-fd map)
	  1)) ;; lock_sh

(defun unlock-mapping (map)
  (%flock (mapping-fd map)
	  8)) ;; lock_un

)

(defun read-mapping-block (sequence mapping offset &key (start 0) end)
  "Read from the mapping offset into the sequence."
  (bt:with-lock-held ((mapping-lock mapping))
    (lock-mapping mapping)
    (let ((count (- (or end (length sequence)) start)))
      (do ((i 0 (1+ i)))
	  ((= i count))
	(setf (elt sequence (+ i start))
	      (mem-aref (mapping-ptr mapping) :uint8 (+ offset i)))))
    (unlock-mapping mapping)
    sequence))

(defun write-mapping-block (sequence mapping offset &key (start 0) end)
  "Write the sequence into the mapping."
  (bt:with-lock-held ((mapping-lock mapping))
    (lock-mapping mapping)
    (let ((count (- (or end (length sequence)) start)))
      (do ((i 0 (1+ i)))
	  ((= i count))
	(setf (mem-aref (mapping-ptr mapping) :uint8 (+ offset i))
	      (elt sequence (+ i start))))
      (flush-buffers mapping)
      (unlock-mapping mapping)
      count)))

