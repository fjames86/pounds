;;;; Copyright (c) Frank James 2015 <frank.a.james@gmail.com>
;;;; This code is licensed under the MIT license.

(in-package #:pounds)


#+(or windows win32)
(progn


(defstruct device 
  handle geometry)

;; access 
;;#define GENERIC_READ                     (0x80000000L)
;;#define GENERIC_WRITE                    (0x40000000L)
;;#define GENERIC_EXECUTE                  (0x20000000L)
;;#define GENERIC_ALL                      (0x10000000L)

;; mode
;; exclusive #x0
;; FILE_SHARE_READ #x01
;; FILE_SHARE_WRITER #x02

;; disposition
;; CREATE_ALWAYS 2
;; CREATE_NEW 1
;; OPEN_ALWAYS 4
;; OPEN_EXISTING 3
;; TRUNCATE_EXISTING 5

;; flags
;; FILE_ATTRIBUTE_NORMAL 128



#|  
typedef enum _MEDIA_TYPE {
    Unknown,                // Format is unknown
    F5_1Pt2_512,            // 5.25", 1.2MB,  512 bytes/sector
    F3_1Pt44_512,           // 3.5",  1.44MB, 512 bytes/sector
    F3_2Pt88_512,           // 3.5",  2.88MB, 512 bytes/sector
    F3_20Pt8_512,           // 3.5",  20.8MB, 512 bytes/sector
    F3_720_512,             // 3.5",  720KB,  512 bytes/sector
    F5_360_512,             // 5.25", 360KB,  512 bytes/sector
    F5_320_512,             // 5.25", 320KB,  512 bytes/sector
    F5_320_1024,            // 5.25", 320KB,  1024 bytes/sector
    F5_180_512,             // 5.25", 180KB,  512 bytes/sector
    F5_160_512,             // 5.25", 160KB,  512 bytes/sector
    RemovableMedia,         // Removable media other than floppy
    FixedMedia,             // Fixed hard disk media
    F3_120M_512,            // 3.5", 120M Floppy
    F3_640_512,             // 3.5" ,  640KB,  512 bytes/sector
    F5_640_512,             // 5.25",  640KB,  512 bytes/sector
    F5_720_512,             // 5.25",  720KB,  512 bytes/sector
    F3_1Pt2_512,            // 3.5" ,  1.2Mb,  512 bytes/sector
    F3_1Pt23_1024,          // 3.5" ,  1.23Mb, 1024 bytes/sector
    F5_1Pt23_1024,          // 5.25",  1.23MB, 1024 bytes/sector
    F3_128Mb_512,           // 3.5" MO 128Mb   512 bytes/sector
    F3_230Mb_512,           // 3.5" MO 230Mb   512 bytes/sector
    F8_256_128,             // 8",     256KB,  128 bytes/sector
    F3_200Mb_512,           // 3.5",   200M Floppy (HiFD)
    F3_240M_512,            // 3.5",   240Mb Floppy (HiFD)
    F3_32M_512              // 3.5",   32Mb Floppy
} MEDIA_TYPE, *PMEDIA_TYPE;
|#


(defun open-device-handle (name &key (access #x10000000) (mode 0) (disposition 3) (flags 0))
  (let ((handle (%create-file name access mode (null-pointer) disposition flags (null-pointer))))
    (if (invalid-handle-p handle)
        (get-last-error)
        handle)))

(defcfun (%device-io-control "DeviceIoControl" :convention :stdcall)
    :boolean
  (handle :pointer)
  (code :uint32)
  (buffer :pointer)
  (size :uint32)
  (out-buffer :pointer)
  (out-size :uint32)
  (bytes :pointer)
  (overlapped :pointer))

(defcstruct %disk-geometry 
  (cylinders :uint64)
  (media-type :uint32)
  (tracks-per-cylinder :uint32)
  (sectors-per-track :uint32)
  (bytes-per-sector :uint32))

(defstruct geometry 
  cylinders media-type tracks sectors bytes size)

;; IOCTL_DISK_GET_DRIVE_GEOMETRY == #x70000
(defun get-disk-geometry (handle)
  (with-foreign-objects ((geo '(:struct %disk-geometry))
			 (bytes :uint32))
    (let ((res (%device-io-control handle 
				   #x70000
				   (null-pointer)
				   0
				   geo
				   (foreign-type-size '(:struct %disk-geometry))
				   bytes
				   (null-pointer))))
      (if res
	  (let ((g (make-geometry)))
	    (setf (geometry-cylinders g)
		  (foreign-slot-value geo '(:struct %disk-geometry) 'cylinders)
		  (geometry-media-type g) 
		  (foreign-slot-value geo '(:struct %disk-geometry) 'media-type)
		  (geometry-tracks g)
		  (foreign-slot-value geo '(:struct %disk-geometry) 'tracks-per-cylinder)
		  (geometry-sectors g)
		  (foreign-slot-value geo '(:struct %disk-geometry) 'sectors-per-track)
		  (geometry-bytes g)
		  (foreign-slot-value geo '(:struct %disk-geometry) 'bytes-per-sector)
		  (geometry-size g)
		  (* (geometry-cylinders g)
		     (geometry-tracks g)
		     (geometry-sectors g)
		     (geometry-bytes g)))
	    g)
	  (get-last-error)))))


(defun open-device (path)
  (let ((handle (open-device-handle path :mode #x03)))
    (handler-case 
	(let ((geometry (get-disk-geometry handle)))
	  (make-device :handle handle
		       :geometry geometry))
      (error (e)
	(%close-handle handle)
	(error e)))))
  
(defun close-device (device)
  (%close-handle (device-handle device)))

) ;; windows/win32

#-(or windows win32)
(progn

(defstruct device 
  fd geometry)

;; FIXME: get device geometry on linux

(defun open-device (path)
  (let ((fd (%open path 0 0)))
    (if (< fd 0)
	(get-last-error)
	(make-device :fd fd))))
		     
(defun close-device (device)
  (%close (device-fd device)))
)


(defun write-block (device offset sequence &key (start 0) end)
  (write-file (device-handle device)
	      offset
	      sequence
	      :start start
	      :end end))

(defun read-block (device offset sequence &key (start 0) end)
  (let ((count (- (or end (length sequence)) start)))
    (read-file (device-handle device)
	       sequence
	       offset
	       count
	       :start start
	       :end end)))


