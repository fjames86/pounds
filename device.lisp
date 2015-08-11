;;;; Copyright (c) Frank James 2015 <frank.a.james@gmail.com>
;;;; This code is licensed under the MIT license.

;;; This file defines an API for operating on block devices (Windows only).
;;; Can be used to perform direct I/O on physical disks. Usually best to ensure the disk is "offline".
;;; At the moment all the I/O is synchronous, we could add OVERLAPPED calls but 
;;; it adds a lot of complexity and there's no need for it right now.

(defpackage #:pounds.device 
  (:use #:cl #:cffi)
  (:export #:open-device
           #:close-device
           #:read-block
           #:write-block
           #:clear-block
           #:move-block))

(in-package #:pounds.device)


#+(or win32 windows)
(progn

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

;; typedef enum _MEDIA_TYPE {
;;     Unknown,                // Format is unknown
;;     F5_1Pt2_512,            // 5.25", 1.2MB,  512 bytes/sector
;;     F3_1Pt44_512,           // 3.5",  1.44MB, 512 bytes/sector
;;     F3_2Pt88_512,           // 3.5",  2.88MB, 512 bytes/sector
;;     F3_20Pt8_512,           // 3.5",  20.8MB, 512 bytes/sector
;;     F3_720_512,             // 3.5",  720KB,  512 bytes/sector
;;     F5_360_512,             // 5.25", 360KB,  512 bytes/sector
;;     F5_320_512,             // 5.25", 320KB,  512 bytes/sector
;;     F5_320_1024,            // 5.25", 320KB,  1024 bytes/sector
;;     F5_180_512,             // 5.25", 180KB,  512 bytes/sector
;;     F5_160_512,             // 5.25", 160KB,  512 bytes/sector
;;     RemovableMedia,         // Removable media other than floppy
;;     FixedMedia,             // Fixed hard disk media
;;     F3_120M_512,            // 3.5", 120M Floppy
;;     F3_640_512,             // 3.5" ,  640KB,  512 bytes/sector
;;     F5_640_512,             // 5.25",  640KB,  512 bytes/sector
;;     F5_720_512,             // 5.25",  720KB,  512 bytes/sector
;;     F3_1Pt2_512,            // 3.5" ,  1.2Mb,  512 bytes/sector
;;     F3_1Pt23_1024,          // 3.5" ,  1.23Mb, 1024 bytes/sector
;;     F5_1Pt23_1024,          // 5.25",  1.23MB, 1024 bytes/sector
;;     F3_128Mb_512,           // 3.5" MO 128Mb   512 bytes/sector
;;     F3_230Mb_512,           // 3.5" MO 230Mb   512 bytes/sector
;;     F8_256_128,             // 8",     256KB,  128 bytes/sector
;;     F3_200Mb_512,           // 3.5",   200M Floppy (HiFD)
;;     F3_240M_512,            // 3.5",   240Mb Floppy (HiFD)
;;     F3_32M_512              // 3.5",   32Mb Floppy
;; } MEDIA_TYPE, *PMEDIA_TYPE;

(defstruct device 
  handle 
  geometry
  bytes
  blocks
  size)


(defun open-device-handle (name &key (access #x10000000) (mode 0) (disposition 3) (flags 0))
  (let ((handle (pounds::%create-file name access mode (null-pointer) disposition flags (null-pointer))))
    (if (pounds::invalid-handle-p handle)
        (pounds::get-last-error)
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
  (cylinders 0 :type fixnum)
  (media-type 0 :type fixnum)
  (tracks 0 :type fixnum)
  (sectors 0 :type fixnum)
  (bytes 0 :type fixnum)
  (size 0 :type integer))

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
          (pounds::get-last-error)))))


(defun open-device (n)
  (declare (type integer n))
  (let ((path (format nil "\\\\.\\PhysicalDrive~A" n)))
    (let ((handle (open-device-handle path :mode #x03)))
      (handler-bind ((error (lambda (e)
                              (declare (ignore e))
                              ;; decline to handle the error, ensure we close the handle to clean up properly 
                              (pounds::%close-handle handle))))
        (let ((geometry (get-disk-geometry handle)))
          (make-device :handle handle
                       :geometry geometry
                       :bytes (geometry-bytes geometry)
                       :blocks (* (geometry-cylinders geometry) 
                                  (geometry-sectors geometry) 
                                  (geometry-tracks geometry))
                       :size (geometry-size geometry)))))))

(defun close-device (device)
  (declare (type device device))
  (pounds::%close-handle (device-handle device)))

(defun write-block (device block sequence &key (start 0) end)
  (declare (type device device)
           (type integer block)
           (type (vector (unsigned-byte 8)) sequence))
  (let ((offset (* block (device-bytes device))))
    (pounds::write-file (device-handle device)
                offset
                sequence
                :start start
                :end end)))

(defun read-block (device block sequence &key (start 0) end)
  (declare (type device device)
           (type integer block)
           (type (vector (unsigned-byte 8)) sequence))
  (let ((count (- (or end (length sequence)) start))
        (offset (* block (device-bytes device))))
    (pounds::read-file (device-handle device)
		       sequence
		       offset
		       count
		       :start start
		       :end (or end (+ start count)))))

(defun clear-block (device block &optional (value 0))
  (declare (type device device)
           (type integer block)
           (type (unsigned-byte 8) value))
  (let ((length (device-bytes device))
        (offset (* block (device-bytes device))))
    (with-foreign-objects ((buffer :uint8 length)
                           (nbytes :uint32)
                           (overlapped '(:struct pounds::overlapped)))
      ;; initialize the buffer with the constant value 
      (do ((i 0 (1+ i)))
          ((= i length))
        (setf (mem-aref buffer :uint8 i) value))
      ;; write to the file 
      (multiple-value-bind (offset-low offset-high) (pounds::split-offset offset)
        (setf (foreign-slot-value overlapped '(:struct pounds::overlapped)
                                  'pounds::offset)
              offset-low
              (foreign-slot-value overlapped '(:struct pounds::overlapped)
                                  'pounds::offset-high)
              offset-high))
      (let ((res (pounds::%write-file (device-handle device)
                              buffer
                              length
                              nbytes
                              overlapped)))
        (unless res 
          (pounds::get-last-error))))))

(defun move-block (device new-block old-block)
  (declare (type device device)
           (type integer new-block old-block))
  (let ((length (device-bytes device)))
    (with-foreign-objects ((buffer :uint8 length)
                           (nbytes :uint32)
                           (overlapped '(:struct pounds::overlapped)))
      ;; start by reading the old block
      (let ((offset (* old-block length)))
        (multiple-value-bind (offset-low offset-high) (pounds::split-offset offset)
          (setf (foreign-slot-value overlapped '(:struct pounds::overlapped)
                                    'pounds::offset)
                offset-low
                (foreign-slot-value overlapped '(:struct pounds::overlapped)
                                    'pounds::offset-high)
                offset-high)
          (let ((res (pounds::%read-file (device-handle device)
                                 buffer 
                                 length 
                                 nbytes
                                 overlapped)))
            (unless res
              (pounds::get-last-error)))))
      ;; now write it back out to the new location
      (let ((offset (* new-block length)))
        (multiple-value-bind (offset-low offset-high) (pounds::split-offset offset)
          (setf (foreign-slot-value overlapped '(:struct pounds::overlapped)
                                    'pounds::offset)
                offset-low
                (foreign-slot-value overlapped '(:struct pounds::overlapped)
                                    'pounds::offset-high)
                offset-high)
          (let ((res (pounds::%write-file (device-handle device)
                                  buffer 
                                  length 
                                  nbytes
                                  overlapped)))
            (unless res
              (pounds::get-last-error))))))))

) ;; Windows 

#-(or win32 windows) 
(progn

(defstruct device
  fd 
  bytes 
  size)

(defun open-device (path)
  nil)

(defun close-device (device)
  nil)

(defun read-block (device block sequence &key (start 0) end)
  nil)

(defun write-block (device block sequence &key (start 0) end)
  nil)

(defun clear-block (device block &optional (value 0))
  nil)

(defun move-block (device new-block old-block)
  nil)


) ;; non-Windows


