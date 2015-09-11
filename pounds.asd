;;;; Copyright (c) Frank James 2015 <frank.a.james@gmail.com>
;;;; This code is licensed under the MIT license.

(asdf:defsystem :pounds
  :name "pounds"
  :author "Frank James <frank.a.james@gmail.com>"
  :description "Provides portable file mappings and related utilities."
  :license "MIT"
  :version "0.2.0"
  :serial t
  :components
  ((:file "package")
   (:file "ffi")
   (:file "mappings")
   (:file "log")
   (:file "database"))
;;   (:file "device"))
  :depends-on (:cffi :trivial-gray-streams 
	       :nibbles :babel :bordeaux-threads))

