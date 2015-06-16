;;;; Copyright (c) Frank James 2015 <frank.a.james@gmail.com>
;;;; This code is licensed under the MIT license.

(asdf:defsystem :pounds
  :name "pounds"
  :author "Frank James <frank.a.james@gmail.com>"
  :description "Provides portable file mappings and related utilities."
  :license "MIT"
  :version "0.2.0"
  :components
  ((:file "package")
   (:file "ffi" :depends-on ("package"))
   (:file "mappings" :depends-on ("ffi"))
   (:file "log" :depends-on ("mappings"))
   (:file "database" :depends-on ("mappings")))
  :depends-on (:cffi :trivial-gray-streams :nibbles :babel :bordeaux-threads))




