;;;; Copyright (c) Frank James 2015 <frank.a.james@gmail.com>
;;;; This code is licensed under the MIT license.

(asdf:defsystem :pounds
  :name "pounds"
  :author "Frank James <frank.a.james@gmail.com>"
  :description "Lisp block storage, provides portable file mappings amongst other things."
  :license "MIT"
  :version "0.1.6"
  :components
  ((:file "package")
   (:file "ffi" :depends-on ("package"))
   (:file "mappings" :depends-on ("ffi"))
   (:file "log" :depends-on ("mappings")))
  :depends-on (:cffi :trivial-gray-streams :nibbles :babel :bordeaux-threads))




