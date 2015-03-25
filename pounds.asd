;;;; Copyright (c) Frank James 2015 <frank.a.james@gmail.com>
;;;; This code is licensed under the MIT license.

(asdf:defsystem :pounds
  :name "lbs"
  :author "Frank James <frank.a.james@gmail.com>"
  :description "Lisp block storage"
  :license "MIT"
  :components
  ((:file "package")
   (:file "ffi" :depends-on ("package"))
   (:file "streams" :depends-on ("ffi"))
   (:file "log" :depends-on ("streams")))
  :depends-on (:cffi :trivial-gray-streams :frpc))



