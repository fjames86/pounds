;;;; Copyright (c) Frank James 2015 <frank.a.james@gmail.com>
;;;; This code is licensed under the MIT license.


(defpackage #:pounds
  (:use #:cl #:cffi #:trivial-gray-streams)
  (:export #:open-mapping
	   #:close-mapping
	   #:mapping-stream-position
	   #:mapping-stream-mapping
	   #:mapping-size
	   #:mapping-stream
	   #:make-mapping-stream))



