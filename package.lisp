;;;; Copyright (c) Frank James 2015 <frank.a.james@gmail.com>
;;;; This code is licensed under the MIT license.


(defpackage #:pounds
  (:use #:cl #:cffi #:trivial-gray-streams)
  (:nicknames #:lbs)
  (:export #:open-mapping
	   #:close-mapping
	   #:with-open-mapping))

