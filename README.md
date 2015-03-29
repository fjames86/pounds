# pounds
Pounds provides several tools for manipulaing block storage from Lisp. This inspired the name, Lisp Block Storage (lbs), the name is somewhat of a misnomer but it has now stuck.


## 1. File mappings
The base functionality is provided by mmap'ing files and defining a stream class to read/write them. 


```
;; open a 1MB file and mmap it
(defparameter *mymap* (open-mapping "mymap.dat" :size (* 1024 1024)))

(with-mapping-stream (s *mymap*)
  (write-sequence #(1 2 3 4) s))

(close-mapping *mymap*)
```


## 2. Circular logs
The package POUNDS.LOG (nickname PLOG) implements a circular log by writing to the mmap'ed file. 

### 2.1 Motivation
The reason for writing to a log file are twofold: firstly, as an aid to development so that the
programmer can watch control flow. Secondly, to always be writing this information to the log 
so that it is possible to diagnose what caused an issue after the event. Typically long-running 
processes that provide services require such a logging system. This also provides a mechanism
for recording activity, for example users of the service if it provides an RPC interface.

Since the typical process will be writing to the log over a long period of time, and often in scenarios
where latency is an issue, two properties are required: it must be circular (so that we can write an arbitrary 
number of messages) and writes to the log must not block for a noticable period of time. The pounds logging
system attempts to provide such a logging system.

### 2.2 Usage

Open a log file (creating it if it doesn't exist) by calling OPEN-LOG. If no pathname 
is provided, a file in the local directory named "pounds.log" will be created. 

Write to the log using WRITE-MESSAGE.

E.g.
```
(defvar *mylog* (pounds.log:open-log))

(pounds.log:write-message *mylog* :info (format nil "Hello from Lisp. 1 + 2 == ~A" (+ 1 2)))

(pounds.log:close-log *mylog*)
```

Follow the log's output by using the command:
```
(pounds.log:start-following *mylog*)

(pounds.log:stop-following)
``` 

The log operators are thread-safe: you are free to write to the log from different threads. 

It is often the case that different modules of the same program want to write to the same log, but 
with a tag to differentiate them. 
```
(defvar *the-log* (open-log))

(let ((tag (babel:string-to-octets "LOG1")))
  (defun write1 (lvl format-control &rest args)
    (write-message *the-log* lvl (apply #'format nil format-control args) 
                   :tag tag)))
(let ((tag (babel:string-to-octets "LOG2")))
  (defun write2 (lvl format-control &rest args)
    (write-message *the-log* lvl (apply #'format nil format-control args)
                   :tag tag)))
```

## 3. Notes

The primary development platform was SBCL on Windows but it should also work on Linux. It may work on other unix-like platforms but
I've not tested it there.

## License

Released under the terms of the MIT license.

Frank James
March 2015.