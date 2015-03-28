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

It is often the case that several different modules may wish to share the same log file.
To syncronize multiple threads writing to the log, you MUST only open the log once. Other
sub-modles may copy the log so that they can use different tags. They will share the same 
underlying file mapping (and mutex for syncronization). 
```
(defvar *log1* (pounds.log:open-log :tag "LOG1"))
(defvar *log2* (pounds.log:copy-log *log1* :tag "LOG2"))

(pounds.log:write-message *log1* :info "Hello")
(pounds.log:write-message *log2* :info "Goodbye")
```

## 3. Notes

The primary development platform was SBCL on Windows but it should also work on Linux. It may work on other unix-like platforms but
I've not tested it there.

## License

Released under the terms of the MIT license.

Frank James
March 2015.