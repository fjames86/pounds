# Pounds
Pounds provides several tools for common operations on mmaped files. Currently, it offers a stream class
to read from and write to a mmaped file. Using this, it implements a simple logging system.

## 1. File mappings
The base functionality is provided by mmap'ing files and defining a stream class to read/write them. 


```
;; open a 1MB file and mmap it
(defparameter *mymap* (open-mapping "mymap.dat" (* 1024 1024)))

;; make a stream from the mapping and write to it
(with-mapping-stream (s *mymap*)
  (write-sequence #(1 2 3 4) s))

(close-mapping *mymap*)
```


## 2. Circular logs
The package POUNDS.LOG (nickname PLOG) implements a circular log by writing to the mmap'ed file. 

### 2.1 Motivation
Note: I'm aware there are several other logging systems for Common Lisp but I have a specific
set of requirements which these other libraries didn't provide, so I wrote my own.

Long running services need to record their activity. For instance, who mounted an NFS, what files they 
accessed etc. This is needed for two reasons: both for auditing service usage, and as a record of what 
the service actually did. We need this because when things go wrong, you need a record of what happened
to analyze. 

We therefore need a logging system which has the following properties:
* Can write an arbitrary number of messages without running out of space. This means a circular log.
* Writing to the log must have a very low-latency. Writes should never block for a significant time.
* Stability and reliability are critical.
* Portability: working on Windows and Linux is mandatory.

Writing to a mmap'ed file should satisfy the above requirements.

### 2.2 Usage

Open a log file (creating it if it doesn't exist) by calling OPEN-LOG. If no pathname 
is provided, a file in the local directory named "pounds.log" will be created. 
The defualt size is 16k blocks of 128 bytes each (so a 2MB file). This means it can 
store a maximum of 16k messages. If a message is larger than the block size it will 
use multiple contiguous blocks.

Write a message to the log using WRITE-MESSAGE.

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
This starts a thread which monitors the log and prints messages to the output stream 
as they are written to the log. 

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

The log operators (READ-MESSAGE and WRITE-MESSAGE) are thread-safe. It is assumed that 
the Lisp process is the only process accessing the files -- it is therefore NOT permitted 
to access a log file simulataneously from multiple Lisp images. It is easy to add this 
(using flock or equivalent) but since the typical use-case doesn't require it, I don't do it.

### 2.3 Performance

I've not done any rigorous benchmarking but performance feels sufficiently good. I tested on my 
crummy old laptop (1.5Ghz core 2 duo, 1GB RAM running Windows 8.1). 

I wrote 100,000 messages to a standard 2MB (16k/128) log and it took 100 seconds, SBCL maxed out 1 of my cores. 
Taking 1ms per write seems acceptable. 

The equivalent experiment using LOG4CL took over 200 seconds to write 10,000 messages 
before I gave up waiting. Both SBCL and emacs were maxing out a core each. 


## 3. Notes

The primary development platform was SBCL on Windows 8.1 but I've used it on Ubuntu Linux.
It may work on other unix-like platforms but I've not tested it there.

## License

Released under the terms of the MIT license.

Frank James
March 2015.