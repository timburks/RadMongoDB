;; test_index.nu
;;  tests for RadMongoDB database access.
;;
;;  Copyright (c) 2013 Tim Burks, Radtastical Inc.

(load "RadMongoDB")

(class TestIndex is NuTestCase
 
 (- testMakeIndex is
    (set database "indextest")
    (set collection "sample")
    (set path (+ database "." collection))
    
    (set mongo (RadMongoDB new))
    (set connected (mongo connect))
    (assert_equal 0 connected)
    
    
    
    (unless (eq connected 0)
            (puts "could not connect to database. Is mongod running?")
            (return))
    ;(mongo authenticateUser:username withPassword:password forDatabase:"admin")
    
    ;; start clean
    (mongo dropDatabase:"indextest")
    
    ;; insert some numbers
    (100 times:
         (do (i)
             (set object (dict number:i text:(i stringValue)))
             (mongo insertObject:object intoCollection:"indextest.numbers")))
    
    (set indexCount (mongo countWithCondition:nil inCollection:"system.indexes" inDatabase:"indextest"))
    (assert_equal 1 indexCount)
    
    ;; create an index
    (set success (mongo ensureCollection:"indextest.numbers" hasIndex:(dict number:1 text:1) withOptions:0))
    (assert_equal 1 success)
    
    (set indexCount (mongo countWithCondition:nil inCollection:"system.indexes" inDatabase:"indextest"))
    (assert_equal 2 indexCount))
 )