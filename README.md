COSGoid
=======

one tool for the orpahned Goid fetch with high speed and efficiency


cserver hold all the Goids within internal tables including the Good Goids and Orphaned Goids.
the definition of "Orphaned" Goids is the goids locate in the Cserver only , not in DB dedicated 3rd party Application. Exp:Cassandra

this Tools help Tester/QA to filter orphaned goids from the pools. expecially huge bulk of the goids existing in the system,which result
in the CD fetch very very slow, especially for cassandra 2.1, should be cqlsh command issue inside, timeout err will be triggered for the long time


this tool use multiple processes to reduce the pressure of single request to DB, which improve the efficiency and time saving.

usage:
python COSGoid.py


Requisite module:
mmh3--murmur3 hash package

printout structure:

[root@cos89 ~]# cd COSGoidDIR/
[root@cos89 COSGoidDIR]# ll
total 1376
-rw-r--r-- 1 root root    834 Dec 30 03:05 boundryFile
-rw-r--r-- 1 root root     13 Dec 30 03:05 bug-goids
drwxr-xr-x 2 root root   4096 Dec 30 03:05 db
-rw-r--r-- 1 root root      0 Dec 30 03:05 orphaned-goids
-rw-r--r-- 1 root root 564606 Dec 30 03:05 servinfo
-rw-r--r-- 1 root root 821771 Dec 30 03:05 tokeninfo
[root@cos89 COSGoidDIR]# cd db
[root@cos89 db]# ls
list     list_12  list_17  list_21  list_26  list_30  list_35  list_4   list_8
list_0   list_13  list_18  list_22  list_27  list_31  list_36  list_40  list_9
list_1   list_14  list_19  list_23  list_28  list_32  list_37  list_5   list_comb
list_10  list_15  list_2   list_24  list_29  list_33  list_38  list_6   list_disorder
list_11  list_16  list_20  list_25  list_3   list_34  list_39  list_7
