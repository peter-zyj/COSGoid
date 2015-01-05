#!/usr/bin/env python
import os, sys
import time, datetime
import subprocess
import mmh3
from multiprocessing import Process, Queue


def timeout_command(command, timeout):
    """call shell-command and either return its output or kill it
    if it doesn't normally exit within timeout seconds and return None"""
    import signal

    start = datetime.datetime.now()
    print "start subprocess"
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    while process.poll() is None:
        now = datetime.datetime.now()
        j = (now - start).seconds
        sys.stdout.write("... Waiting for cassandra return ..." + str(j) + ' seconds' + "\r")
        sys.stdout.flush()
        if (now - start).seconds > timeout:
            os.kill(process.pid, signal.SIGKILL)
            os.waitpid(-1, os.WNOHANG)
            print
            print "cz::debug::timeout!!!!!"
            return None
    print
    time.sleep(5)
    return process.stdout.read()


def FileHandle(time_wait0):
    cmd1 = 'echo 1 > /proc/calypso/test/reopen_logfiles'
    cmd2 = 'echo 2 > /proc/calypso/tunables/cm_logserverinfo'
    cmd3 = 'ls -tr /arroyo/log/serverinfo.log* | tail -n 1'

    os.popen(cmd1)
    os.popen(cmd2)
    time.sleep(time_wait0)
    file = os.popen(cmd3).read().strip()
    fileUntilReady(file, time_wait0)

    os.popen("awk '/\w+ Object/{if(NF==4){sub(/0x/, \"\");print\" \"$3}}' %s |sort|uniq | tee %s 2>/dev/null" % (
        file, "/root/COSGoidDIR/servinfo"))


def TokenCreation(q):
    f = open('/root/COSGoidDIR/servinfo', 'r')
    content = f.read().split()
    f.close()
    murMurList = []

    for i in content:
        temp = mmh3.hash64(i.strip())[0]
        murMurList.append(temp)
    murMurList.sort()
    f2 = open('/root/COSGoidDIR/tokeninfo', 'w+')
    for k in murMurList:
        f2.write('%d\n' % k)
    f2.close()
    q.put([murMurList[0], murMurList[-1]])


def fileUntilReady(file, wTime):
    if os.path.exists(file):
        while True:
            p = os.popen("ls -s %s | awk '{print $1}'" % (file)).read()
            # print "p",p
            time.sleep(wTime)
            p1 = os.popen("ls -s %s | awk '{print $1}'" % (file)).read()
            #print "p1",p1
            if p == p1 and p.strip() != '0':
                break
    else:
        return False

    return True


def fileReady(file, wTime=1):
    if os.path.exists(file):
        p = os.popen("ls -s %s | awk '{print $1}'" % (file)).read()
        #print "p",p
        time.sleep(wTime)
        p1 = os.popen("ls -s %s | awk '{print $1}'" % (file)).read()
        #print "p1",p1
        if p == p1 and p.strip() != "0":
            return True
        else:
            return False
    else:
        return False


def dirReady(folder, wTime=1):
    if os.path.exists(folder):
        p = os.popen("du %s | awk '{print $1}'" % (folder)).read()
        # print "p",p
        time.sleep(wTime)
        p1 = os.popen("du %s | awk '{print $1}'" % (folder)).read()
        #print "p1",p1
        if p == p1:
            return True
        else:
            return False
    else:
        return False


def CassandraHandleFork(index, preGoid, sufGoid):
    # print preGoid
    # print sufGoid
    # print index
    cmd1 = 'cqlsh -e "SELECT goid from cos.goid where token(goid) < %s and token(goid) >= %s" > /root/COSGoidDIR/db/list_tmp_%s' % (sufGoid, preGoid, index)
    print cmd1
    os.popen(cmd1)

    os.popen("awk '{if($0~/ \w\w\w\w\w/){print}}' /root/COSGoidDIR/db/list_tmp_%s |sort|uniq | tee /root/COSGoidDIR/db/list_%s 2>/dev/null" % (index, index))
    os.popen("rm -rf /root/COSGoidDIR/db/list_tmp_%s" % (index))
    return


def execute():
    os.popen("rm -rf /root/COSGoidDIR/db 2>/dev/null")
    os.popen("rm -rf /root/COSGoidDIR 2>/dev/null")
    os.popen("mkdir /root/COSGoidDIR 2>/dev/null")
    os.popen("mkdir /root/COSGoidDIR/db 2>/dev/null")

    LocalIP = os.popen("ifconfig | grep -A 1 'eth0'|awk '/inet addr/{print $2}'").read().split(':')[-1]
    DB_Complex_Handle = False

    res = os.popen("cqlsh -e 'describe keyspace cos'").read()
    if res == '':
        os.popen("awk '{if($0~/^rpc_address: */){print \"rpc_address: 0.0.0.0\\nbroadcast_rpc_address: %s\";}else{print $0;}}' /etc/cassandra/default.conf/cassandra.yaml > /etc/cassandra/default.conf/cassandra.yaml_2" % (LocalIP.strip()))
        os.popen("mv /etc/cassandra/default.conf/cassandra.yaml /etc/cassandra/default.conf/cassandra.yaml_bu")
        os.popen("mv /etc/cassandra/default.conf/cassandra.yaml_2 /etc/cassandra/default.conf/cassandra.yaml")
        os.popen("service cassandra restart")
        # time.sleep(20)
    while True:
        res = os.popen("cqlsh -e 'describe keyspace cos'").read()
        if res != "":
            break


    cmd1 = "cqlsh -e 'SELECT goid from cos.goid' > /root/COSGoidDIR/db/list"
    result = timeout_command(cmd1, 10)

    if result == None:
        DB_Complex_Handle = True
        time_wait0 = 5
    else:
        DB_Complex_Handle = False
        time_wait0 = 2


    # "TBD-cpu1"  // servfile handling
    p1 = Process(target=FileHandle, args=(time_wait0,))
    p1.start()

    #cqlsh cmd setting

    start = datetime.datetime.now()
    while True:
        if fileReady('/root/COSGoidDIR/servinfo', time_wait0):
            #if os.path.exists("/root/COSGoidDIR/servinfo") == True and os.path.getsize("/root/COSGoidDIR/servinfo") != 0:
            now = datetime.datetime.now()
            j = (now - start).seconds
            sys.stdout.write("... Waiting for cserver return ..." + str(j) + ' seconds' + "\r")
            sys.stdout.flush()
            #print "####:",os.path.getsize("/root/COSGoidDIR/servinfo")
            break
        else:
            now = datetime.datetime.now()
            j = (now - start).seconds
            sys.stdout.write("... Waiting for cserver return ..." + str(j) + ' seconds' + "\r")
            sys.stdout.flush()

    print

    if DB_Complex_Handle == True:
        print "Start Multiple processiong"
        q = Queue()
        p2 = Process(target=TokenCreation, args=(q,))
        p2.start()
        token1st = q.get()

        start = datetime.datetime.now()
        while True:
            if fileReady('/root/COSGoidDIR/tokeninfo', time_wait0):
                now = datetime.datetime.now()
                j = (now - start).seconds
                sys.stdout.write("... Waiting for TokenFile return ..." + str(j) + ' seconds' + "\r")
                sys.stdout.flush()
                break
            else:
                now = datetime.datetime.now()
                j = (now - start).seconds
                sys.stdout.write("... Waiting for TokenFile return ..." + str(j) + ' seconds' + "\r")
                sys.stdout.flush()

        print

        if fileReady('/root/COSGoidDIR/tokeninfo', time_wait0):
            boundaryList = []
            os.popen("awk '{if(NR%1000==0){print}}END{print}' /root/COSGoidDIR/tokeninfo > /root/COSGoidDIR/boundryFile")
            boundaryList = open('/root/COSGoidDIR/boundryFile', 'r').read().strip().split()
            # print boundaryList

            for index in range(len(boundaryList)):
                if index == 0:
                    preGoid = token1st[0]
                    sufGoid = int(boundaryList[index + 1])
                elif index == len(boundaryList) - 1:
                    preGoid = token1st[1]
                    sufGoid = token1st[1] + 1
                else:
                    preGoid = int(boundaryList[index])
                    sufGoid = int(boundaryList[index + 1])
                Process(target=CassandraHandleFork, args=(index, preGoid, sufGoid,)).start()
        else:
            raise ("not exist:/root/COSGoidDIR/tokeninfo")

        while True:
            if dirReady('/root/COSGoidDIR/db', time_wait0):
                os.popen('cat /root/COSGoidDIR/db/list_* > /root/COSGoidDIR/db/list_disorder')
                break
            else:
                continue

        os.popen("sort /root/COSGoidDIR/db/list_disorder > /root/COSGoidDIR/db/list_comb")
        os.popen(
            "diff /root/COSGoidDIR/servinfo /root/COSGoidDIR/db/list_comb | grep '<' | awk '{print $2}' | tee /root/COSGoidDIR/orphaned-goids 2>/dev/null")
        os.popen(
            "diff /root/COSGoidDIR/servinfo /root/COSGoidDIR/db/list_comb | grep '>' | awk '{print $2}' | tee /root/COSGoidDIR/bug-goids 2>/dev/null")




    else:
        os.popen(
            "awk '{if($0~/ \w\w\w\w\w/){print}}' /root/COSGoidDIR/db/list |sort|uniq | tee /root/COSGoidDIR/db/list2 2>/dev/null")
        os.popen("rm -rf /root/COSGoidDIR/db/list")

        os.popen(
            "diff /root/COSGoidDIR/servinfo /root/COSGoidDIR/db/list2 | grep '<' | awk '{print $2}' | tee /root/COSGoidDIR/orphaned-goids 2>/dev/null")
        os.popen(
            "diff /root/COSGoidDIR/servinfo /root/COSGoidDIR/db/list2 | grep '>' | awk '{print $2}' | tee /root/COSGoidDIR/bug-goids 2>/dev/null")

    os.popen("rm -rf /etc/cassandra/default.conf/.cqlshrc")

    return 1


def DBquery(l, n):
    print "DBquery::%s~~~%s" % (len(l), n)
    orphaned_goids = []
    for goid in l:
        cmd = 'cqlsh -e "SELECT * from cos.goid where goid = \'%s\'"' % (goid)
        result = os.popen(cmd).read()
        if '(0 rows)' in result:
            orphaned_goids.append(goid)
    content = '\n'.join(orphaned_goids)
    f = open('/root/COSGoidDIR/db/list_%s' % (n))
    f.write(content)
    f.close()


def usage():
    print "usage: " + "[SYNTAX:] python " + "COSGoid.py "
    sys.exit(1)


if __name__ == '__main__':
    numargs = len(sys.argv) - 1
    if numargs != 0:
        usage()

    else:
        execute()

    sys.exit(1) 
