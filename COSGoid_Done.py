#!/usr/bin/env python
import os,sys
import time,datetime
import socket,subprocess
import fcntl
import struct,mmh3
# import pexpect
# import timeout
from multiprocessing import Process,Queue

#! ~~Tips~~~
# awk '/\w+ Object/{if(NF==4){sub(/0x/, "");print $3}}' serverinfo.log.20141225 > file

# sort file > file2

# diff file(much) file_bu(less) | grep '<' | awk '{print $2}' > orphaned-goids
# diff file(much) file_bu(less) | grep '>' | awk '{print $2}' > bug-goids


# cqlsh -e "SELECT * from cos.goid limit 1000" > cqlsh.out
# cqlsh> SELECT goid from cos.goid where token(goid) < token('15499250fefd') and token(goid) > token('154992734848');

#  goid
# --------------
#  154992407d89
#  1549925e4d7a
#  15499274d346
#  15499261af0e
#  154991a633bb
#  154991e2acd2

# (6 rows)
# cqlsh>

#~ ~~END~~~
########SSH logon stuff############
default_passwd = "rootroot"
prompt_firstlogin = "Are you sure you want to continue connecting \(yes/no\)\?"
prompt_passwd = "root@.*'s password:"
prompt_logined = "\[root@.*\]#"
cassandra_prompt = "cqlsh>"
cassandra_printout = "---MORE---"


def Standard_Reply(ssh):
    try:
        #ssh = pexpect.spawn('ssh root@%s' % IP)
        result = ssh.expect([prompt_firstlogin, prompt_passwd, prompt_logined, cassandra_prompt, cassandra_printout, pexpect.TIMEOUT],timeout=2)
        #ssh.logfile = sys.stdout                                            # printout in real time
        ssh.logfile = None
        
        if result == 0:
          ssh.sendline('yes')
          ssh.expect(prompt_passwd)
          ssh.sendline(default_passwd)
          ssh.expect(prompt_logined )
        elif result == 1:
          ssh.sendline(default_passwd)
          ssh.expect(prompt_logined)
        elif result == 2:
          pass
        elif result == 3:
          return (ssh,1)
        elif result == 4:
          return (ssh,0)
        elif result == 5:
          print "ssh timeout"
          sys.exit(0)
        return ssh
    except:
        print 'Mismatch BTW default expect!!'
        return ssh
        sys.exit(0)


def timeout_command(command, timeout):
    """call shell-command and either return its output or kill it
    if it doesn't normally exit within timeout seconds and return None"""
    import signal
    start = datetime.datetime.now()
    print "start subprocess"
    process = subprocess.Popen(command,shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    while process.poll() is None:
    #while fileReady("/root/COSGoidDIR/db/list",5) == False:
      #time.sleep(0.1)
      now = datetime.datetime.now()
      #print "CZ:debug==time::",(now - start).seconds
      j = (now - start).seconds
      sys.stdout.write("... Waiting for cassandra return ..."+str(j)+' seconds'+"\r")
      sys.stdout.flush()
      if (now - start).seconds> timeout:
        os.kill(process.pid, signal.SIGKILL)
        os.waitpid(-1, os.WNOHANG)
    	print
	    print "cz::debug::timeout!!!!!"
        return None
    print
    time.sleep(5)
    return process.stdout.read()


def CassandraHandle():

    cmd1 = 'cqlsh %s' % (LocalIP.strip())
    cmd2 = 'SELECT goid from cos.goid;'
    cmd3 = 'exit;'

    f = open('/root/COSGoidDIR/cassandraGoid.log','w+') 
    DBcontent = ''    

    ssh = pexpect.spawn('ssh root@127.0.0.1')
    Standard_Reply(ssh)

    ssh.sendline(cmd1)
    ssh.expect(cassandra_prompt)
    #ssh.expect(cassandra_prompt)
    ssh.sendline(cmd2)
    while True:
        ssh,result = Standard_Reply(ssh)
        #INCcontent = ssh.before[:-1]
        DBcontent = DBcontent + ssh.before[:-1]
        #print "size::",len(DBcontent)
        #f.write(INCcontent)
        if result == 1:
            break
        ssh.sendline() 
    f.write(DBcontent)
    f.close()

def FileHandle(time_wait0):

    cmd1 = 'echo 1 > /proc/calypso/test/reopen_logfiles'
    cmd2 = 'echo 2 > /proc/calypso/tunables/cm_logserverinfo'
    cmd3 = 'ls -tr /arroyo/log/serverinfo.log* | tail -n 1'

    os.popen(cmd1)
    os.popen(cmd2)
    time.sleep(time_wait0)
    file = os.popen(cmd3).read().strip()
    fileUntilReady(file,time_wait0)

    os.popen("awk '/\w+ Object/{if(NF==4){sub(/0x/, \"\");print\" \"$3}}' %s |sort|uniq | tee %s 2>/dev/null" % (file,"/root/COSGoidDIR/servinfo"))

#option    TokenCreation()   


def TokenCreation(q):
    f = open('/root/COSGoidDIR/servinfo','r')
    content = f.read().split()
    f.close()
    murMurList = []
 
    for i in  content:
        temp = mmh3.hash64(i.strip())[0]
        murMurList.append(temp)
    murMurList.sort()
    #content2 = '\n'.join(murMurList)
    f2 = open('/root/COSGoidDIR/tokeninfo','w+')
    for k in murMurList:
	f2.write('%d\n'%k)
    f2.close()
    q.put([murMurList[0],murMurList[-1]])


def fileUntilReady(file,wTime):
    if os.path.exists(file):
    	while True:
        	p = os.popen("ls -s %s | awk '{print $1}'" % (file)).read()
        	#print "p",p
        	time.sleep(wTime)
        	p1 = os.popen("ls -s %s | awk '{print $1}'" % (file)).read()
        	#print "p1",p1
        	if p==p1:
           	   break
    else:
	return False

    return True

def fileReady(file,wTime=1):
    if os.path.exists(file):
    	p = os.popen("ls -s %s | awk '{print $1}'" % (file)).read()
        #print "p",p
       	time.sleep(wTime)
       	p1 = os.popen("ls -s %s | awk '{print $1}'" % (file)).read()
       	#print "p1",p1
       	if p==p1:
           return True
	else:
	   return False
    else:
	return False


def dirReady(folder,wTime=1):
    if os.path.exists(folder):
    	p = os.popen("du %s | awk '{print $1}'" % (folder)).read()
        #print "p",p
       	time.sleep(wTime)
       	p1 = os.popen("du %s | awk '{print $1}'" % (folder)).read()
       	#print "p1",p1
       	if p==p1:
           return True
	else:
	   return False
    else:
	return False

def CassandraHandleFork(index,preGoid,sufGoid):
    print preGoid
    print sufGoid
    print index
	
    #cmd1 = 'cqlsh -e "SELECT goid from cos.goid where token(goid) <= token(%s) and token(goid) > token(%s)" > /root/COSGoidDIR/db/list_tmp_%s' % (sufGoid,preGoid,index)
    cmd1 = 'cqlsh -e "SELECT goid from cos.goid where token(goid) < %s and token(goid) >= %s" > /root/COSGoidDIR/db/list_tmp_%s' % (sufGoid,preGoid,index)
    print cmd1
    os.popen(cmd1)


    os.popen("awk '{if($0~/ \w\w\w\w\w/){print}}' /root/COSGoidDIR/db/list_tmp_%s |sort|uniq | tee /root/COSGoidDIR/db/list_%s 2>/dev/null" % (index,index))
    os.popen("rm -rf /root/COSGoidDIR/db/list_tmp_%s" % (index))
    return

def execute():   
    os.popen("rm -rf /root/COSGoidDIR/db 2>/dev/null")
    os.popen("rm -rf /root/COSGoidDIR 2>/dev/null")
    os.popen("mkdir /root/COSGoidDIR 2>/dev/null")
    os.popen("mkdir /root/COSGoidDIR/db 2>/dev/null")

    #LocalIP = get_ip_address('eth0')
    LocalIP = os.popen("ifconfig | grep -A 1 'eth0'|awk '/inet addr/{print $2}'").read().split(':')[-1]
    DB_Conmplex_Handle = False


    #os.popen("awk '{if($0~/^hostname = /){print \"hostname = 10.74.17.89\";}else{print $0;}}' /etc/cassandra/default.conf/cqlshrc.sample > /etc/cassandra/default.conf/.cqlshrc")
    #os.popen("service cassandra restart")
    #time.sleep(20)
    cmd1 = "cqlsh -e 'SELECT goid from cos.goid' > /root/COSGoidDIR/db/list"
    result = timeout_command(cmd1,1)

    if result == None:
        DB_Conmplex_Handle = True
        time_wait0 = 5
    else:
        DB_Conmplex_Handle = False
        time_wait0 = 2


    # "TBD-cpu1"  // servfile handling
    p1 = Process(target=FileHandle, args=(time_wait0,))
    p1.start()

    #cqlsh cmd setting

    start = datetime.datetime.now()
    while True:
     	if fileReady('/root/COSGoidDIR/servinfo',time_wait0):
	#if os.path.exists("/root/COSGoidDIR/servinfo") == True and os.path.getsize("/root/COSGoidDIR/servinfo") != 0:
		now = datetime.datetime.now()
   		j = (now - start).seconds
          	sys.stdout.write("... Waiting for cserver return ..."+str(j)+' seconds'+"\r")
    		sys.stdout.flush()
		#print "####:",os.path.getsize("/root/COSGoidDIR/servinfo") 
		break
        else:
	     	now = datetime.datetime.now()
	    	j = (now - start).seconds
      		sys.stdout.write("... Waiting for cserver return ..."+str(j)+' seconds'+"\r")
		sys.stdout.flush()

    print 		




    if DB_Conmplex_Handle == True:
        print "Start Multiple processiong"
#option while True:
#option     if fileReady('/root/COSGoidDIR/tokeninfo',time_wait0):
#option        	boundaryList = []
#option        	os.popen("awk '{if(NR%1000==0){print}}END{print}' /root/COSGoidDIR/tokeninfo > /root/COSGoidDIR/boundryFile")
#option        	boundaryList = open('/root/COSGoidDIR/boundryFile','r').read().strip().split()
#option        	print boundaryList
#option
#option        	for index in range(len(boundaryList)):
#option        		if index == 0:
#option        			preGoid = '\''+'0'+'\''
#option        			sufGoid = '\''+boundaryList[index+1]+'\''
#option        		elif index == len(boundaryList)-1:
#option        			continue
#option        		else:
#option        			preGoid = '\''+boundaryList[index]+'\''
#option        			sufGoid = '\''+boundaryList[index+1]+'\''
#option        			
#option        		Process(target=CassandraHandleFork, args=(index,preGoid,sufGoid,)).start()
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
	q = Queue()
        p2 = Process(target=TokenCreation,args=(q,))
        p2.start()
	token1st = q.get()

        start = datetime.datetime.now()
        while True:
            if fileReady('/root/COSGoidDIR/tokeninfo',time_wait0):
                now = datetime.datetime.now()
                j = (now - start).seconds
                sys.stdout.write("... Waiting for TokenFile return ..."+str(j)+' seconds'+"\r")
                sys.stdout.flush()
                break
            else:
                now = datetime.datetime.now()
                j = (now - start).seconds
                sys.stdout.write("... Waiting for TokenFile return ..."+str(j)+' seconds'+"\r")
                sys.stdout.flush()

        print

        if fileReady('/root/COSGoidDIR/tokeninfo',time_wait0):
            boundaryList = []
            os.popen("awk '{if(NR%1000==0){print}}END{print}' /root/COSGoidDIR/tokeninfo > /root/COSGoidDIR/boundryFile")
            boundaryList = open('/root/COSGoidDIR/boundryFile','r').read().strip().split()
            print boundaryList

            for index in range(len(boundaryList)):
             if index == 0:
                 preGoid = token1st[0]
                 sufGoid = int(boundaryList[index+1])
             elif index == len(boundaryList)-1:
                 preGoid = token1st[1]
		 sufGoid = token1st[1]+1
             else:
                 preGoid = int(boundaryList[index])
                 sufGoid = int(boundaryList[index+1])
             Process(target=CassandraHandleFork, args=(index,preGoid,sufGoid,)).start()
        else:
            raise("not exist:/root/COSGoidDIR/tokeninfo")

	while True:
	    if dirReady('/root/COSGoidDIR/db',time_wait0):
		os.popen('cat /root/COSGoidDIR/db/list_* > /root/COSGoidDIR/db/list_disorder')
		break
	    else:
		continue

	os.popen("sort /root/COSGoidDIR/db/list_disorder > /root/COSGoidDIR/db/list_comb")
        os.popen("diff /root/COSGoidDIR/servinfo /root/COSGoidDIR/db/list_comb | grep '<' | awk '{print $2}' | tee /root/COSGoidDIR/orphaned-goids 2>/dev/null")
        os.popen("diff /root/COSGoidDIR/servinfo /root/COSGoidDIR/db/list_comb | grep '>' | awk '{print $2}' | tee /root/COSGoidDIR/bug-goids 2>/dev/null")

	
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#	orphaned_goids = []  
#	    
#        content = open('/root/COSGoidDIR/servinfo','r').read().split()
#        num = len(content)/1000
#        jobs = []
#
#        for index in range(num):
#
#            if index*1000 == len(content):
#                pre = index*1000
#                suf = pre+1
#
#            elif (index+1) > len(content):
#                pre = index*1000
#                suf = len(content)+1
#                
#            else:
#                pre = index*1000
#                suf = (index+1)*1000 
#
#            g = Process(target=DBquery, args=(content[pre:suf],index))
#            jobs.append(g)
#            g.start()


    else:
    	os.popen("awk '{if($0~/ \w\w\w\w\w/){print}}' /root/COSGoidDIR/db/list |sort|uniq | tee /root/COSGoidDIR/db/list2 2>/dev/null")
    	os.popen("rm -rf /root/COSGoidDIR/db/list")
		
        os.popen("diff /root/COSGoidDIR/servinfo /root/COSGoidDIR/db/list2 | grep '<' | awk '{print $2}' | tee /root/COSGoidDIR/orphaned-goids 2>/dev/null")
        os.popen("diff /root/COSGoidDIR/servinfo /root/COSGoidDIR/db/list2 | grep '>' | awk '{print $2}' | tee /root/COSGoidDIR/bug-goids 2>/dev/null")


    os.popen("rm -rf /etc/cassandra/default.conf/.cqlshrc")

    return 1


def DBquery(l,n):
    print "DBquery::%s~~~%s" % (len(l),n)
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
