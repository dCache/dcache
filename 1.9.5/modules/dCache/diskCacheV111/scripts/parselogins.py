#!/usr/bin/env python

import os
import socket
import string
import sys
import time

DEBUG=1

processes=[]
hostnames={}

s_unknown = '-unknown-'
l_s_unknown = len(s_unknown)
s_unknown2 = '-Unknown-'
l_s_unknown2 = len(s_unknown2)

dcap_header = "DCap"
l_dcap_header=len(dcap_header)

output = open('/tmp/DOORS.html','w')
koutput =open('/tmp/KILLDOORS','w')
output.write('<html>\n')
output.write('<head>\n')
output.write('<title>dCache Logins</title>\n')
output.write('</head>\n')
output.write('<body background="bg.jpg" link=red vlink=red alink=red>\n')
output.write('<table border=0 cellpadding=10 cellspacing=0 width="90%">\n')
output.write('<tr><td align=center valign=center width="1%">\n')
output.write('<td align=center><h1>Door Logins</h1></td></tr></table>\n')
output.write('<p><h3>%s</h3>'%(time.strftime("%c\n",time.localtime(time.time())),))

output.write('<pre>\n\n')
output.write("\n\n")
output.write("JobId   door     Node                      State  Started         Last Active      UID/PID     Role                         Username                  Pool                      [PNFS Id][Timer][File Seq][Client Id][Client Pid]       Status(time-in-state)            command\n")



 
pipe = open('DOOR','r')
#   <dCacheJobNumber> 
#   <clientIpNumber>  
#   <requestType=[io/stage/check]>
#   [<assignedPoolName>]
#   [<pnfsId>][<fileSequenceNumber>][<clientUserId>][<clientProcessId>]
#   <currentStatus>(<time spend in this status>)

running=0
cell=hostname=kind=kind2=pool=IdSeqCidCpid=status=uid=pid=username=door=jobid=started=lastat=state=what=itype=role="?"
active='idle  '
while 1:
        line = pipe.readline()
        if not line: break
        tokens = line.split()
        if len(tokens) == 0: continue

	if tokens[0] == "UNRESPONSIVE":
		print line[:-1]
		continue
	
	if len(tokens)>=4 and tokens[2]=="ps" and tokens[3]=="-f":
	    if running and cell!='?':
	            print "%10s %-7s %-8s %-25s %-6s %15s %15s %12s %-25s %-25s %-25s %-55s %-32s %s"%(sec_last,jobid,door,hostname,active,started, lastat, uid+'/'+pid, role,username,pool,IdSeqCidCpid,status,what)
		    koutput.write("%3s %s kill %-25s      %s %s %s %s\n"%(door,lastat,cell,active,what,hostname,username))
	    running=1
	    cell=hostname=kind=kind2=pool=IdSeqCidCpid=status=uid=pid=username=door=jobid=started=lastat=state=what=itype=role="?"
	    active='idle  '
	    continue

        if len(tokens)>=5 and tokens[4]=="DCapDoor" or len(tokens)>4 and tokens[3]== "DCapDoor":
	    running=1
            cell = tokens[0]
	    if cell[len(cell)-1] == 'A':
			tmp = cell[:len(cell)-1]
			cell=tmp
	    try:
		    (door,role,jobid) = string.split(cell,'-')
		    itype="DCap"
	    except:
		    (door,role,jobid) = (cell,cell,cell)
            hosta = tokens[len(tokens)-1].split("@")[-1]
            hostb = hosta.split("/")
            if len(hostb[0])==0:
                host = hostb[1]
            else:
                host = hostb[0]
            if hostnames.has_key(host):
                hostname = hostnames[host]
            else:
                try:
                    hostname = socket.gethostbyaddr(host)[0]
                    hostnames[host] = hostname
                except:
                    hostname = host
	    #print "cell",cell, "door",door, "jobid",jobid, "host", hosta,hostb,host, "hostname",hostname, "itype",itype
	    continue

        if tokens[0] == "Started":
            started = ""
            try:
                started = tokens[3]+" "+tokens[4]+" "+tokens[5]
		myyear = tokens[7]
            except:
                print 'STARTED PARSE FAILURE:',line
	    #print "STARTED",started+myyear
	    sec_started = time.mktime(time.strptime(started+myyear,'%b %d %H:%M:%S %Y'))
	    continue
	    
        if tokens[0] == "Last" and tokens[1] == "at":
            lastat = ""
            try:
                lastat = tokens[4]+" "+tokens[5]+" "+tokens[6]
		myyear = tokens[8]
            except:
                print 'LAST AT PARSE FAILURE:',line
	    #print "LAST at",lastat
	    sec_last = time.mktime(time.strptime(lastat+myyear,'%b %d %H:%M:%S %Y'))
	    continue

        if tokens[0] == "pid" and tokens[1] == "=":
            try:
                pid = tokens[2]
            except:
                print 'PID PARSE FAILURE:',line
	    #print "pid=",pid
	    continue

        if itype=="DCap" and len(tokens)>=6 and tokens[1]=="->":
            kind = tokens[2]
            pool = tokens[3]
            pool = string.replace(pool,'[','')
            pool = string.replace(pool,'<','')
            pool = string.replace(pool,'>','')
            pool = string.replace(pool,']','')
            IdSeqCidCpid = tokens[4]+tokens[5]+tokens[6]
            status = tokens[7]
	    active='ACTIVE'
	    #print 'ACTIVE','ACTIVE',kind, pool, IdSeqCidCpid, status
	    continue
	    
        if len(tokens)>=3 and tokens[0]=="Last" and tokens[1]=="Command" and tokens[2]==":":
            last = tokens[3:]
            if (len(last)>=9 and last[2]=="client" and (last[3]=="open") or (len(last)>=7 and last[2]=="client" and last[3]=="stage")):
                try:
                    uid = last[-1].split('=')[1]
                except:
                    uid = last[-1]
		if string.find(hostname,'.fnal.gov')>0:
                 try:
                    cmd='grep %s ~enstore/unix.uid.list'%(uid)
                    #print cmd
                    pipeU = os.popen(cmd,'r')
                    users = pipeU.readlines()
                    pipeU.close()
                    user = users[0].split(':')
                    username = user[2]
		    #print username
                 except:
                    username = uid
		else:
		    username = 'offsite'
                if last[3] == "open":
		    thefile=last[4][:-1]
		    what = 'open  '
		    kind2 ='open '
                elif last[3] == "stage":
		    thefile=last[4][:-1]
		    what = 'stage '
		    kind2 ='stage'
 	        off=string.find(thefile,'cdfen/filesets/')
		if off>0:
                    what=what+thefile[off+15:]
		else:
                    what = what+thefile
              
if running:
	print "%10s %-7s %-8s %-25s %-6s %15s %15s %12s %-25s %-25s %-25s %-55s %-32s %s"%(sec_last,jobid,door,hostname,active,started, lastat, uid+'/'+pid, role,username,pool,IdSeqCidCpid,status,what)
	koutput.write("%3s %s kill %-25s      %s %s %s %s\n"%(door,lastat,cell,active,what,hostname,username))
pipe.close()             




pipe = open('FTPDOOR','r')

running=0
cell=hostname=kind=kind2=pool=IdSeqCidCpid=status=uid=pid=username=door=jobid=started=lastat=state=what=itype=role="?"
active='idle  '
while 1:
        line = pipe.readline()
        if not line: break
        tokens = line.split()
        if len(tokens) == 0: continue

	if tokens[0] == "UNRESPONSIVE":
		print line[:-1]
		continue
	
	if len(tokens)>=4 and tokens[2]=="ps" and tokens[3]=="-f":
	    if running and cell!='?':
	            print "%10s %-7s %-8s %-25s %-6s %15s %15s %12s %-25s %-25s %-5s %-55s %-32s %s"%(sec_last,jobid,door,hostname,active,started, lastat, uid+'/'+pid, role,username,pool,IdSeqCidCpid,status,what)
		    koutput.write("%3s %s kill %-25s      %s %s %s %s\n"%(door,lastat,cell,active,what,hostname,username))
	    running=1
	    cell=hostname=kind=kind2=pool=IdSeqCidCpid=status=uid=pid=username=door=jobid=started=lastat=state=what=itype=role="?"
	    active='      '
	    continue

        if len(tokens)>=6 and string.find(tokens[0],"FTP") > 0:
            o_ftp=string.find(tokens[0],"FTP")
	    started=""
	    lastat=""
	    uid=""
	    pid=""
	    pool=""
	    IdSeqCidCpid=""
	    status=""
	    sec_last=time.time()
	    running=1
            cell = tokens[0]
	    if cell[len(cell)-1] == 'A':
			tmp = cell[:len(cell)-1]
			cell=tmp
            unknown = string.find(cell,s_unknown)
            if unknown>=0:
                jobid = cell[unknown+l_s_unknown:]
		door = cell[o_ftp+3:unknown]
		itype = cell[0:o_ftp+3]
		active = itype
	    else:
	        unknown2 = string.find(cell,s_unknown2)
		if unknown2>=0:
                    jobid = cell[unknown2+l_s_unknown2:]
		    door = cell[o_ftp+3:unknown2]
		    itype = cell[0:o_ftp+3]
		    active = itype
            hosta = tokens[len(tokens)-1].split("@")[-1]
            hostb = hosta.split("/")
            if len(hostb[0])==0:
                host = hostb[1]
            else:
                host = hostb[0]
            if hostnames.has_key(host):
                hostname = hostnames[host]
            else:
                try:
                    hostname = socket.gethostbyaddr(host)[0]
                    hostnames[host] = hostname
                except:
                    hostname = host
	    #print cell,door, jobid, hostname,itype
	    continue

        if tokens[0] == "User" and tokens[1] == ":":
            try:
                username = tokens[2]
            except:
                print 'USER PARSE FAILURE:',line
	    #print "username=",username
	    continue

        if len(tokens)>=3 and tokens[0]=="Last" and tokens[1]=="Command" and tokens[2]==":":
            #print "IN LAST", line
	    if len(tokens)>=4 :
		lastkind=tokens[3]
		kind2=lastkind
		if lastkind=="RETR" or lastkind=="STOR":
		    what = lastkind+" "
		    thefile=tokens[4][:-1]
 	        off=string.find(thefile,'cdfen/filesets/')
		if off>0:
                    what=what+thefile[off+15:]
		else:
                    what = what+thefile
		#print "what=",what
              
if running:
	print "%10s %-7s %-8s %-25s %-6s %15s %15s %12s %-25s %-25s %-25s %-55s %-32s %s"%(sec_last,jobid,door,hostname,active,started, lastat, uid+'/'+pid, role,username,pool,IdSeqCidCpid,status,what)
	koutput.write("%3s %s kill %-25s      %s %s %s %s\n"%(door,lastat,cell,active,what,hostname,username))
pipe.close()             


output.close()
