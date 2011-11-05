#!/usr/bin/env python

import os
import socket
import string
import sys
import time

Usage = "Usage:  ",sys.argv[0]," node sshPort outputFile"

DEBUG=1

try:
    node = sys.argv[1]
    sshPort = sys.argv[2]
    output = open(sys.argv[3]+".temp",'w')
except:
    print Usage
    sys.exit(1)

processes=[]
hostnames={}

s_unknown = '-unknown-'
l_s_unknown = len(s_unknown)

zap_cdf='/cdfen/'
l_zap_cdf=len(zap_cdf)

zap_pnfsfs='"/pnfs/fnal.gov/usr'
l_zap_pnfsfs=len(zap_pnfsfs)


dcap_header = "DCap"
l_dcap_header=len(dcap_header)

ftp_header = "FTP"
l_ftp_header = len(ftp_header)

weak_header = "WFTP"
l_weak_header = len(weak_header)

kerb_header = "KFTP"
l_kerb_header = len(kerb_header)

output.write('<html>\n')
output.write('<head>\n')
output.write('<meta HTTP-EQUIV="Refresh" CONTENT="300">\n')
output.write('<title>Recent dCache Transfers</title>\n')
output.write('</head>\n')
output.write('<body background="bg.svg" link=red vlink=red alink=red>\n')
output.write('<table border=0 cellpadding=10 cellspacing=0 width="90%">\n')
output.write('<tr><td align=center valign=center width="1%">\n')
output.write('<a href="http://%s:443"><img border=0 src="eagleredtrans.gif"></a><br><font color=red>dCache Home</font></td>\n'%(node,))
output.write('<td align=center><h1>Active Transfers</h1></td></tr></table>\n')
output.write('<p><h3>%s</h3>'%(time.strftime("%c\n",time.localtime(time.time())),))

output.write('<pre>\n\n')

output.write("\nWell known dcache services on %s:\n"%(node,))
scommand='exit\n\
set dest System@door0Domain\nps -f\nexit\n\
set dest System@door1Domain\nps -f\nexit\n\
set dest System@door2Domain\nps -f\nexit\n\
set dest System@door3Domain\nps -f\nexit\n\
set dest System@door4Domain\nps -f\nexit\n\
set dest System@door5Domain\nps -f\nexit\n\
set dest System@door6Domain\nps -f\nexit\n\
set dest System@door7Domain\nps -f\nexit\n\
set dest System@door8Domain\nps -f\nexit\n\
set dest System@door9Domain\nps -f\nexit\nexit\n '


cmd = 'echo "%s" | ssh -p %s %s 2>/dev/null'%(scommand,sshPort,node)
if DEBUG: print cmd
pipe = os.popen(cmd,'r')
while 1:
    line = pipe.readline()
    if not line: break
    if DEBUG: print line,
    tokens = line.split()
    if len(tokens) == 0: continue
    if len(tokens) >=5 and string.find(tokens[3],'dest') == 0:
        dest=tokens[4]
    if string.find(tokens[-1],'Door') >= 0:
        door = tokens[-1].split('.')[-1]
        port = tokens[-1].split(';')[0].split('=')[-1]
        output.write("\t%s on port %s\n"%(door,port))
    if string.find(tokens[0],'DCap')==0 or \
       string.find(tokens[0],'FTP')>=0:
        if string.find(tokens[0],'-unknown-')>=0:
            processes.append((dest,tokens[0]))
pipe.close()
if DEBUG: print "Transfers found: %s\n\n"%(processes,)

output.write("\n\n")
output.write("%-5s %-5s %-25s %-5s %-15s %-46s %s\n\n"%("Kind", "JobId","Node","Type","Pool","[PNFS Id][File Seq][Client Id][Client Pid]","Status(time-in-state)"))

#   <dCacheJobNumber> 
#   <clientIpNumber>  
#   <requestType=[io/stage/check]>
#   [<assignedPoolName>]
#   [<pnfsId>][<fileSequenceNumber>][<clientUserId>][<clientProcessId>]
#   <currentStatus>(<time spend in this status>)
 
for (dest,process) in processes:
    cell=hostname=kind=pool=IdSeqCidCpid=status=uid=username=door=jobid=started=lastat="Unknown"
    last=[]
    user=[]
    scommand='exit\nset dest %s\nps -f %s\nexit\nexit'%(dest,process)
    cmd = 'echo "%s" | ssh -p %s %s 2>/dev/null'%(scommand,sshPort,node)
    if DEBUG: print cmd
    pipe = os.popen(cmd,'r')
    printed = 1
    get_node = 0
    while 1:
        line = pipe.readline()
        if not line: break
        if DEBUG: print line,
        tokens = line.split()
        if len(tokens) == 0: continue
        if len(tokens)>=5 and tokens[4]=="DCapDoor":
            printed = 0
            get_node = 1
        if len(tokens)>=5 and tokens[4]=="FtpDoorFnal":
            printed = 0
            get_node = 1
        if len(tokens)>=5 and tokens[4]=="FtpDoorFnalWeakAuth":
            printed = 0
            get_node = 1
        if len(tokens)>=5 and tokens[4]=="FtpDoorFnalWeakAuth":
            printed = 0
            get_node = 1
        if get_node:
            get_node = 0
            cell = tokens[0]
            unknown = string.find(cell,s_unknown)
            if unknown!=0:
                door = cell[:unknown]
                jobid = cell[len(door)+l_s_unknown:]
            for (header,l_header) in [(dcap_header,l_dcap_header),(ftp_header, l_ftp_header),(weak_header,l_weak_header),(kerb_header,l_kerb_header)]:
                head = string.find(cell,header)
                if head == 0:
                    cell = cell[l_header:]
                    type = header[0:4]
                    break
            hosta = tokens[5].split("@")
            hostb = hosta[1].split("/")
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

        if tokens[0] == "Started":
            started = ""
            try:
                started = tokens[0]+" "+tokens[3]+" "+tokens[4]+" "+tokens[5]
            except:
                print 'STARTERD PARSE FAILURE:',line
        if tokens[0] == "Last" and tokens[1] == "at":
            lastat = ""
            try:
                lastat = tokens[0]+" "+tokens[1]+" "+tokens[4]+" "+tokens[5]+" "+tokens[6]
            except:
                print 'LAST AT PARSE FAILURE:',line

        if type=="DCap" and len(tokens)>=6 and tokens[1]=="->":
            kind = tokens[2]
            pool = tokens[3]
            pool = string.replace(pool,'[','')
            pool = string.replace(pool,'<','')
            pool = string.replace(pool,'>','')
            pool = string.replace(pool,']','')
            IdSeqCidCpid = tokens[4]
            status = tokens[5]
            output.write("%-5s %-5s %-25s %-5s %-15s %-46s %s\n"%(door,jobid,hostname,kind,pool,IdSeqCidCpid,status))
            printed=1
            break
        if len(tokens)>=3 and tokens[0]=="Last" and tokens[1]=="Command" and tokens[2]==":":
            last = tokens[3:]
            if (len(last)>=9 and last[2]=="client" and (last[3]=="open") or (len(last)>=7 and last[2]=="client" and last[3]=="stage")):
                try:
                    uid = last[-1].split('=')[1]
                except:
                    uid = last[-1]
                try:
                    cmd='grep %s /home/enstore/unix.uid.list'%(uid)
                    if DEBUG: print cmd
                    pipeU = os.popen(cmd,'r')
                    users = pipeU.readlines()
                    pipeU.close()
                    user = users[0].split(':')
                    username = user[2]
                except:
                    username = uid
                if last[3] == "open":
                    last = last[3:8]
                elif last[3] == "stage":
                    last = last[3:5]
                
                zcdf = string.find(last[1],zap_cdf)
                if zcdf >=0:
                    last[1] = last[1][zcdf+l_zap_cdf:-1]
                pnfsfs = string.find(last[1],zap_pnfsfs)
                if pnfsfs>0:
                    last[1] = last[1][pnfsfs:-1]

            if len(last)>=1 and last[0]=="MIC":
                last = last[0:1]
            if len(last)>=1 and last[0]=="ADAT":
                last = last[0:1]
                    
    pipe.close()
    if not printed:
        output.write("%-5s %-5s %-25s [no more info available]\n"%(door,jobid,hostname))

    if len(user)>=4:
        output.write("\t\t %s=%s  %s"%(user[0],user[3][:-1],user[2]))  #lf present on user[3] as last character
    else:
        output.write("\t\t %s=%s  %s"%('unknown','unknown','unknown'))

    if started!="Unknown":
        output.write("\t\t %s"%(started,))
    else:
        output.write("\t\t %s"%('Unknown start time',))
        
    if lastat!="Unknown":
            output.write("\t\t %s\n"%(lastat,))
    else:
            output.write("\t\t %s\n"%('Unknown last at time',))

    if len(last)!=0:
        last_command=""
        for i in range(0,len(last)):
            last_command=last_command+"%s "%(last[i],)
        output.write("\t\t %s\n"%(last_command,))
    output.write("\n")

scommand='exit\n\
set dest Prestager\ninfo -a\nexit\nexit\n '

cmd = 'echo "%s" | ssh -p %s %s 2>/dev/null'%(scommand,sshPort,node)
output.write('\n\n\nPrestager Information\n')
if DEBUG: print cmd
pipe = os.popen(cmd,'r')
while 1:
    line = pipe.readline()
    if not line: break
    if DEBUG: print line,
    if string.find(line,'Dummy')>=0 or string.find(line,' exit')>=0 or string.find(line,' dest ')>=0 \
       or string.find(line,' ssh ')>=0 or string.find(line,' Exit')>=0 or string.find(line,'Back to ')>=0 \
       or string.find(line,' info -a')>=0:
        continue
    if string.find(line,'Outstanding')>=0:
        output.write('%s'%(line[:-1],))
        break
    output.write('%s'%(line[:-1],))
pipe.close()






output.write("</pre>\n")
output.write('<p><p>Finished at %s'%(time.strftime("%c\n",time.localtime(time.time())),))
output.write("</body></html>\n")

output.close()


cmd='mv %s.temp %s'%(sys.argv[3],sys.argv[3])
if DEBUG: print cmd
pipeU = os.popen(cmd,'r')
status =  pipeU.readlines()
pipeU.close()
if DEBUG: print status
