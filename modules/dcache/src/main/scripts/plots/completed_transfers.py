#!/usr/bin/env python

import sys
import string
import time
import calendar
import os
import os.path
import stat
import re

import make_calendar

#
#  This routine will parse a billings file containing the
# list of completed dCache transfers.  This file is in the
# following format -
#
#  date time [request] [pnfs id,file size]

# if file size is 0, then the job was a request to see if the
# file was on disk or not.
#  date timestamp [cellType:cellName@Domain/doorName:action] [pnfsId,size] fileFamilyishConcept transferSize(transfer only) transferTime(ms) true|false [protocol,requestingNode:port] {errno:errnoTextString}
# or
#  07.23 05:32:54 [pool:r-cdfdca2-1@cdfdca2Domain:transfer] [00010000000000000016A448,581798] cdf.sgi2test@enstore 581798 121 false {DCap-3.0,cdfensrv3.fnal.gov:4910} {0:""}
# action can be one of the following -
#         transfer - from dCache <--> user
#         store - from dCache --> tape
#                      (no protocol,requestingNode:port info)
#         request - checking if file is in cache
#                      (no fileFamilyishConcept or
#                       protocol,requestingNode:port info)
#         restore - from tape --> dCache
#                      (no protocol,requestingNode:port info)
#
# from this line, the following information will be displayed
# on the web page -
#
#      date, timestamp, cellName, action, translated pnfsId,
#      fileFamilyishConcept, transferSize, new, protocol and
#      requestingNode

REQUEST = 'request'
RESTORE = 'restore'
STORE = 'store'
TRANSFER = 'transfer'

def changeToSecs(milliseconds):
    return "%.1f"%(milliseconds/1000.0,)

class BillingLine:

    DCAP = 'DCap'
    GFTP = 'GFtp'
    FTP = 'Ftp'

    def parse_cellInfo(self, cellInfo):
        ci = string.replace(cellInfo, "[", "")
        ci = string.replace(ci, "]", "")
        ci_l = string.split(ci, ":")
        try:
            if not string.find(ci_l[1], "@") == -1:
                # now split out domain from 2nd element
                ci_l[1] = string.split(ci_l[1], "@")
            else:
                ci_l[1] = [ci_l[1], ""]
        except:
                ci_l = ["parse-error", ["parse-error", "parse-error"],"parse-error"]
        return ci_l

    def parse_pnfsInfo(self, pnfsInfo):
        pi = string.replace(pnfsInfo, "[", "")
        pi = string.replace(pi, "]", "")
        return string.split(pi, ",")

    def parse_ffLike(self, ffLike):
        return string.replace(ffLike, "@enstore", "")

    def parse_protocol(self, protocol):
        prot = string.replace(protocol, "{", "")
        prot = string.replace(prot, "}", "")
        return string.replace(prot, ".0", "")

    def parse_dcap_protocolInfo(self, protocolInfo):
        pi_l = string.split(protocolInfo, ",")
        prot = self.parse_protocol(pi_l[0])
        node, port = string.split(pi_l[1], ":")
        return prot, node, port

    def parse_port(self, port):
        return string.replace(port, "}", "")

    def parse_error(self, error):
        if len(error) > 1:
            return error[1]
        else:
            return ""

    def get_timestamp(self):
        month, day = string.split(self.date, '.')
        month_abbrev = calendar.month_abbr[int(month)]
        return "%s %s %s"%(month_abbrev, day, self.time)

    def __init__(self, line):
        self.state = 1
        self.error = ''                 # VP
        tmp_s = string.strip(line)      # remove any carriage return
        fields = string.split(tmp_s)
        self.date = fields[0]
        self.time = fields[1]
        self.hour = int(self.time[0:2])
        self.timestamp = self.get_timestamp()
        self.cellType, [self.cellName, self.cellDomain],\
                       self.action = self.parse_cellInfo(fields[2])
        if self.cellType == "PoolManager":
            self.state = 0
            return
        try:
            self.pnfsId, self.size = self.parse_pnfsInfo(fields[3])
        except:
            print "Trouble parsing pnfsInfo", line
            self.state = 0
            return
        self.ffLike = self.parse_ffLike(fields[4])
        if self.action in [STORE, RESTORE]:
            self.transfer_size = ""
            self.transfer_time = changeToSecs(long(fields[5]) + long(fields[6]))
            self.new = ""
            self.protocol = ""
            self.node = ""
            self.port = ""
            self.error = self.parse_error(fields[7])
        elif self.action in [TRANSFER]:
            self.transfer_size = fields[5]
            self.transfer_time = changeToSecs(long(fields[6]))
            self.new = fields[7]
            # the rest of the fields are formatted differently
            # depending on the protocol
            if not string.find(fields[8], self.DCAP) == -1:
                # this is DCap protocol
                self.protocol, self.node, \
                               self.port = self.parse_dcap_protocolInfo(fields[8])
                self.unknown = fields[9]                
                self.error = self.parse_error(fields[9])          # VP
            elif not string.find(fields[8], self.GFTP) == -1:
                # this is GFtp protocol
                self.protocol = self.parse_protocol(fields[8])
                self.node = fields[9]
                self.port = self.parse_port(fields[10])
                self.error = self.parse_error(fields[11])
            elif not string.find(fields[8], self.FTP) == -1:
                # this is Ftp protocol
                self.protocol = self.parse_protocol(fields[8])
                self.node = ""
                self.port = ""
                self.error = self.parse_error(fields[9])
            else:
                # unknown protocol
                self.new = ""
                self.protocol = ""
                self.node = ""
                self.port = ""
                self.error = ""

        else:
            # unknown action
            self.transfer_size = ""
            self.transfer_time = ""
            self.new = ""
            self.protocol = ""
            self.node = ""
            self.port = ""
            self.error = ""

    def __repr__(self):
        return "%s %s %s %s %s %s %s %s %s %s"%(self.date, self.time, self.cellName,
                                                self.action, self.pnfsId, self.ffLike,
                                                self.transfer_size, self.new,
                                                self.protocol, self.node)


class File:

    def __init__(self, filename):
        self.filename = filename
        self.tmp_filename = filename
        self.openfile = 0
        self.fd = 0
        self.lines_l = []

    def open(self, mode='r'):
        if not self.openfile:
            self.fd = open(self.tmp_filename, mode)
            self.openfile = 1

    def read(self):
        self.open()
        self.lines_l = self.fd.readlines()

    def write(self, data):
        self.fd.write(data)

    def close(self):
        if self.openfile:
            self.fd.close()
            self.openfile = 0

    def install(self):
        if not self.filename == self.tmp_filename:
            os.system("mv %s %s"%(self.tmp_filename, self.filename))


class BillingFile(File):
    
    def __init__(self, filename):
        File.__init__(self, filename)
        self.parsed_hour_d = {0:[], 1:[], 2:[], 3:[], 4:[], 5:[], 6:[],
                              7:[], 8:[], 9:[], 10:[], 11:[], 12:[],
                              13:[], 14:[], 15:[], 16:[], 17:[], 18:[],
                              19:[], 20:[], 21:[], 22:[], 23:[]}

    def parse(self):
        for line in self.lines_l:
            bl = BillingLine(line)
            if bl.state == 1:
                self.parsed_hour_d[bl.hour].append(bl)

    def get_parsed_lines(self):
        return self.parsed_hour_d


def date_sort(one, two):
    if one.timestamp < two.timestamp:
        return -1
    elif one.timestamp > two.timestamp:
        return 1
    else:
        return 0


class BillingHtmlFileHour(File):
    
    HEADER = ['<html>\n <head>\n  <title>Completed dCache Transfers for ',
              '</title>\n </head>\n <body background="bg.svg">\n  <table border=0 cellpadding=10 cellspacing=0 width="90%">\n  <tr><td align=center valign=center width="1%">\n<a href="',
              '"><img border=0 src="eagleredtrans.gif"></a><br><font color=red>dCache Home</font></td>\n  <td align=center colspan=7><h1>Completed Transfers</h1></td></tr>\n  <tr><td colspan=2><B>',
              '</B></td></tr>\n  <tr><td colspan=2>&nbsp;</td></tr>\n  <tr><font size="+2"><th>Time</th><th>SG.FF</th><th>Node</th><th>Operation</th><th>PNFS Id/File</th><th>Transfer Size</th><th>Transfer Time</th><th>Protocol</th><th>Cell Name</th></font></tr>\n']

    TRAILER = '  </table>\n </body>\n</html>'

    def __init__(self, filename, url, hour):
        File.__init__(self, filename)
        self.tmp_filename = "%s.tmp"%(filename,)
        self.url = url
        self.hour = hour
        self.date = make_calendar.get_date(self.filename, make_calendar.ASCII)

    def open(self):
        File.open(self, 'w')
        self.write('%s%s%s%s%s%s%s'%(self.HEADER[0], self.date, self.HEADER[1],
                                     self.url, self.HEADER[2],
                                     time.ctime(time.time()), self.HEADER[3]))

    def close(self):
        self.write(self.TRAILER)
        File.close(self)


# given a directory get a list of the files and their sizes
def get_file_list(dir, needed_str):
    logfiles = []
    if dir == '':
        dir = '.'
    files = os.listdir(dir)
    re_obj = re.compile(needed_str)
    # pull out the files and get their sizes
    for file in files:
        # needed_str should be in the files' name 
        if re_obj.match(file):
            logfiles.append(file)
    return logfiles

def split_dir(str):
    if os.path.isdir(str):
        return "", str
    else:
        # strip off the last set of chars after the last /
        file_spec = os.path.split(str)
        return file_spec[0], file_spec[1]

    
def remove_dir(str):
    if os.path.isdir(str):
        return str
    else:
        # strip off the directory
        file_spec = os.path.split(str)
        return file_spec[-1]


class PNFSCache(File):

    def read(self):
        File.read(self)
        for line in self.lines_l:
            l = string.strip(line)
            pnfsid, filename = string.split(l)
            self.cache[pnfsid] = filename

    def __init__(self, filename):
        File.__init__(self, filename)
        self.cache = {}
        if os.path.exists(self.filename):
            self.open()
            self.read()
            self.close()
        self.open('a')

    def translate_pnfsid(self, pnfsid):
        filename = ""
        lines = os.popen('. /usr/local/etc/setups.sh; setup enstore; cd /pnfs/fs;enstore pnfs --bfid %s | xargs enstore file --bfid '%(pnfsid,)).readlines()
        for line in lines:
            #if not string.find(line, self.PNFS_NAME0) == -1:
            if not string.find(line, "pnfs_name0") == -1:
                # found the filename
                l = string.strip(line)
                l_l = string.split(l, ":")
                filename = string.split(l_l[1], "'")[1]               
        return filename

    def find_filename(self, pnfsid):
        return "noname"
        # we are keeping a cache file of translated pnfs ids, check
        # if this pnfs id is in the cache before we go and get it.
        if self.cache.has_key(pnfsid):
            return self.cache[pnfsid]
        else:
            filename = self.translate_pnfsid(pnfsid)
            self.cache[pnfsid] = filename
            if filename:
                self.write("%s %s\n"%(pnfsid, filename))
                self.fd.flush()
            return filename
    
class BillingHtmlFile(File):

    HEADER = '<HTML>\n<HEAD>\n        <TITLE>Completed dCache Transfers</TITLE>\n</HEAD>\n<BODY BACKGROUND="bg.svg">\n<TABLE align="LEFT" cellpadding="0" cellspacing="0"><TR><TD align=left valign=center width="1%"><a href="http://cdfdca:443"><img border=0 src="eagleredtrans.gif"></a><br><font color=red>dCache Home</font></TD></TR><TR><TD>\n'
    TRAILER = '\n</TD></TR>\n</TABLE>\n</BODY> </HTML>\n'
    TRUE = 'true'
    PNFS_NAME0 = 'pnfs_name0'

    ACTION_TRANSLATION_TABLE = {REQUEST : "dCache check",
                                RESTORE : "Tape -> dCache restore",
                                STORE : "dCache store -> Tape",
                                TRANSFER : ["User -> dCache mover", "dCache mover -> User"]}

    def __init__(self, dir, filename, fileURL, url, pnfsCacheFile):
        File.__init__(self, "%s/%s"%(dir, filename))
        self.tmp_filename = "%s/%s.tmp"%(dir, filename,)
        self.url = url
        self.dir = dir
        self.fileURL = fileURL
        self.pnfsCacheFile = pnfsCacheFile

    def open(self):
        File.open(self, 'w')

    def translate_action(self, action, new):
        # if new == 'true', then this was a write to dcache, else
        # a read
        xact = self.ACTION_TRANSLATION_TABLE.get(action, "")
        if xact:
            if action == TRANSFER:
                if new == self.TRUE:
                    return xact[0]
                else:
                    return xact[1]
            else:
                return xact
        else:
            return action

    def make_td(self, data):
        return "<td>%s</td>\n"%(data,)

    def write(self, billing_l):
        self.pnfs_cache = PNFSCache(self.pnfsCacheFile)
        for billingFile in billing_l:
            # hours_d = {hour1: [line1, line2...]...}
            hours_d = billingFile.get_parsed_lines()
            for hour in hours_d.keys():
                if hours_d[hour]:
                    lines = hours_d[hour]
                    htmlBillingFileName = "%s.%s.html"%(billingFile.filename, hour)
                    htmlBillingFile = remove_dir(htmlBillingFileName)
                    hbf = BillingHtmlFileHour("%s/%s"%(self.dir, htmlBillingFile), self.url, hour)
                    hbf.open()
                    lines.sort(date_sort)
                    for line in lines:
                        if not line.action == REQUEST:
                            if line.action in [RESTORE, STORE]:
                                size = line.size
                            else:
                                size = line.transfer_size
                            hbf.write("<tr>%s%s%s%s%s%s%s%s%s</tr>"%(self.make_td(line.timestamp),
                                                                     self.make_td(line.ffLike),
                                                                     self.make_td(line.node),
                                                                     self.make_td(self.translate_action(line.action,
                                                                                                        line.new)),
                                                                     self.make_td("%s<BR>%s"%(line.pnfsId,
                                                                                              self.pnfs_cache.find_filename(line.pnfsId))),
                                                                     self.make_td(size),
                                                                     self.make_td(line.transfer_time),
                                                                     self.make_td(line.protocol),
                                                                     self.make_td(line.cellName)))
                    else:
                        hbf.close()
                        hbf.install()
        else:
            self.pnfs_cache.close()
            # now write the file with all the links to the
            # separate files we just created
            file_l = get_file_list(self.dir, "%s....\...\...\..*?\%s"%(make_calendar.BILLING, make_calendar.HTML))
            self.open()
            cal = make_calendar.HtmlCalendar(self.fd, self.fileURL)
            self.fd.write(self.HEADER)
            self.fd.write("<BR>%s<BR></TD></TR>"%(time.ctime(time.time())))
            cal.write(file_l)
            self.fd.write(self.TRAILER)
            

if __name__ == "__main__" :

    # args are the name of the html file to generate and a
    # list of billing files.
    if len(sys.argv) < 5:
        print "USAGE: %s htmlFileName htmlFileURL dCacheHomeURL pnfsCacheFile billingFile [billingFile ...]"%(sys.argv[0])
    else:
        billing_l = []
        htmlFilePath = sys.argv[1]
        # split into directory and filename
        dir, htmlFile = split_dir(htmlFilePath)
        if dir == "":
            dir = "."
        htmlFileURL = sys.argv[2]
        homeURL = sys.argv[3]
        pnfsCacheFile = sys.argv[4]
        billingFile_l = sys.argv[5:]
        billingFile_l.sort()
        for billingFile in billingFile_l:
            print billingFile
            # get the last mod time for the html file associated with
            # this billing file.  if the time is after the last mod time
            # of the billing file, then the html file is up-to-date and
            # we can skip it.
            try:
                file = remove_dir(billingFile)
                bfHtmlMTime = os.stat("%s/.%s"%(dir, file))[stat.ST_MTIME]
            except OSError:
                # the timestamp file did not exist
                bfHtmlMTime = 0
            try:
                bfMTime = os.stat(billingFile)[stat.ST_MTIME]
                if bfHtmlMTime < bfMTime:
                    bf = BillingFile(billingFile)
                    bf.read()
                    bf.close()
                    bf.parse()                           # parse each line
                    billing_l.append(bf)
            except OSError:
                # the billing file did not exist, ignore this one
                pass
        else:
            #VP
            fileDaily = None
            #          Jun Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec
            days = [0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
            #
            for bfile in billing_l:
                fileName = os.path.basename(bfile.filename)
                nameElem = fileName.split('.')

                monthNo = nameElem[1]
                dayNo   = nameElem[2]

                nameDaily = "%s.%s.daily" % (nameElem[0], monthNo)

                nameHourly = "%s.hourly" % fileName
                fileHourly = open(nameHourly, 'w')

                parsedLines = bfile.get_parsed_lines()
                hours = parsedLines.keys()
                hours.sort()
                
                sumtoDCdaily = 0.0; sumfrDCdaily = 0.0; sumtoENdaily = 0.0; sumfrENdaily = 0.0
                totaltoDCdaily = 0.0; totalfrDCdaily = 0.0

                cntrtoDCdaily = 0; cntrfrDCdaily = 0; cntrtoENdaily = 0; cntrfrENdaily = 0

                for hour in hours:
                    oneHourList = parsedLines[hour]
                    sumtoDChourly = 0.0; sumfrDChourly = 0.0; sumtoENhourly = 0.0; sumfrENhourly = 0.0
                    for item in oneHourList:
                        if item.error != 0:
                            continue
                        # print "%s %s %s %s %s" % (item.date, item.time, item.action, item.transfer_size, item.new)
                        if item.action == 'store':
                            sumtoENhourly += float(item.size)
                            sumtoENdaily += float(item.size)
                            cntrtoENdaily += 1
                        elif item.action == 'restore':
                            sumfrENhourly += float(item.size)
                            sumfrENdaily += float(item.size)
                            cntrfrENdaily += 1
                        elif item.action == 'transfer' and item.new == 'false':
                            sumfrDChourly += float(item.transfer_size)
                            sumfrDCdaily += float(item.transfer_size)
                            totalfrDCdaily += float(item.size)
                            cntrfrDCdaily += 1
                        elif item.action == 'transfer' and item.new == 'true':
                            sumtoDChourly += float(item.transfer_size)
                            sumtoDCdaily += float(item.transfer_size)
                            totaltoDCdaily += float(item.size)
                            cntrtoDCdaily += 1

                    fileHourly.write("%d %f %f %f %f\n" % (hour, sumtoDChourly, sumfrDChourly, sumtoENhourly, sumfrENhourly))

                fileHourly.close()

                if fileDaily == None:
                        fileDaily = open(nameDaily, 'a')
                        # fileDaily.write("# Date sumtoDCdaily sumfrDCdaily sumtoENdaily sumfrENdaily totaltoDCdaily totalfrDCdaily\n")
                else:
                    if nameDaily != fileDaily.name:
                        fileDaily.close()
                        fileDaily = open(nameDaily, 'a')
                        # fileDaily.write("# Date sumtoDCdaily sumfrDCdaily sumtoENdaily sumfrENdaily totaltoDCdaily totalfrDCdaily\n")
                        
                fileDaily.write("%s-%s %f %f %f %f %f %f ( %d %d %d %d )\n" % (monthNo, dayNo,
                                                                               sumtoDCdaily, sumfrDCdaily,
                                                                               sumtoENdaily, sumfrENdaily,
                                                                               totaltoDCdaily, totalfrDCdaily, 
                                                                               cntrtoDCdaily, cntrfrDCdaily,
                                                                               cntrtoENdaily, cntrfrENdaily))

            # for dd in range(int(dayNo), days[int(monthNo)]+1):
            #    fileDaily.write("%s-%02d  0  0  0  0  0  0 (  0  0  0  0 )\n" % (monthNo, dd))

            if fileDaily != None:
                fileDaily.close()
            #VP
##VP
##            billing_l.sort()
##            htmlFile = BillingHtmlFile(dir, htmlFile, htmlFileURL, homeURL, pnfsCacheFile)
##            htmlFile.write(billing_l)
##            htmlFile.close()
##            htmlFile.install()
##VP
            # now make the timestamp files for all the billing files
            # we read
            for bf in billing_l:
                file = remove_dir(bf.filename)
                os.system("touch %s/.%s"%(dir, file))
 
