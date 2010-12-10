import re
import commands
import logging
logger = logging.getLogger("dCacheConfigure.parse_site_info_def")

ignoredkeys = ["SHELLOPTS"]

def checkfile(filename):
    cmd = ". %s" % (filename)
    status,output = commands.getstatusoutput(cmd)
    if status != 0:
        raise siteinfodef("Error parsing '%s':%s" % (filename,output))
    parseerror = False
    for line in output.split('\n'):
        if line != "":
            parseerror = True
    if parseerror:
        raise siteinfodef("Error parsing '%s' it produced the following output :\n%s" % (filename,output))
    return True

def get_keys(filename):
    ws_pattern = re.compile("[ \t]")
    cmd = ". %s &&  set" % (filename)
    status,output = commands.getstatusoutput(cmd)
    if status != 0:
        raise siteinfodef("Error parsing '%s':%s" % (filename,output))
    allkeys = set([])
    for line in output.split('\n'):
        splitline = line.split('=')
        if len(splitline) < 2:
            continue
        if None == ws_pattern.search(splitline[0]):
            if splitline[0] in ignoredkeys:
                continue
            allkeys.add(splitline[0])
    return allkeys



def key_get_var(filename,variable):
    cmd = ". %s ;  echo ${%s}" % (filename,variable)
    env = commands.getstatusoutput(cmd)
    if env[0] == 0:
        return env[1]
    else:
        raise p

def keys_get_var(filename,keys):
    output = {}
    cmd = ". %s " % (filename)
    for key in keys:
        cmd += "\necho %s=@${%s}@" % (key,key)
    status,coutput = commands.getstatusoutput(cmd)
    if status != 0:
        raise siteinfodef("Error parsing '%s':%s" % (filename,output))
    for line in coutput.split('\n'):
        splitline = line.split('=@')
        if len(splitline) < 2:
            continue
        thiskey = splitline[0]
        value = splitline[1]
        if value[-1] == '@':
            if thiskey != "":
                output[str(thiskey)] = str(value[:-1])
    return output

def parse_site_info(filename):
    checkfile(filename)
    keys = get_keys(filename)
    return keys_get_var(filename,keys)
