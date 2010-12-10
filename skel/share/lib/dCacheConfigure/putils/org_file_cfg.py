import os.path
import shutil
import re
import time
import logging
from datetime import datetime
try:
    import hashlib as hasher
except:
    import md5 as hasher

logger = logging.getLogger("dCacheConfigure.putils.org_file_cfg")

class file_cfg:
    def __init__(self, filename):
        if not os.path.isfile(filename):
            self.created_file = True
        else:
            self.created_file = False
        self.originalfile = filename

    def wc_open(self):
        if self.created_file:
            self.workingfile = self.originalfile
            return self.workingfile
        ofp = open(self.originalfile, 'rb')
        odata = ofp.read()
        ohash = hasher.md5()
        ohash.update(odata)
        omd5 = ohash.hexdigest()
        self.workingfile = "%s.%s" % (self.originalfile,omd5)
        shutil.copyfile(self.originalfile,self.workingfile)
        return self.workingfile

    def wc_close(self):
        if self.created_file:
            return
        if str(self.originalfile) == str(self.workingfile):
            return
        ofp = open(self.originalfile, 'rb')
        wfp = open(self.workingfile, 'rb')
        odata = ofp.read()
        wdata = wfp.read()
        ohash = hasher.md5()
        ohash.update(odata)
        omd5 = ohash.hexdigest()
        whash = hasher.md5()
        whash.update(wdata)
        wmd5 = whash.hexdigest()
        if str(omd5) == str(wmd5):
            os.remove(self.workingfile)
            return
        backup_filename = "%s.%s" % (self.originalfile,datetime.now().strftime('%Y-%m-%d_%H-%M-%S'))
        shutil.copyfile(self.originalfile,backup_filename)
        shutil.move(self.workingfile,self.originalfile)


    def wc_get(self):
        return self.workingfile



class file_cfg_kv(file_cfg):
    def kv_set(self,key,value):
        logging.debug( "setting key=%s,value=%s" % (key , value))
        matcheskey = re.compile(str("[ \t]*%s[ \t]*=" %(key)))
        content = []
        set_key_value = False
        wfp = open(self.workingfile, 'rb')
        for line in wfp.readlines():
            line = line.strip()
            if None == matcheskey.match(line):
                content.append(line)
                continue
            splitline = line.split('=')
            ckey = splitline[0].strip()
            cval = ""
            for i in splitline[1:]:
                cval += "=%s" %( i )
            cval = cval.lstrip('=').strip()
            if (cval == value) and (not set_key_value):
                content.append(line)
                set_key_value = True
                continue
            line = "# %s" % (line)
            content.append(line)
            if set_key_value:
                continue
            line = "%s = %s" % (key,value)
            content.append(line)
            set_key_value = True

        # Add key value if not already added
        if not set_key_value:
            line = "%s = %s" % (key,value)
            content.append(line)
        wfp = open(self.workingfile, 'wb')
        for line in content:
            wfp.write(line + '\n')


