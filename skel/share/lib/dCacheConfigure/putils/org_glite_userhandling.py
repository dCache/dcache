# This class is for handling users from the user format.

# For users file
#  UID = user ID
# LOGIN = login name
# GID1 = primary group ID
# GID2 = secondary group ID
# GROUP1 = primary group
# GROUP2 = secondary group
# VO = virtual organization
# FLAG = string to identify special users, further described below
#  sgm - sgm user (with write permission on the shared software area)
# prd - prd user (with production manager privileges, if needed)
# dpm - dpmmgr (the server account owning files written under "/dpm")
import os
import logging

logger = logging.getLogger("dCacheConfigure.putils.org_glite_userhandling")

class user_handling:
    def __init__(self, filename):
        self.users = []
        fp = open(filename,'r')
        lineno = 0
        for line in fp:
            lineno += 1
            linesplit = line.split(":")
            linelen = len(linesplit)
            if linelen < 6:
                logger.warning("Parsing file '%s' skipping line '%s'" %(filename,lineno))
                continue
            try:
                uid = int(linesplit[0].strip())
                login = str(linesplit[1].strip())
                gid_list = linesplit[2].split(',')
                gid_len = len(gid_list)
                group_list = linesplit[3].split(',')
                group_len = len(group_list)
                if group_len != gid_len:
                    logger.warning("Parsing file '%s' skipping line '%s' gid list and group list different lengths" %(filename,lineno))
                    continue
                vo = str(linesplit[4].strip())
                flags = str(linesplit[5].strip())
                gid_listcleaned = []
                for gid in gid_list:
                    cgid = int(gid.strip())
                    gid_listcleaned.append(cgid)
            except ValueError:
                logger.warning("Parsing file '%s' skipping line '%s' as an error was detected" %(filename,lineno))
                continue
            content = {
                'UID':uid,
                'LOGIN':login,
                'GIDS':gid_listcleaned,
                'GROUPS':group_list,
                'VO':vo,
                'FLAGS':flags,
                'LOGIN':login
            }
            self.users.append(content)

    def get_by_name(self,name):
        '''
        Gets the user records by name
        '''
        output = []
        for content in self.users:
            if content['LOGIN'] == name:
                output.append(content)
        return output

    def get_by_vo(self,vo):
        '''
        Gets the user records by vo
        By convention the first UID is most important.
        '''
        output = []
        for content in self.users:
            if content['VO'] == vo:
                output.append(content)
        return output



import unittest
class Test_user_handling(unittest.TestCase):
    def setUp(self):
        filename = "/root/users.conf"
        self.user_handling = user_handling(filename)
    def testInsufficientArgs(self):
        foo = "sdsdsD"
        self.failUnlessRaises(IOError,user_handling , foo)
    def test_get_by_name(self):
        login = "dteam001"
        foundIds = self.user_handling.get_by_name(login)
        self.assertTrue(1 == len(foundIds),"didnt find a record for %s" % (login))
    def test_get_by_voflags(self):
        vo = "dteam"
        foundIds = self.user_handling.get_by_vo(vo)
        self.assertTrue(0 < len(foundIds),"didnt find a record for %s" % (vo))

if __name__ == '__main__':
    unittest.main()
