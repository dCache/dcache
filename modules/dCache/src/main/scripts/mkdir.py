#
# @(#) $Id: mkdir.py,v 1.1 2002-02-20 23:37:36 cvs Exp $
#
# $Log: not supported by cvs2svn $
# Revision 1.1  2001/09/20 02:43:02  ivm
# Initial CVS deposit
#
#

import os
import sys
import string

if __name__=='__main__':
	# usage: python mkdir.py <uid> <gid> <path>
	uid = string.atoi(sys.argv[1])
	gid = string.atoi(sys.argv[2])
	path = sys.argv[3]

	os.setgid(gid)
	os.setuid(uid)
	os.mkdir(path)
	sys.exit(0)
