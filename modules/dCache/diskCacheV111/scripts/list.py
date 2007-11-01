#
# @(#) $Id: list.py,v 1.1 2002-02-20 23:37:36 cvs Exp $
#
# $Log: not supported by cvs2svn $
# Revision 1.2  2001/10/22 21:54:43  ivm
# Added list.sh, list.py
#
# Revision 1.1  2001/10/22 21:43:15  ivm
# .
#
#

import os
import sys
import string

if __name__=='__main__':
	# usage: python list.py <uid> <gid> <cwd> <pattern>
	uid = string.atoi(sys.argv[1])
	gid = string.atoi(sys.argv[2])
	cwd = sys.argv[3]
	ptrn = sys.argv[4]
	ok = 0
	os.setgid(gid)
	os.setuid(uid)
	p = os.popen('/bin/sh -c "cd %s; /bin/ls %s"' % (cwd, ptrn))
	l = p.readline()
	while l:
		print l
		l = p.readline()
	sys.exit(0)

