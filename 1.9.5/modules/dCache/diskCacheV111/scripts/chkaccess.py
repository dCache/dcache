#
# @(#) $Id: chkaccess.py,v 1.1 2002-02-20 23:37:36 cvs Exp $
#
# $Log: not supported by cvs2svn $
# Revision 1.1  2001/09/20 02:43:02  ivm
# Initial CVS deposit
#
#

import os
import sys
import string
import stat

if __name__=='__main__':
	# usage: python chkaccess.py <uid> <gid> <access> <path>
	# access: r,w,x,c
	# c means check if file <path> can be created
	uid = string.atoi(sys.argv[1])
	gid = string.atoi(sys.argv[2])
	access = sys.argv[3]
	path = sys.argv[4]
	ok = 0
	os.setgid(gid)
	os.setuid(uid)
	if access == 'r':
		ok = os.access(path, os.R_OK)
	elif access == 'w':
		ok = os.access(path, os.W_OK)
	elif access == 'x':
		ok = os.access(path, os.X_OK)
	elif access == 'c':
		if path == '/':
			pass
		else:
			lst = string.split(path,'/')[:-1]
			if not lst:
				lst = ['.']
			path = string.join(lst,'/')
		#print 'dirpath=', path
		try:	st = os.stat(path)
		except:	pass
		else:
			ok = stat.S_ISDIR(st[stat.ST_MODE]) and \
				os.access(path, os.W_OK)
	sys.exit(not ok)
