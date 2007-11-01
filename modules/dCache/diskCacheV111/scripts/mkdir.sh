#!/bin/sh -f
#
# @(#) $Id: mkdir.sh,v 1.2 2002-11-14 20:59:48 cvs Exp $
#
# $Log: not supported by cvs2svn $
# Revision 1.1  2002/02/20 23:37:36  cvs
# Added scripts directory and scripts needed for enstore interaction
#
# Revision 1.3  2001/11/30 21:46:04  wellner
# Added a new file to find ups and updated dependent scripts
#
# Revision 1.2  2001/11/30 20:46:47  wellner
# added an example of the configuration file needed for kerberos mapping, updated scripts to look in the right places for other scripts.
#
# Revision 1.1  2001/09/20 02:43:02  ivm
# Initial CVS deposit
#
#

mydir=`dirname $0`
. $mydir/findsetup.sh

#setup python
. ~enstore/dcache-deploy/scripts/setup-enstore

python $mydir/mkdir.py $@
exit $?
