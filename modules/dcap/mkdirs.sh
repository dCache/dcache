#!/bin/sh
#
# $Id: mkdirs.sh,v 1.1 2002-02-19 11:06:57 cvs Exp $
#

while [ $# -gt 0 ]
do
	if [ ! -d $1 ]; then
		echo "Creating directory $1";
		mkdir -p $1;
		chmod 755 $1;
	fi
	shift
done
