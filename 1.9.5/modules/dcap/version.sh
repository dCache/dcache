#!/bin/sh
#
# $Id: version.sh,v 1.2 2003-10-16 08:59:33 tigran Exp $
#

cat dcap_version.c  |\
	grep return |\
	head -1 |\
	awk '{print $3}' |\
	awk -F"-" '{print  $2 "." $3 "." $4 }'

