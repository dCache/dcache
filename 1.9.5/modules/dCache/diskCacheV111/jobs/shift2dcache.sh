#!/bin/sh
#
# $Log: not supported by cvs2svn $
#

# /acs/data/[Y][y]/dst[R]/CDST[R].[Y][y][number]

PREFIX=/acs/data/

if [ "$1" = "" ]
then
   echo "Usage: $0 <DST file name>"
   exit 1;
fi

R=`echo $1 | cut -c5`
Y=`echo $1 | cut -c8`
y=`echo $1 | cut -c9`

SHIFT_LOCATION=`sfget -p h1dst -u h1 -k $1`
if [ "$?" != "0" ]
then
   echo "Unable to find file $1 in SHIFT"
   exit 2
fi

PNFS_FILE="$PREFIX$Y$y/dst$R/$1"
PNFS_ID=`cat "$PREFIX$Y$y/dst$R/.(id)($1)"`

#echo "PNFS_FILE = $PNFS_FILE"
#echo "PNFS_ID = $PNFS_ID"
#echo "SHIFT_LOCATION = $SHIFT_LOCATION"

if [ ! -f "$SHIFT_LOCATION" ]
then
   echo "File is missing in the SHIFT pool"
   exit 3
fi

if [ ! -d "control" ]
then
   echo "Creating directory [control]"
   mkdir control
fi

if [ ! -d "data" ]
then
   echo "Creating directory [data]"
   mkdir data
fi

echo "Creating control file for pnfsID $PNFS_ID"
echo cached > control/$PNFS_ID
echo "Creating link for data/$PNFS_ID to $SHIFT_LOCATION"
ln -s $SHIFT_LOCATION data/$PNFS_ID

exit 0
