#!/bin/sh
BASENAME=basename
DIRNAME=dirname
hsmbase=/tmp/stores
#
getgroup() {
   dir=$1
   x=`cat "$dir/.(tag)(sGroup)" 2>/dev/null`
   if [ $? -ne 0 ] ; then echo "" ; return 1 ; fi
   echo "$x"
   return
}
getstore() {
   dir=$1
   x=`cat "$dir/.(tag)(OSMTemplate)" 2>/dev/null`
   if [ $? -ne 0 ] ; then echo "" ; return 1 ; fi
   x=`echo $x | awk '{ if($1=="StoreName")printf "%s",$2 }' 2>/dev/null`
   echo "$x"
   return
}
gethost() {
   dir=$1
   x=`cat "$dir/.(tag)(OSMTemplate)" 2>/dev/null | grep Host `
   if [ $? -ne 0 ] ; then echo "" ; return 1 ; fi
   x=`echo $x | awk '{ if($1=="Host")printf "%s",$2 }' 2>/dev/null`
   echo "$x"
   return
}
if [ $# -ne 2 ] ; then
   echo "Usage $0 <from> <to>"
   exit 4
fi
from=$1
to=$2
#
if [ ! -f $from ] ; then
   echo "Problem : $from is not a valid source file"
   exit 4
fi
fromdir=`$DIRNAME $from`
fromfile=`$BASENAME $from`
cat "$fromdir/.(const)(c)" >/dev/null 2>/dev/null
if [ $? -eq 0 ] ; then
   fromtype=pnfs
else
   fromtype=local
fi
if [ -d $to ] ; then
   todir=$to
   tofile=$fromfile
   to=$todir/$fromfile
else
   todir=`$DIRNAME $to`
   tofile=`$BASENAME $to`
fi
if [ -f $to ] ; then
   echo "Destination file exists"
   exit 4
fi
cat "$todir/.(const)(c)" >/dev/null 2>/dev/null
if [ $? -eq 0 ] ; then
   totype=pnfs
else
   totype=local
fi
# echo $fromdir $todir
# echo $fromtype $totype
#
if [ \( $fromtype == "local" \) -a \( $totype == "local" \) ] ; then
   echo "Won't copy form local to local" 
   exit 5
elif [ \( $fromtype == "pnfs" \) -a \( $totype == "pnfs" \) ] ; then
   echo "Won't copy form pnfs to pnfs" 
   exit 6
elif [ \( $fromtype == "pnfs" \) -a \( $totype == "local" \) ] ; then
   direction=get
   x=`cat "$fromdir/.(use)(1)($fromfile)" 2>/dev/null`
   if [ $? -ne 0 ] ; then
      echo "Can't determine bitfileid"
      exit 4
   fi
   store=`echo $x | awk '{ print $1 }'`
   group=`echo $x | awk '{ print $2 }'`
   bfid=`echo $x | awk  '{ print $3 }'`
   source=$hsmbase/$store/$group/$bfid
   destination=$to
   cp $source $destination >/dev/null 2>/dev/null
   if [ $? -ne 0 ] ; then
      echo "Copy failed"
      rm $to >/dev/null 2>/dev/null
      exit 5
   else
      exit 0
   fi
elif [ \( $fromtype == "local" \) -a \( $totype == "pnfs" \) ] ; then
   direction=put
   group=`getgroup $todir 2>/dev/null`
   store=`getstore $todir 2>/dev/null`
#   host=`gethost $todir 2>/dev/null`
   if [ \( -z "$store" \) -o \( -z "$group" \)  ] ; then
      echo "Can't determine store or group or host"
      exit 4
   fi
   touch $to >/dev/null 2>/dev/null
   if [ $? -ne 0 ] ; then
      echo "No write permission for : $to"
      exit 5
   fi
   bfid=`cat "$todir/.(id)($tofile)" 2>/dev/null`
#   echo "destination : $host:hsm/$store/$group/$bfid"
   echo "$store $group $bfid" 2>/dev/null >"$todir/.(use)(1)($tofile)"
   destination=$hsmbase/$store/$group/$bfid
   source=$from
   cp $source $destination >/dev/null 2>/dev/null
   if [ $? -ne 0 ] ; then
      echo "Copy failed"
      rm $to >/dev/null 2>/dev/null
      exit 5
   else
      size=`ls -l $source | awk '{ print $5 }' 2>/dev/null`
      touch "$todir/.(fset)($tofile)(size)($size)" 2>/dev/null
      exit 0
   fi
fi
