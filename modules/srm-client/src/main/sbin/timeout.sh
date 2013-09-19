#!/bin/sh
# script to execute a program with a timeout
#
# usage: timeout.sh [-debug] timeout program [program arguments]
#        timeout.sh [-help|--help] to print this help
#
# author: Timur Perelmutov, parts were taken from dCache's  generic.lib.sh


function execute
{
  pidfile=$1
  shift
  rcfile=$1  
  shift
  program=$*
  decho "running << $program >>" 
  $program &
  prog_pid=$!
  decho sleep pid is $prog_pid
  decho writing $prog_pid to $pidfile
  echo $prog_pid >$pidfile
  wait $prog_pid
  return_code=$?
  decho "<< $program >> terminated"
  decho removing $pidfile
  echo $return_code >$rcfile
  rm $pidfile
  exit $return_code
}

function usage
{
	
	echo "usage: timeout.sh [-debug] timeout program [program arguments]"
	echo "       timeout.sh [-help|--help] to print this help"
}

if [ "$1" = "-help" -o "$1" = "--help" ]
then
	usage
	exit 0
fi

if [ "$1" = "-debug" ]
then 
	debug=true
	shift
fi

function decho
{
	if [ "$debug" = "true" ]
	then
		echo $*
	fi
}

timeout=$1
if [ -z $timeout ]
then
	echo timout is not specified 2>&1
	usage
	exit 1
fi

shift
args=$*
if [ -z $1 ]
then
	echo program is not specified 2>&1
	usage
	exit 1
fi

pidfile=/tmp/globus-url-copy.pid.$$
rcfile=/tmp/globus-url-copy.rc.$$
#./long.sh &
(execute $pidfile $rcfile $args)&
sleep 1

while [ "$timeout" -gt "0" ]
do
  if [ ! -f $pidfile ]
  then
	decho " << $args >> terminated before timeout expired"
	
	if [ -f $rcfile ]
	then
		rc=`cat $rcfile`
		rm $rcfile
		decho "exit with return code $rcfile"
		exit $rc
	fi
	decho can not determine return code
	exit  1
  fi
  decho timeout wait sleep for 5 sec
  sleep 5
  let timeout=$timeout-5
done

if [ -f $pidfile ] 
then
	decho "<< $args >> is still running, terminating"
	x=`cat $pidfile` 
	decho globus-url-copy is $x
	if [ "$x" = "" ]
	then
		decho "cannot get valid pid for << $args >>"
		exit 1
	fi
	kill -TERM $x 1>/dev/null 2>/dev/null
        for c in  0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20
	do 
            sleep 1
            kill -0 $x 1>/dev/null 2>/dev/null
            if [ $? -ne 0 ]  
	    then
                decho "Done" 
                exit 1
            fi
            printf "$c "
            if [ $c -eq 8 ]  
	    then
               kill -9 $x
            fi
        done
        decho "Giving up : << $args >> might still be running" 1>&2
        exit 4
fi


