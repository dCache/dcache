#!/bin/sh
#
#  Wait for dCache to have certain cells present.  The function will time-out if
#  dCache takes too long.
#

ourHomeDir=${ourHomeDir:-/opt/d-cache}


# Default polling frequency, in seconds.
poll=5

# Our return code; default is 0 (=> OK)
rc=0

xslt_dir=$ourHomeDir/share/xml/xslt

usage()
{
    echo "Usage:"
    echo "   `basename $0` [-p <poll freq>] [-d | --dots] [-l | --list-missing]"
    echo "                     [-H <host>] [-P <port>] <timeout> <cell> [<cell> ...]"
    echo
    echo "Checks every <poll freq> seconds (${poll}s by default) whether the listed cells are up."
    echo "Obtains dCache current status by querying the web interface.  This is assumed to be"
    echo "running on the local machine, but the -H option can be set for remote queries.  If"
    echo "the dCache web interface is using a non-default port, the -P option is needed."
    echo
    echo "All <cell>s are specified as <cell-name>@<domain-name> or <well-known-cell-name>"
    echo
    echo "The -d and -l options alter what output the script provides on stdout.  If neither is"
    echo "specified then no output is emitted."
    echo
    echo "Returns:"
    echo "    0 if all cells are up,"
    echo "    1 if at least one cell was not after waiting for <timeout> seconds,"
    echo "    5 if parameters to this script are wrong."
}

# Minimum timeout, in seconds
min_timeout=$poll

#  Default values of host and port
host=localhost
port=2288

while [ $# -gt 1 ]; do
    case $1 in
	-p)
	    shift
	    poll=$1
	    shift
	    ;;

        -P)
	    shift
	    port=$1
	    shift
	    ;;

        -h | --help)
	    usage
	    exit 5
	    ;;

	-d | --dots)
	    dots=1
	    shift
	    ;;

	-l | --list-missing)
	    list_missing=1
	    shift
	    ;;

	-H)
	    shift
	    host=$1
	    shift
	    ;;

	-*)
	    echo "Unknown option $1"
	    echo
	    usage
	    exit 5
	    ;;

	*)
	    # Assume the remaining arguments are the required args.
	    break;
	    ;;
    esac
done

if [ $# -lt 2 ]
then
    usage
    exit 5
fi

timeout=$1
shift

cells="$@"

if [ "$timeout" -lt $min_timeout ]; then
  echo Timeout $timeout is less that minimum $min_timeout
  exit 5
fi

#
#  See which cells are currently missing
#
list_missing_cells()
{
    xsltproc --stringparam cells "$cells" $xslt_dir/wait-for-cells.xsl "http://$host:$port/info/domains"
}






#
#  Loop, waiting for either timeout or cells to come up.
#
timeout=$(date -d "$timeout seconds" +%s)

while :; do
  now=$(date +%s)

  if [ $now -gt $timeout ]; then
      # One or more cells were not up by timeout, exit with rc=1
      rc=1
      break;
  fi

  list_missing_cells | grep "Missing:" >/dev/null

  if [ $? -eq 1 ]; then
      # All cells are present, exit with rc=0
      break; 
  fi

  if [ "x$dots" = "x1" ]; then
      echo -n "."
  fi

  sleep $poll
done


#
# Tidy up and exit.
#

if [ "x$dots" = "x1" ]; then
    echo
fi

if [ $rc -ne 0 -a "$list_missing" = 1 ]; then
  list_missing_cells
fi

exit $rc
