#!/bin/sh

#  This version of bootstrap.sh will attempt to locate a consistent
#  set of aclocal and automake.  "Consistent" here means that they
#  have the same suffix; for example, aclocal-1.9 and automake-1.9.
#
#  This identification of a common set of aclocal and automake is a
#  work-around.  It is needed because of a packaging issue with
#  OpenSolaris, which does not always provide "aclocal" and "automake"
#  that work.  Instead, there are aclocal-nn and automake-nn (where
#  "nn" is the corresponding version number) allows external scripts
#  to deduce which versions will work together.  This problem does not
#  exists on the Linux distributions tried so far (Scientific Linux
#  and Debian).
#
#  Unfortunately, it is unclear how to submit bugs against OpenSolais,
#  so there is no indication if and when the problem will be fixed.

function haveToolsWithSuffix() # in $1 - version suffix
{
  found_count=0
  for prog in aclocal automake; do
    which $prog$1 >/dev/null 2>&1 || break
    found_count=$(( $found_count + 1 ))
  done

  [ $found_count -eq 2 ]
}

#  Echo a command then run it.  If it fails terminate script.
function run() # in $1 - program to run, $2.. arguments
{
  echo "+ $*"
  "$@"
  rc=$?
  if [ $rc -ne 0 ]; then
      exit $rc
  fi
}

for suffix in -1.11 -1.10 -1.9 ""; do
  if haveToolsWithSuffix "$suffix"; then
      ACLOCAL=aclocal$suffix
      AUTOMAKE=automake$suffix
      break;
  fi
done

if [ ! "$ACLOCAL" ]; then
    echo "No suitable autotools found."
    exit 1
fi

mkdir -p config

run $ACLOCAL -I config
run autoheader
run libtoolize --automake
run $AUTOMAKE --add-missing --copy --foreign
run autoconf
