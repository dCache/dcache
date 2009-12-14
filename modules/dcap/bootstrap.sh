#!/bin/sh
set -x
mkdir -p config
touch AUTHORS ChangeLog NEWS README COPYING
aclocal -I config
#aclocal-1.10 -I config
autoheader
libtoolize --automake
automake  --add-missing --copy
#automake-1.10  --add-missing --copy
autoconf

