#!/bin/sh
set -x
mkdir -p config
aclocal -I config
#aclocal-1.10 -I config
autoheader
libtoolize --automake
automake  --add-missing --copy --foreign
#automake-1.10  --add-missing --copy
autoconf

