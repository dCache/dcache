#!/bin/sh

BINDIR=$1
BUILDDIR=$2
DISTDIR=$3
VERSION=$4

cd ${BINDIR}

echo 'i pkginfo' > prototype
find . | grep -v '^./prototype$' | grep -v '^./pkginfo$' | pkgproto \
    | awk '{ print $1, $2, $3, $4, "root", "bin" }' >> prototype

pkgmk -o -d ${BUILDDIR} -r .
pkgtrans -s ${BUILDDIR} ${DISTDIR}/dcache-server-${VERSION}.pkg dCache

