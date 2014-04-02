#!/bin/sh

BUILDDIR=$1
DISTDIR=$2
VERSION=$3
NAME=$4

echo 'i pkginfo' > prototype
find . | grep -v '^./prototype$' | grep -v '^./pkginfo$' | pkgproto \
    | awk '{ print $1, $2, $3, $4, "root", "bin" }' >> prototype

mkdir -p ${BUILDDIR}
pkgmk -o -d ${BUILDDIR} -r .
pkgtrans -s ${BUILDDIR} ${DISTDIR}/${NAME}-${VERSION}.pkg dCache
