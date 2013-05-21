#!/bin/sh

BINDIR=$1
BUILDDIR=$2
DISTDIR=$3
VERSION=$4
NAME=$5

cd ${BINDIR}

echo 'i pkginfo' > prototype
find . | grep -v '^./prototype$' | grep -v '^./pkginfo$' | pkgproto \
    | awk '{ print $1, $2, $3, $4, "root", "bin" }' >> prototype

mkdir -p ${BUILDDIR}
pkgmk -o -d ${BUILDDIR} -r .
pkgtrans -s ${BUILDDIR} ${DISTDIR}/${NAME}-${VERSION}.pkg dCache
