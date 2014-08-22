#!/bin/sh

SRCDIR=`pwd`
BUILDDIR=$1
DISTDIR=$2
VERSION=$3
NAME=$4

echo 'i pkginfo' > prototype
find . | grep -v '^./prototype$' | grep -v '^./pkginfo$' | pkgproto \
    | awk '{ print $1, $2, $3, $4, "root", "bin" }' >> prototype

mkdir -p "${BUILDDIR}"

#  Work-around pkgmk brokenness.
#
#  When building a package, one of the directories is "-P pkg".  When
#  processing the prototype file, pkgmk will prepend the path to the
#  files.  Unfortunately, it cannot cope with a space being present in
#  the resulting path.  Further, it also cannot cope with cwd
#  containing a space, perhaps as it must find the prototype file.
#
#  As a work-around, we copy everything into a temporary directory
#  that (on Solaris) will not contain a space and change to that
#  directory for building the package.

TEMP_STORAGE=`mktemp -d`
cp -r * "${TEMP_STORAGE}"
cd "${TEMP_STORAGE}"

pkgmk -o -d "${BUILDDIR}" -r .

cd "${SRCDIR}"
rm -rf "${TEMP_STORAGE}"

pkgtrans -s "${BUILDDIR}" "${DISTDIR}/${NAME}-${VERSION}.pkg" dCache

rm -rf "${BUILDDIR}"
