#!/bin/sh


SRM_RPM_NAME=`ls modules/srm-client/target/rpmbuild/RPMS/noarch/ | grep dcache-srmclient`
SRM_RPM_SUM=`md5sum modules/srm-client/target/rpmbuild/RPMS/noarch/$SRM_RPM_NAME | cut -d ' ' -f 1`
DEB_NAME=`ls packages/fhs/target/ | grep dcache`
DEB_SUM=`md5sum packages/fhs/target/$DEB_NAME | cut -d ' ' -f 1`
TAR_NAME=`ls packages/tar/target/ | grep dcache`
TAR_SUM=`md5sum packages/tar/target/$TAR_NAME | cut -d ' ' -f 1`
RPM_NAME=`ls packages/fhs/target/rpmbuild/RPMS/noarch/ | grep dcache`
RPM_SUM=`md5sum packages/fhs/target/rpmbuild/RPMS/noarch/$RPM_NAME | cut -d ' ' -f 1`
DATE=`date +"%Y.%m.%d"`

echo "| Download   | Build date | md5 sum  |"
echo "|:-----------|:-----------|----------|"
echo "| $RPM_NAME  | $DATE      | $RPM_SUM |"
echo "| $DEB_NAME  | $DATE      | $DEB_SUM |"
echo "| $TAR_NAME  | $DATE      | $TAR_SUM |"
echo "| $SRM_RPM_NAME | $DATE   | $SRM_RPM_SUM |"

echo ; echo; echo
git log `git describe --tags --abbrev=0`...`git describe --tags --abbrev=0 HEAD^` --no-merges --format='[%h](https://github.com/dcache/dcache/commit/%H)%n:    %s%n'