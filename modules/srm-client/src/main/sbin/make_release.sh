#!/bin/sh

version=$1
if [ "$version" = "" ]
then 
	echo "please specify version as an argument" >&2
	echo "for example first version was  v1_0" >&2
	exit 1
fi
	

if [ "$SRM_PATH" = "" ]
then
   command=`which $0`
   commanddir=`dirname $command`
   SRM_PATH=`dirname $commanddir`
   if [ -x $SRM_PATH/sbin/srm ]
   then
      export SRM_PATH
   else
      echo "cannot determine path to srm product, please define and export SRM_PATH enviroment variable" 1>&1
      exit 1
   fi
fi

cd $SRM_PATH
make clean
make

if [ -d $SRM_PATH/ups_release ]
then 
	rm -r $SRM_PATH/ups_release
fi
mkdir $SRM_PATH/ups_release
mkdir $SRM_PATH/ups_release/bin
cp $SRM_PATH/bin/logback-all.xml $SRM_PATH/bin/srmcp  $SRM_PATH/ups_release/bin
cp $SRM_PATH/bin/srm-advisory-delete  $SRM_PATH/bin/srm-storage-element-info $SRM_PATH/ups_release/bin
cp $SRM_PATH/bin/srm-get-metadata  $SRM_PATH/bin/srm-get-request-status $SRM_PATH/ups_release/bin
cp $SRM_PATH/bin/gridftpcopy $SRM_PATH/bin/adler32  $SRM_PATH/bin/gridftplist $SRM_PATH/ups_release/bin
cp $SRM_PATH/bin/srmls $SRM_PATH/bin/srmmkdir $SRM_PATH/bin/srmrmdir  $SRM_PATH/ups_release/bin
cp $SRM_PATH/bin/srmmv $SRM_PATH/bin/srmrm $SRM_PATH/bin/srmstage  $SRM_PATH/ups_release/bin
cp $SRM_PATH/bin/srm-release-space $SRM_PATH/bin/srm-reserve-space $SRM_PATH/ups_release/bin
cp $SRM_PATH/bin/srm-bring-online $SRM_PATH/bin/srmping $SRM_PATH/ups_release/bin
cp $SRM_PATH/bin/srm-get-space-metadata $SRM_PATH/bin/srm-get-space-tokens $SRM_PATH/ups_release/bin
cp $SRM_PATH/bin/srm-get-permissions $SRM_PATH/ups_release/bin
cp $SRM_PATH/bin/srm-set-permissions $SRM_PATH/ups_release/bin
cp $SRM_PATH/bin/srm-check-permissions $SRM_PATH/ups_release/bin
chmod a+rx $SRM_PATH/ups_release/bin/*
mkdir $SRM_PATH/ups_release/sbin
cp $SRM_PATH/sbin/srm $SRM_PATH/sbin/url-copy.sh $SRM_PATH/sbin/timeout.sh $SRM_PATH/ups_release/sbin
chmod a+rx $SRM_PATH/ups_release/sbin/*
mkdir $SRM_PATH/ups_release/lib
cp  $SRM_PATH/lib/*.jar $SRM_PATH/ups_release/lib
mkdir $SRM_PATH/ups_release/lib/glue
cp  $SRM_PATH/lib/glue/* $SRM_PATH/ups_release/lib/glue
mkdir $SRM_PATH/ups_release/lib/globus
cp  $SRM_PATH/lib/globus/* $SRM_PATH/ups_release/lib/globus
mkdir $SRM_PATH/ups_release/lib/axis
cp  $SRM_PATH/lib/axis/* $SRM_PATH/ups_release/lib/axis
mkdir $SRM_PATH/ups_release/lib/xml
cp  $SRM_PATH/lib/xml/*.jar $SRM_PATH/ups_release/lib/xml
mkdir $SRM_PATH/ups_release/conf
cp $SRM_PATH/conf/SRMServerV1.map $SRM_PATH/conf/logback-all.xml $SRM_PATH/ups_release/conf
cp $SRM_PATH/README $SRM_PATH/ups_release
cp $SRM_PATH/README.SECURITY $SRM_PATH/ups_release
cp $SRM_PATH/FNAL.LICENSE $SRM_PATH/ups_release
mkdir $SRM_PATH/ups_release/ups
chmod a+rx $SRM_PATH/ups/check_java
cp $SRM_PATH/ups/check_java   $SRM_PATH/ups/install_line   $SRM_PATH/ups/srmcp.table $SRM_PATH/ups_release/ups
mkdir $SRM_PATH/ups_release/man 
mkdir $SRM_PATH/ups_release/man/man1
mkdir $SRM_PATH/ups_release/doc
cp $SRM_PATH/man/man1/* $SRM_PATH/ups_release/man/man1
cp $SRM_PATH/doc/* $SRM_PATH/ups_release/doc
mkdir $SRM_PATH/ups_release/etc
cp $SRM_PATH/etc/setup* $SRM_PATH/ups_release/etc
cp $SRM_PATH/etc/config.xml $SRM_PATH/ups_release/etc
find $SRM_PATH/ups_release -name CVS -exec rm -rf \{\} \;
sed s/v1_0/$version/ $SRM_PATH/ups/install_line > $SRM_PATH/ups_release/ups/install_line
mv $SRM_PATH/ups_release $SRM_PATH/srmclient
cd $SRM_PATH
tar -cvf $SRM_PATH/srmcp_${version}_NULL.tar srmclient
mv $SRM_PATH/srmclient $SRM_PATH/ups_release
mv $SRM_PATH/srmcp_${version}_NULL.tar $SRM_PATH/ups_release

