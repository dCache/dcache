#! /bin/bash

source setenv.sh
base_dir=`pwd`

subject=`grid-cert-info -subject`
gmap=`mktemp`
echo "\"$subject\" `whoami`" > $gmap

cat $gmap
export GRIDMAP=$gmap

export GLOBUS_TCP_PORT_RANGE=50000,51000
cd src/org/globus/ftp/test

out_prop="$base_dir/build/classes/org/globus/ftp/test/test.properties"
out_prop="$base_dir/test.properties"
out_prop="$base_dir/src/org/globus/ftp/test/test.properties"

ports="6666 7777 8888 9999"
server="localhost"

gftp_pid=""
for p in $ports
do
    globus-gridftp-server -p $p -aa &
    gftp_pid="$gftp_pid $!"
done

destDir=`mktemp -d`
srcDir=`mktemp -d`
srcFile="jglobusTest"
smallFile="jglobusSmall"
largeFile="jglobusBig"
fsize=1048576

dd if=/dev/urandom of=$srcDir/$srcFile bs=1024 count=1024
dd if=/dev/urandom of=$srcDir/$smallFile bs=10240 count=1
dd if=/dev/urandom of=$srcDir/$largeFile bs=10240 count=10240

sub="-e s^@@PORTA@@^6666^g -e s^@@PORTB@@^7777^g -e s^@@PORTF@@^8888^g -e s^@@PORTG@@^9999^g"
seqs="A B F G"
for s in $seqs
do
    relname="$srcFile""$s"
    fname="$srcDir/$relname"
    echo $fname
    dd if=/dev/urandom of=$fname bs=1024 count=1024
    cksum=`md5sum $fname | awk '{ print $1 }'`
    sub="$sub -e s^@@SRC_FILE$s@@^$relname^g -e s^@@MD5SUM$s@@^$cksum^g"
done

echo "SUB"
echo "$sub"
echo "ENDSUB"

cksum=`md5sum $srcDir/$srcFile | awk '{ print $1 }'`


sub="$sub -e s^@@PORTA$s@@^$relname^g"

sed -e "s^@@DST_DIR@@^$destDir^g" -e "s^@@SRC_DIR@@^$srcDir^g" -e "s^@@SRC_FILE@@^$srcFile^g" -e "s^@@PORT@@^$port^g" -e "s^@@SMALL_FILE@@^$smallFile^g" -e "s^@@LARGE_FILE@@^$largeFile^g" -e "s^@@SIZE@@^$fsize^g" -e "s^@@SUBJECT@@^$subject^g" -e "s^@@MD5SUM@@^$cksum^g" -e "s^@@HOST@@^$server^g" $sub test.properties.in > $out_prop

cd $base_dir

ant test

for p in $gftp_pid
do
    kill $p
done
rm -rf $destDir $srcDir
