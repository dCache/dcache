#! /usr/bin/env perl

use strict;
use File::Temp qw/ tempfile tempdir /;

my $localDir = tempdir( CLEANUP => 0);
my $remoteDir;
my $cmd;
my @hosts;
my $remoteHost = shift;
my $port = shift;
my $dn = shift;
my $smallFile="sm.data";
my $largeFile="lg.data";
my $file="lg.data";
my $fCount=1024;
my $fBS=10240;
my $fSize=$fCount*$fBS;

push(@hosts, "serverA");
push(@hosts, "serverB");
push(@hosts, "serverF");
push(@hosts, "serverG");

$cmd = "ssh $remoteHost mktemp -d";
my $serverADir=`$cmd`;
chomp($serverADir);

my $lSrcDir = "$localDir/src";
my $lDstDir = "$localDir/dst";
system("mkdir -p $lSrcDir");
system("mkdir -p $lDstDir");
system("dd if=/dev/urandom of=$lSrcDir/srcF count=64 bs=1024");
system("dd if=/dev/urandom of=$lDstDir/srcF count=64 bs=1024");

my @cksums;

foreach(@hosts)
{
    my $ld = $_;

    system("ssh $remoteHost mkdir -p $serverADir/$ld/srcDir");
    system("ssh $remoteHost mkdir -p $serverADir/$ld/dstDir");

    system("ssh $remoteHost dd if=/dev/urandom of=$serverADir/$ld/srcDir/$smallFile count=64 bs=1024");
    system("ssh $remoteHost dd if=/dev/urandom of=$serverADir/$ld/srcDir/$largeFile count=$fCount bs=$fBS");
    system("ssh $remoteHost dd if=/dev/urandom of=$serverADir/$ld/dstDir/$largeFile count=$fCount bs=$fBS");


    $cmd="ssh $remoteHost md5sum $serverADir/$ld/srcDir/$largeFile";
    my $largeCkSum=`$cmd`;
    my @tmpA = split (/ /, $largeCkSum);
    $largeCkSum = $tmpA[0];

    print "$tmpA[1] $largeCkSum\n";

    push(@cksums, $largeCkSum);
}

# create remoteDirs for all hosts

print "-------- test.properties -----------\n";

print "org.globus.ftp.test.local.destDir=$lSrcDir\n";
print "org.globus.ftp.test.local.srcDir=$lDstDir\n";
print "org.globus.ftp.test.local.srcFile=srcF\n";
#default local server port (may be left empty)
print "org.globus.ftp.test.local.serverPort=0\n";


my $i = 0;
foreach(@hosts)
{
    my $ld = $_;

#GridFTP server A (source)
##########################

    print "# $ld props\n";
    print "org.globus.ftp.test.gridftp.$ld.host=$remoteHost\n";
    print "org.globus.ftp.test.gridftp.$ld.port=$port\n";
    print "org.globus.ftp.test.gridftp.$ld.noSuchPort=5680\n";

#  dir/file must exist (this is the source file)
    print "org.globus.ftp.test.gridftp.$ld.dir=$serverADir/$ld/srcDir\n";
    print "org.globus.ftp.test.gridftp.$ld.smallFile=$smallFile\n";
    print "org.globus.ftp.test.gridftp.$ld.largeFile=$largeFile\n";

    print "org.globus.ftp.test.gridftp.$ld.file=$largeFile\n";
    print "org.globus.ftp.test.gridftp.$ld.file.size=$fSize\n";
    print "org.globus.ftp.test.gridftp.$ld.subject=$dn\n";

    my $cksm=$cksums[$i];
    print "org.globus.ftp.test.gridftp.$ld.file.checksum=$cksm\n";

    print "org.globus.ftp.test.gridftp.$ld.nosuchfile=nosuchfile\n";
    print "\n";
    print "org.globus.ftp.test.$ld.host=$remoteHost\n";
    print "org.globus.ftp.test.$ld.port=$port\n";

    print "org.globus.ftp.test.$ld.dir=$serverADir/$ld/srcDir\n";
    print "org.globus.ftp.test.$ld.file=$largeFile\n";
    print "org.globus.ftp.test.$ld.user=anonymous\n";
    print "org.globus.ftp.test.$ld.password=poop\n";
    print "org.globus.ftp.test.$ld.subject=$dn\n";
    print "org.globus.ftp.test.$ld.file.size=$fSize\n";

    print "org.globus.ftp.test.$ld.nosuchdir=nosuchdir\n";

    $i++;
    print "\n";
}


print "\n";
print "org.globus.ftp.test.gridftp.parallelism=6\n";
print "\n";
print "org.globus.ftp.test.noSuchServer.host=no.such.server\n";
print "\n";




