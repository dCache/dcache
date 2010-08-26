Summary: PNFS 
Name: pnfs
Version: 3.1.10 
Release: 12
#Conflicts: pnfs
Obsoletes: pnfs
Source0: %{name}.linux.%{version}.tar.gz
BuildRoot: /usr/src/redhat/BUILD/%{name}-%{version}
License: Open Source
Group: Applications/System
# Requires: j2re >= 1.4.2
# Prereq: j2re
%description
RPM to install PNFS, created by Michael Ernst/DESY 04/27/04

%pre
svc=/opt/pnfs.3.1.10/pnfs/tools/pnfs.server
#    if test -x $svc
pnfsd_stat=0    
    pnfsd_stat=`ps -ef 2>/dev/null | awk '{print $0}' | grep -v grep | grep pnfsd | tail -1 | awk '{print $8}'`
    if [ ! -z $pnfsd_stat ]; then  
          echo "PNFS is still running - Shutting it down for the upgrade"
          RETVAL=0
          cp=0
          cp=`df /pnfs/fs 2>/dev/null | tail -1 | awk '{print $0}' |grep /pnfs | awk '{print $2}'`
          if [ ! -z $cp ]; then
          umount /pnfs/fs
          RETVAL=`echo $?`
          fi
          if [ $RETVAL -ne 0 ]; then
             echo "Unable to umount /pnfs - Exiting without installing RPM"
             exit 1 
          fi
	  $svc stop || true
    fi

%prep
%setup -c
mkdir -p opt/pnfs.3.1.10/pnfs
mv pnfs/bin opt/pnfs.3.1.10/pnfs/bin
mv pnfs/etc opt/pnfs.3.1.10/pnfs/etc
mv pnfs/docs opt/pnfs.3.1.10/pnfs/docs
mv pnfs/install opt/pnfs.3.1.10/pnfs/install
mv pnfs/tools opt/pnfs.3.1.10/pnfs/tools
#mv pnfs/pnfsSetup opt/pnfs.3.1.10/pnfs/pnfsSetup
%build

%install


%clean

%post

%files
/opt/pnfs.3.1.10/pnfs/
/opt/pnfs.3.1.10/pnfs/bin
/opt/pnfs.3.1.10/pnfs/bin/pnfs
/opt/pnfs.3.1.10/pnfs/install/pnfs-install.sh
/opt/pnfs.3.1.10/pnfs/etc
/opt/pnfs.3.1.10/pnfs/etc/pnfs_config.template
/opt/pnfs.3.1.10/pnfs/tools/
/opt/pnfs.3.1.10/pnfs/tools/autoinstall-s.sh
/opt/pnfs.3.1.10/pnfs/tools/backupConfig
/opt/pnfs.3.1.10/pnfs/tools/checkDots
/opt/pnfs.3.1.10/pnfs/tools/checkTags
/opt/pnfs.3.1.10/pnfs/tools/dbserverWatchdog
/opt/pnfs.3.1.10/pnfs/tools/heartbeat
/opt/pnfs.3.1.10/pnfs/tools/heartbeatBackup
/opt/pnfs.3.1.10/pnfs/tools/install-s.sh
/opt/pnfs.3.1.10/pnfs/tools/md
/opt/pnfs.3.1.10/pnfs/tools/mdb
/opt/pnfs.3.1.10/pnfs/tools/mdconfig
/opt/pnfs.3.1.10/pnfs/tools/mdcreate
/opt/pnfs.3.1.10/pnfs/tools/observer.sh
/opt/pnfs.3.1.10/pnfs/tools/packup
/opt/pnfs.3.1.10/pnfs/tools/packup.sof
/opt/pnfs.3.1.10/pnfs/tools/pathfinder
/opt/pnfs.3.1.10/pnfs/tools/pcpattr
/opt/pnfs.3.1.10/pnfs/tools/pexports
/opt/pnfs.3.1.10/pnfs/tools/pflags
/opt/pnfs.3.1.10/pnfs/tools/platform
/opt/pnfs.3.1.10/pnfs/tools/playout
/opt/pnfs.3.1.10/pnfs/tools/plog
/opt/pnfs.3.1.10/pnfs/tools/pls
/opt/pnfs.3.1.10/pnfs/tools/pmount
/opt/pnfs.3.1.10/pnfs/tools/pnewpool
/opt/pnfs.3.1.10/pnfs/tools/pnfs
/opt/pnfs.3.1.10/pnfs/tools/pnfs.server
/opt/pnfs.3.1.10/pnfs/tools/pnfsFastBackup
/opt/pnfs.3.1.10/pnfs/tools/pnfsHeartbeat
/opt/pnfs.3.1.10/pnfs/tools/pnfsHour
/opt/pnfs.3.1.10/pnfs/tools/pnfsMinute
/opt/pnfs.3.1.10/pnfs/tools/pnfsObserver
/opt/pnfs.3.1.10/pnfs/tools/pnfsperf.sh
/opt/pnfs.3.1.10/pnfs/tools/pnfstools
/opt/pnfs.3.1.10/pnfs/tools/ps.linux.sed
/opt/pnfs.3.1.10/pnfs/tools/ps.sed
/opt/pnfs.3.1.10/pnfs/tools/pshowmounts
/opt/pnfs.3.1.10/pnfs/tools/ptools
/opt/pnfs.3.1.10/pnfs/tools/scandir.sh
/opt/pnfs.3.1.10/pnfs/tools/setversion
/opt/pnfs.3.1.10/pnfs/tools/showlog
/opt/pnfs.3.1.10/pnfs/tools/smd
/opt/pnfs.3.1.10/pnfs/tools/special
/opt/pnfs.3.1.10/pnfs/tools/linux/
/opt/pnfs.3.1.10/pnfs/tools/linux/shmcom
/opt/pnfs.3.1.10/pnfs/tools/linux/md2tool
/opt/pnfs.3.1.10/pnfs/tools/linux/md3tool
/opt/pnfs.3.1.10/pnfs/tools/linux/dbserver
/opt/pnfs.3.1.10/pnfs/tools/linux/sclient
/opt/pnfs.3.1.10/pnfs/tools/linux/pmountd
/opt/pnfs.3.1.10/pnfs/tools/linux/pnfsd
/opt/pnfs.3.1.10/pnfs/tools/md2tool
/opt/pnfs.3.1.10/pnfs/tools/md3tool
/opt/pnfs.3.1.10/pnfs/tools/pmountd
/opt/pnfs.3.1.10/pnfs/tools/pnfsd
/opt/pnfs.3.1.10/pnfs/tools/shmcom
/opt/pnfs.3.1.10/pnfs/tools/dbserver
/opt/pnfs.3.1.10/pnfs/tools/sclient
/opt/pnfs.3.1.10/pnfs/tools/autoinstall-s.sh
/opt/pnfs.3.1.10/pnfs/docs/
/opt/pnfs.3.1.10/pnfs/docs/html/
/opt/pnfs.3.1.10/pnfs/docs/html/LICENSE.html
/opt/pnfs.3.1.10/pnfs/docs/html/admin.html
/opt/pnfs.3.1.10/pnfs/docs/html/backup.html
/opt/pnfs.3.1.10/pnfs/docs/html/basics.html
/opt/pnfs.3.1.10/pnfs/docs/html/checkBackup.html
/opt/pnfs.3.1.10/pnfs/docs/html/download.html
/opt/pnfs.3.1.10/pnfs/docs/html/export.html
/opt/pnfs.3.1.10/pnfs/docs/html/gettingStarted.html
/opt/pnfs.3.1.10/pnfs/docs/html/index.html
/opt/pnfs.3.1.10/pnfs/docs/html/info.html
/opt/pnfs.3.1.10/pnfs/docs/html/logformat.html
/opt/pnfs.3.1.10/pnfs/docs/html/moresecurity.html
/opt/pnfs.3.1.10/pnfs/docs/html/movedb.html
/opt/pnfs.3.1.10/pnfs/docs/html/noio.html
/opt/pnfs.3.1.10/pnfs/docs/html/pnfsDirAttr.html
/opt/pnfs.3.1.10/pnfs/docs/html/pnfslogo3.gif
/opt/pnfs.3.1.10/pnfs/docs/html/remove.html
/opt/pnfs.3.1.10/pnfs/docs/html/shrink.html
/opt/pnfs.3.1.10/pnfs/docs/html/tags.html
/opt/pnfs.3.1.10/pnfs/docs/html/v3.1.10.html
/opt/pnfs.3.1.10/pnfs/docs/html/v3.1.3.a.html
/opt/pnfs.3.1.10/pnfs/docs/html/v3.1.3.html
/opt/pnfs.3.1.10/pnfs/docs/html/v3.1.4.html
/opt/pnfs.3.1.10/pnfs/docs/html/v3.1.5.html
/opt/pnfs.3.1.10/pnfs/docs/html/v3.1.8.html
/opt/pnfs.3.1.10/pnfs/docs/html/v3.1.9.html
/opt/pnfs.3.1.10/pnfs/docs/html/worms.html
/opt/pnfs.3.1.10/pnfs/docs/notes
/opt/pnfs.3.1.10/pnfs/docs/notes.problems
#/opt/pnfs.3.1.10/pnfs/pnfsSetup
%changelog
* Wed Mar 17 2004 root <root@cmssrv06.fnal.gov>
- Initial build.
