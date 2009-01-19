Summary: d-cache Server
Name: d-cache-core
Version: 1.5.2 
Release: 27
Conflicts: dcache-pool
Obsoletes: dcache
Source0: %{name}.tar
BuildRoot: /usr/src/redhat/BUILD/%{name}-%{version}
License: Open Source
Group: Applications/System
#Requires: j2re >= 1.4.2
#Prereq: j2re
AutoReqProv: no
%description
RPM to install dCache core, created by Michael Ernst/DESY 11/02/04

%pre
if [ -f /opt/d-cache/config/lastPid.`/bin/hostname|awk -F"." '{print  $1}'` ]; then
   if [ -f /opt/d-cache/bin/dcache-pool ]; then
      if [ -f /opt/d-cache/jobs/needFulThings.sh ]; then
         /opt/d-cache/bin/dcache-pool stop
      fi
   fi
fi
if [ -f /opt/d-cache/config/lastPid.srm ]; then
   if [ -f /opt/d-cache/bin/dcache-opt ]; then
      if [ -f /opt/d-cache/jobs/needFulThings.sh ]; then
         /opt/d-cache/bin/dcache-opt stop
      fi
   fi
fi
if [ -f /opt/d-cache/config/lastPid.dCache ]; then
   if [ -f /opt/d-cache/bin/dcache-core ]; then
      if [ -f /opt/d-cache/jobs/needFulThings.sh ]; then
      /opt/d-cache/bin/dcache-core stop
      fi
   fi
fi

%prep

%setup -c
mkdir -p opt/d-cache
mv d-cache/bin opt/d-cache
mv d-cache/classes opt/d-cache
mv d-cache/config opt/d-cache
mv d-cache/docs opt/d-cache
mv d-cache/etc opt/d-cache
mv d-cache/install opt/d-cache
mv d-cache/jobs opt/d-cache
mv d-cache/log opt/d-cache
mv d-cache/billing opt/d-cache

%build

%install


%clean

%post

%files
%defattr(-,root,root)
/opt/d-cache/
/opt/d-cache/bin/
/opt/d-cache/bin/dcache-core
/opt/d-cache/bin/dcache-pool
/opt/d-cache/classes/cells.jar
/opt/d-cache/classes/dcache.jar
/opt/d-cache/classes/javatunnel.jar
/opt/d-cache/config/
/opt/d-cache/config/server_config.awk
/opt/d-cache/config/pool_config.awk
/opt/d-cache/config/pool_size.awk
/opt/d-cache/config/extract-dcache
/opt/d-cache/config/JHRM.map
/opt/d-cache/config/PoolManager.conf.temp
/opt/d-cache/config/adminDoor.batch
/opt/d-cache/config/dCache.batch
/opt/d-cache/config/dCacheSetup.temp
/opt/d-cache/config/defaultPools.poollist.temp
/opt/d-cache/config/domain.list
/opt/d-cache/config/dir.batch
/opt/d-cache/config/door.batch
/opt/d-cache/config/httpd.batch
/opt/d-cache/config/lm.batch
/opt/d-cache/config/lm.config.temp
/opt/d-cache/config/pnfs.batch
/opt/d-cache/config/pool.batch
/opt/d-cache/config/securityDoorSetup
/opt/d-cache/config/setup.temp
/opt/d-cache/config/PoolManager.conf
/opt/d-cache/config/lm.config
/opt/d-cache/config/adminDoorSetup
/opt/d-cache/config/allinoneV3Setup
/opt/d-cache/config/allinoneV4Setup
/opt/d-cache/config/doorSetup
/opt/d-cache/config/httpdSetup
/opt/d-cache/config/lmSetup
/opt/d-cache/config/pnfsSetup
/opt/d-cache/config/poolSetup
/opt/d-cache/config/users/
/opt/d-cache/config/users/acls/
/opt/d-cache/config/users/meta/
/opt/d-cache/config/users/relations/
/opt/d-cache/config/README
/opt/d-cache/config/realStop.dcache-servDomain
/opt/d-cache/docs/
/opt/d-cache/docs/
/opt/d-cache/docs/developer/
/opt/d-cache/docs/developer/Entries
/opt/d-cache/docs/developer/BenchSunLangen.html
/opt/d-cache/docs/developer/CreatePackage.html
/opt/d-cache/docs/developer/InstallPackage.html
/opt/d-cache/docs/developer/PoolAdmin.html
/opt/d-cache/docs/developer/PoolManager-guide.html
/opt/d-cache/docs/developer/PoolManagerV3.html
/opt/d-cache/docs/developer/index.html
/opt/d-cache/docs/developer/osmIoErrors.txt
/opt/d-cache/docs/developer/rel-dcache-1.1.0.html
/opt/d-cache/docs/developer/rel-dcache-1.2.0.html
/opt/d-cache/docs/developer/rel-dcache-1.4.0.html
/opt/d-cache/docs/developer/theDiction.html
/opt/d-cache/docs/developer/PoolCost.html
/opt/d-cache/docs/developer/rel-dcache-1-4-7-beta-3.html
/opt/d-cache/docs/developer/req-dcache-1-4-8.html
/opt/d-cache/docs/developer/rel-dcache-1-4-8.html
/opt/d-cache/docs/developer/req-dcache-1.5.2.html
/opt/d-cache/docs/developer/Pool2PoolSteering.html
/opt/d-cache/docs/developer/developer.gif
/opt/d-cache/docs/images/
/opt/d-cache/docs/images/area.gif
/opt/d-cache/docs/images/bg.jpg
/opt/d-cache/docs/images/birdline.gif
/opt/d-cache/docs/images/bluebox.gif
/opt/d-cache/docs/images/cian.gif
/opt/d-cache/docs/images/dark-grey.gif
/opt/d-cache/docs/images/dark-tabaco.gif
/opt/d-cache/docs/images/desy_logo.gif
/opt/d-cache/docs/images/desy_logo_trans.gif
/opt/d-cache/docs/images/dev_trans.gif
/opt/d-cache/docs/images/developer.gif
/opt/d-cache/docs/images/documentation.gif
/opt/d-cache/docs/images/download.gif
/opt/d-cache/docs/images/eagle-grey.gif
/opt/d-cache/docs/images/eagle-main.gif
/opt/d-cache/docs/images/eagle_logo.gif
/opt/d-cache/docs/images/eagle_logo_draft.gif
/opt/d-cache/docs/images/eaglebw.gif
/opt/d-cache/docs/images/eaglered.gif
/opt/d-cache/docs/images/eagleredtrans.gif
/opt/d-cache/docs/images/eurogate.gif
/opt/d-cache/docs/images/eurogatetrans.gif
/opt/d-cache/docs/images/eurogatetranssmall.gif
/opt/d-cache/docs/images/fermi_logo.gif
/opt/d-cache/docs/images/fermi_logo_trans.gif
/opt/d-cache/docs/images/greenbox.gif
/opt/d-cache/docs/images/large001.gif
/opt/d-cache/docs/images/line.gif
/opt/d-cache/docs/images/new-green.gif
/opt/d-cache/docs/images/new01.gif
/opt/d-cache/docs/images/newsletter.gif
/opt/d-cache/docs/images/p2001.gif
/opt/d-cache/docs/images/pMarch.gif
/opt/d-cache/docs/images/pMay.gif
/opt/d-cache/docs/images/pnfslogo1.gif
/opt/d-cache/docs/images/pnfslogotrans.gif
/opt/d-cache/docs/images/rateDist.gif
/opt/d-cache/docs/images/redbox.gif
/opt/d-cache/docs/images/s_top_tux.gif
/opt/d-cache/docs/images/sgi.gif
/opt/d-cache/docs/images/sorry.gif
/opt/d-cache/docs/images/sunlogo.gif
/opt/d-cache/docs/images/tabaco.gif
/opt/d-cache/docs/images/trudex.gif
/opt/d-cache/docs/images/trudey.gif
/opt/d-cache/docs/images/tux.gif
/opt/d-cache/docs/images/violet.gif
/opt/d-cache/docs/images/yellowbox.gif
/opt/d-cache/docs/manuals/
/opt/d-cache/docs/manuals/PoolStatistics.html
/opt/d-cache/docs/manuals/dCacheInst.1.4.x.html
/opt/d-cache/docs/manuals/dcap_setup.html
/opt/d-cache/docs/manuals/dccp.html
/opt/d-cache/docs/manuals/index.html
/opt/d-cache/docs/manuals/libdcap.html
/opt/d-cache/docs/manuals/pnfs.1.4.x.html
/opt/d-cache/docs/manuals/FAQ.html
/opt/d-cache/docs/manuals/PoolUsersGuide.html
/opt/d-cache/docs/manuals/tunnel-HOWTO.html
/opt/d-cache/docs/manuals/m-PAMAuthentificator.html
/opt/d-cache/docs/manuals/m-style.css
/opt/d-cache/docs/manuals/m-DCapDoorInterpreterV3.html
/opt/d-cache/docs/manuals/LiveMonitor.html
/opt/d-cache/docs/manuals/dCache-Hsm-Interface.html
/opt/d-cache/docs/manuals/dCache-pnfs-extentions.html
/opt/d-cache/docs/manuals/PoolChecksum.html
/opt/d-cache/docs/manuals/PoolSpaceReservation.html
/opt/d-cache/docs/manuals/dCacheInstallUpgrade.html
/opt/d-cache/docs/manuals/PoolManager.html
/opt/d-cache/install/
/opt/d-cache/install/install.sh
/opt/d-cache/jobs/
/opt/d-cache/jobs/adminDoor
/opt/d-cache/jobs/dcache-pool
/opt/d-cache/jobs/dcache.lib.sh
/opt/d-cache/jobs/dcache.sh
/opt/d-cache/jobs/dcacheSetup.temp
/opt/d-cache/jobs/diskcacheSetup.temp
/opt/d-cache/jobs/encp.sh
/opt/d-cache/jobs/generic.lib.sh
/opt/d-cache/jobs/hsmcp.sh
/opt/d-cache/jobs/hsmcpV4.sh
/opt/d-cache/jobs/initPackage.sh
/opt/d-cache/jobs/init_dcache.sh
/opt/d-cache/jobs/makeApi
/opt/d-cache/jobs/needFulThings.sh
/opt/d-cache/jobs/patch-addons
/opt/d-cache/jobs/preparePackage.sh
/opt/d-cache/jobs/remote-osmcp.sh
/opt/d-cache/jobs/setup.example
/opt/d-cache/jobs/wrapper.sh
/opt/d-cache/jobs/wrapper2.sh
/opt/d-cache/jobs/adminDoor.lib.sh
/opt/d-cache/jobs/dCache
/opt/d-cache/jobs/dCache.lib.sh
/opt/d-cache/jobs/door
/opt/d-cache/jobs/door.lib.sh
/opt/d-cache/jobs/httpd
/opt/d-cache/jobs/httpd.lib.sh
/opt/d-cache/jobs/lm
/opt/d-cache/jobs/lm.lib.sh
/opt/d-cache/jobs/pnfs
/opt/d-cache/jobs/pnfs.lib.sh
/opt/d-cache/jobs/pool
/opt/d-cache/jobs/pool.lib.sh
/opt/d-cache/jobs/securityDoor
/opt/d-cache/jobs/securityDoor.lib.sh
/opt/d-cache/jobs/server_key
/opt/d-cache/jobs/server_key.pub
/opt/d-cache/etc/
/opt/d-cache/etc/node_config.template
/opt/d-cache/etc/dCacheSetup.template
/opt/d-cache/etc/dcache.kpwd.template
/opt/d-cache/etc/pool_path.template
/opt/d-cache/log/
/opt/d-cache/billing/
%changelog
* Wed Nov  02 2004 root <root@zitpcx4298.desy.de>
- Initial build.
