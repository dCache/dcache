Summary: d-cache Server
Name: d-cache-opt 
Version: 1.5.3 
Release: 5
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
RPM to install dCache server node, created by Michael Ernst/DESY 12/03/04

#%pre
#if [ -f /opt/d-cache/config/lastPid.`/bin/hostname|awk -F"." '{print  $1}'` ]; then
#   if [ -f /opt/d-cache/bin/dcache-pool ]; then
#      if [ -f /opt/d-cache/jobs/needFulThings.sh ]; then
#         /opt/d-cache/bin/dcache-pool stop
#      fi
#   fi
#fi
%pre
if [ -f /opt/d-cache/config/lastPid.srm ]; then
   if [ -f /opt/d-cache/bin/dcache-opt ]; then
      if [ -f /opt/d-cache/jobs/needFulThings.sh ]; then
         /opt/d-cache/bin/dcache-opt stop
      fi
   fi
fi
#
#if [ -f /opt/d-cache/config/lastPid.dCache ]; then
#   if [ -f /opt/d-cache/bin/dcache-core ]; then
#      if [ -f /opt/d-cache/jobs/needFulThings.sh ]; then
#      /opt/d-cache/bin/dcache-core stop
#      fi
#   fi
#fi

%prep

%setup -c
mkdir -p opt/d-cache
mv d-cache/bin opt/d-cache
mv d-cache/classes opt/d-cache
mv d-cache/config opt/d-cache
mv d-cache/jobs opt/d-cache

%build

%install


%clean

%post

%files
%defattr(-,root,root)
/opt/d-cache/
/opt/d-cache/bin/
/opt/d-cache/bin/dcache-opt
/opt/d-cache/classes/dcache-srm.jar
/opt/d-cache/classes/concurrent.jar
/opt/d-cache/classes/pgjdbc2.jar
/opt/d-cache/classes/globus/
/opt/d-cache/classes/globus/cog-axis.jar
/opt/d-cache/classes/globus/cog-jglobus.jar
/opt/d-cache/classes/globus/cog-lib.jar
/opt/d-cache/classes/globus/cog-tomcat.jar
/opt/d-cache/classes/globus/cryptix-asn1.jar
/opt/d-cache/classes/globus/cryptix.jar
/opt/d-cache/classes/globus/cryptix32.jar
/opt/d-cache/classes/globus/jce-jdk13-120.jar
/opt/d-cache/classes/globus/jgss.jar
/opt/d-cache/classes/globus/junit.jar
/opt/d-cache/classes/globus/log4j-1.2.8.jar
/opt/d-cache/classes/globus/puretls.jar
/opt/d-cache/classes/glue/
/opt/d-cache/classes/glue/GLUE-STD.jar
/opt/d-cache/classes/glue/collections.jar
/opt/d-cache/classes/glue/dom.jar
/opt/d-cache/classes/glue/jcert.jar
/opt/d-cache/classes/glue/jnet.jar
/opt/d-cache/classes/glue/jsse.jar
/opt/d-cache/classes/glue/servlet.jar
/opt/d-cache/classes/tomcat/
/opt/d-cache/classes/tomcat/catalina.jar
/opt/d-cache/classes/tomcat/ant.jar
/opt/d-cache/classes/tomcat/bootstrap.jar
/opt/d-cache/classes/tomcat/commons-beanutils.jar
/opt/d-cache/classes/tomcat/commons-collections.jar
/opt/d-cache/classes/tomcat/commons-digester.jar
/opt/d-cache/classes/tomcat/commons-logging.jar
/opt/d-cache/classes/tomcat/jakarta-regexp-1.2.jar
/opt/d-cache/classes/tomcat/jasper-compiler.jar
/opt/d-cache/classes/tomcat/jasper-runtime.jar
/opt/d-cache/classes/tomcat/naming-common.jar
/opt/d-cache/classes/tomcat/naming-factory.jar
/opt/d-cache/classes/tomcat/naming-resources.jar
/opt/d-cache/classes/tomcat/servlet.jar
/opt/d-cache/classes/tomcat/servlets-common.jar
/opt/d-cache/classes/tomcat/servlets-default.jar
/opt/d-cache/classes/tomcat/servlets-invoker.jar
/opt/d-cache/classes/tomcat/tomcat-coyote.jar
/opt/d-cache/classes/tomcat/tomcat-http11.jar
/opt/d-cache/classes/tomcat/tomcat-util.jar
/opt/d-cache/config/
/opt/d-cache/config/gridftpdoor.batch
/opt/d-cache/config/gridftpdoorSetup
/opt/d-cache/config/gsidcapdoor.batch
/opt/d-cache/config/gsidcapdoorSetup
/opt/d-cache/config/srm.batch
/opt/d-cache/config/srmSetup
/opt/d-cache/jobs/
/opt/d-cache/jobs/gridftpdoor
/opt/d-cache/jobs/gridftpdoor.lib.sh
/opt/d-cache/jobs/gsidcapdoor
/opt/d-cache/jobs/gsidcapdoor.lib.sh
/opt/d-cache/jobs/srm
/opt/d-cache/jobs/srm.lib.sh
%changelog
* Wed Apr  28 2004 root <root@zitpcx4298.desy.de>
- Initial build.
