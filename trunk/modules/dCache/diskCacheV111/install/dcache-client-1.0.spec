Summary: d-cache Client
Name: d-cache-client
Version: 1.0 
Release: 24
Obsoletes: dcache
Source0: %{name}.tar
BuildRoot: /usr/src/redhat/BUILD/%{name}-%{version}
License: Open Source
Group: Applications/System
#Requires: j2re >= 1.4.2
#Prereq: j2re
%description
RPM to install dCache client, created by Michael Ernst/DESY 11/02/04
%prep
%setup -c
mkdir -p opt/d-cache
mv d-cache/srm opt/d-cache
mv d-cache/dcap opt/d-cache
%build

%install


%clean

%post

%files
%defattr(-,root,root)
/opt/d-cache/
/opt/d-cache/srm/
/opt/d-cache/srm/bin/
/opt/d-cache/srm/bin/srmcp
/opt/d-cache/srm/bin/url-copy.sh
/opt/d-cache/srm/bin/srm-advisory-delete
/opt/d-cache/srm/bin/srm
/opt/d-cache/srm/bin/srm-storage-element-info
/opt/d-cache/srm/bin/srm-get-metadata
/opt/d-cache/srm/conf/
/opt/d-cache/srm/conf/SRMServerV1.map
/opt/d-cache/srm/lib/
/opt/d-cache/srm/lib/srm_client.jar
/opt/d-cache/srm/lib/srm.jar
/opt/d-cache/srm/lib/glue/
/opt/d-cache/srm/lib/glue/collections.jar
/opt/d-cache/srm/lib/glue/dom.jar
/opt/d-cache/srm/lib/glue/GLUE-STD.jar
/opt/d-cache/srm/lib/glue/jcert.jar
/opt/d-cache/srm/lib/glue/jnet.jar
/opt/d-cache/srm/lib/glue/jsse.jar
/opt/d-cache/srm/lib/glue/servlet.jar
/opt/d-cache/srm/lib/globus/
/opt/d-cache/srm/lib/globus/bouncycastle.LICENSE
/opt/d-cache/srm/lib/globus/cog-axis.jar
/opt/d-cache/srm/lib/globus/cog-jglobus.jar
/opt/d-cache/srm/lib/globus/cog-lib.jar
/opt/d-cache/srm/lib/globus/cog-tomcat.jar
/opt/d-cache/srm/lib/globus/cryptix32.jar
/opt/d-cache/srm/lib/globus/cryptix-asn1.jar
/opt/d-cache/srm/lib/globus/cryptix.jar
/opt/d-cache/srm/lib/globus/cryptix.LICENSE
/opt/d-cache/srm/lib/globus/jce-jdk13-120.jar
/opt/d-cache/srm/lib/globus/jgss.jar
/opt/d-cache/srm/lib/globus/junit.jar
/opt/d-cache/srm/lib/globus/junit.LICENSE
/opt/d-cache/srm/lib/globus/log4j-1.2.8.jar
/opt/d-cache/srm/lib/globus/log4j.LICENSE
/opt/d-cache/srm/lib/globus/puretls.jar
/opt/d-cache/srm/lib/globus/puretls.LICENSE
/opt/d-cache/srm/lib/globus/version.txt
/opt/d-cache/srm/lib/dcache/
/opt/d-cache/srm/lib/dcache/dcache-srm.jar
/opt/d-cache/srm/lib/xml/
/opt/d-cache/srm/lib/xml/xercesImpl.jar
/opt/d-cache/srm/lib/xml/xmlParserAPIs.jar
/opt/d-cache/srm/README
/opt/d-cache/srm/README.SECURITY
/opt/d-cache/dcap/
/opt/d-cache/dcap/bin/
/opt/d-cache/dcap/bin/dccp
/opt/d-cache/dcap/lib/
/opt/d-cache/dcap/lib/libdcap.so
/opt/d-cache/dcap/lib/libdcap1.2.33.so
/opt/d-cache/dcap/lib/libpdcap.so
/opt/d-cache/dcap/lib/libpdcap1.2.33.so
/opt/d-cache/dcap/lib/libgsiTunnel.so
/opt/d-cache/dcap/include/
/opt/d-cache/dcap/include/dcap.h
/opt/d-cache/dcap/include/dcap_errno.h
/opt/d-cache/dcap/include/dc_hack.h
/opt/d-cache/dcap/sources/
/opt/d-cache/dcap/sources/dccp.c
%changelog
* Wed Mar  17 2004 root <root@cmssrv06.fnal.gov>
- Initial build.
