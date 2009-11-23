#
# $Id: dcache-client.spec.template,v 1.3 2007-07-05 09:22:06 timur Exp $
#
Summary: dCache Client
Vendor: dCache.org
Name: dcap
Version: 1.9.3
Release: 3rc
Prefix: /opt/d-cache/dcap
BuildRoot: %{_tmppath}/%{name}-%{version}-build
Source: %{name}.src.tgz
BuildRequires: make
BuildRequires: gcc
BuildRequires: openssl
Provides: dcap
#Provdes dcache-dcap for backwards compatibility
Provides: dcache-dcap
# Commenting out VDT as its not a fixed dependency
#BuildRequires: vdt_globus_essentials
Provides: d-cache-dcap
Obsoletes: dcache-dcap
AutoReqProv: no
License: LGPL

Group: Applications/System
%description
dCache is a distributed mass storage system.

This package contains the client components.
%package -n libdcap
Summary: dCache Client libraries
Group: Applications/System
%description -n libdcap
dCache is a distributed mass storage system.

This package contains the client libraries dcachedcap.
%package -n libdcap-devel
Summary: dCache Client Headers
Group: Applications/System
Requires: libdcap
Obsoletes: dcache-dcap
%description -n libdcap-devel
This package contains the client library dcachedcap header files.

%package -n libdcap-tunnel-gsi
Summary: dCache GSI Tunnel
Group: Applications/System
Requires: libdcap
Obsoletes: dcache-dcap
%description -n libdcap-tunnel-gsi
This package contains the gsi tunnel plugin library used by dcachedcap.
This library is dynamically loaded at run time.

%package -n libdcap-tunnel-krb
Summary: dCache GSI Tunnel
Group: Applications/System
Requires: libdcap
Obsoletes: dcache-dcap
%description -n libdcap-tunnel-krb
This package contains the gsi tunnel plugin library used by dcachedcap.
This library is dynamically loaded at run time.

%package -n libdcap-tunnel-telnet
Summary: dCache GSI Tunnel
Group: Applications/System
Requires: libdcap
Obsoletes: dcache-dcap
%description -n libdcap-tunnel-telnet
This package contains the gsi tunnel plugin library used by dcachedcap.
This library is dynamically loaded at run time.

%prep

%setup -c

%build
libdir=%{prefix}/lib


arch=32
%ifos Linux
%ifarch x86_64
libdir=%{prefix}/lib64
arch=64
%endif
%endif
make clean
make install BIN_PATH=%{buildroot}%{prefix} LIB_PATH=%{buildroot}/${libdir}
OLDCWD=`pwd`
cd security_plugins/gssapi
make clean
make install ARCH=${arch} LIB_PATH=%{buildroot}/${libdir}
make clean
make -f  Makefile.gss install ARCH=${arch} LIB_PATH=%{buildroot}/${libdir}
cd ${OLDCWD}
cd security_plugins/telnet
make clean
make
make install ARCH=${arch} LIB_PATH=%{buildroot}/${libdir}
%clean
rm -rf $RPM_BUILD_ROOT


%files

%attr(0755,root,root) %{prefix}/bin/dccp

%changelog
* Fri Sep 26 2009 - owen.synge (at) desy.de
- Added cleaned up packaging.

* Wed Sep 24 2009 - owen.synge (at) desy.de
- Released first version of this packaging.

%files -n libdcap
%ifos Linux
%ifarch x86_64
%{prefix}/lib64/libdcap*.so
%{prefix}/lib64/libpdcap*.so
%else
%{prefix}/lib/libdcap*.so
%{prefix}/lib/libpdcap*.so
%endif
%else
%{prefix}/lib/libdcap*.so
%{prefix}/lib/libpdcap*.so
%endif

%files -n libdcap-devel
%defattr(-,root,root)
%{prefix}/include
%{prefix}/sources
%ifos Linux
%ifarch x86_64
%{prefix}/lib64/libdcap.a
%else
%{prefix}/lib/libdcap.a
%endif
%else
%{prefix}/lib/libdcap.a
%endif

%files -n libdcap-tunnel-gsi
%defattr(-,root,root)
%ifos Linux
%ifarch x86_64
%{prefix}/lib64/libgsiTunnel.so
%else
%{prefix}/lib/libgsiTunnel.so
%endif
%else
%{prefix}/lib/libgsiTunnel.so
%endif

%files -n libdcap-tunnel-krb
%defattr(-,root,root)
%ifos Linux
%ifarch x86_64
%{prefix}/lib64/libgssTunnel.so
%else
%{prefix}/lib/libgssTunnel.so
%endif
%else
%{prefix}/lib/libgssTunnel.so
%endif

%files -n libdcap-tunnel-telnet
%defattr(-,root,root)
%ifos Linux
%ifarch x86_64
%{prefix}/lib64/libtelnetTunnel.so
%else
%{prefix}/lib/libtelnetTunnel.so
%endif
%else
%{prefix}/lib/libtelnetTunnel.so
%endif
