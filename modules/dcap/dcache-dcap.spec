#
# $Id: dcache-client.spec.template,v 1.3 2007-07-05 09:22:06 timur Exp $
#
Summary: dCache Client
Vendor: dCache.ORG
Name: dcachedcap
Version: 1.9.3
Release: 1rc
Prefix: /opt/d-cache/dcap
BuildRoot: %{_tmppath}/%{name}-%{version}-build
Source: %{name}.src.tgz

Provides: d-cache-dcap
AutoReqProv: no

License: Free
Group: Applications/System
%description
dCache is a distributed mass storage system.

This package contains the client components.
%package -n libdcachedcap
Summary: dCache Client libararies
Group: Applications/System
%description -n libdcachedcap
dCache is a distributed mass storage system.

This package contains the client libraries dcachedcap.
%package devel
Summary: dCache Client Headers
Group: Applications/System
#BuildArch: noarch
#Until rpm version 4.6.0 sub packages of different architectures are not suported
%description devel
This package contains the client libary dcachedcap header files.

%prep

%setup -c

%build
libdir=%{prefix}/lib



%ifos Linux
%ifarch x86_64
libdir=%{prefix}/lib64
%endif
%endif

make install BIN_PATH=%{buildroot}%{prefix} LIB_PATH=%{buildroot}/${libdir}


%clean
rm -rf $RPM_BUILD_ROOT

%pre

#%post

%preun

#%postun

%files

%attr(0755,root,root) %{prefix}/bin/dccp
%changelog

%files -n libdcachedcap
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

%files devel
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
