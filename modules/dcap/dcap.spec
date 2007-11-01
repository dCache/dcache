#
# $Id: dcap.spec,v 1.1 2004-10-05 14:45:40 tigran Exp $
#
Vendor:       dCache.ORG
Distribution: All
Packager:     Tigran Mkrtchyan <tigran.mkrtchyan@dcache.org>

Group:        System
Name:         dcap
Version:      1.2.32
Release:      1
Copyright:    dCache PUBLIC License

Summary:      dCache Access client
URL:          http://www.dcache.org

BuildRoot:    /tmp/dcap-root
Source:       dcap-%{version}.tar.gz


Prefix:       /usr/local

# no autodependencies
AutoReqProv: no

%description
dCache Access Protocol client

%prep
%setup

%build
make

%install
make install BIN_PATH="$RPM_BUILD_ROOT/usr/local"

%clean
rm -rf "$RPM_BUILD_ROOT"


%files
%attr (0755, root, root)   /usr/local/bin/dccp
%attr (0644, root, root)   /usr/local/include/dc_hack.h
%attr (0644, root, root)   /usr/local/include/dcap.h
%attr (0644, root, root)   /usr/local/include/dcap_errno.h
%attr (0755, root, root)   /usr/local/lib/libdcap1.2.32.so
%attr (0755, root, root)   /usr/local/lib/libpdcap1.2.32.so
%attr (0644, root, root)   /usr/local/sources/dccp.c
