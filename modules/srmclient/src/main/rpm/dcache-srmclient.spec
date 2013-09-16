#
# $Id: dcache-srmclient.spec.template,v 1.2 2007-07-05 09:22:06 timur Exp $
#
Summary: dCache Client
Vendor: dCache.ORG
Name: dcache-srmclient
Version: @Version@
Release: @Release@
BuildArch: noarch
Prefix: /usr
#BuildRoot: %{_topdir}/BUILDROOT/%{name}-%{version}
#Source0: dcache-srmclient.tar
AutoReqProv: no

License: Free
Group: Applications/System
%description
dCache is a distributed mass storage system.

This package contains the clinet components.

#%prep
#%setup -c
#%build
#%install
#rm -rf $RPM_BUILD_ROOT/*
#mkdir -p $RPM_BUILD_ROOT/opt/d-cache/
#cp -a $RPM_PACKAGE_NAME/* $RPM_BUILD_ROOT/opt/d-cache/
#%clean
#rm -rf $RPM_BUILD_ROOT/*
#rm -rf $RPM_PACKAGE_NAME


%pre

#%post

%preun

#%postun

%files
%defattr(-,root,root)
%attr(0755,root,root) /usr/bin/adler32
%attr(0755,root,root) /usr/bin/srm-get-request-status
%attr(0755,root,root) /usr/bin/srm-get-request-tokens
%attr(0755,root,root) /usr/bin/srm-get-request-summary
%attr(0755,root,root) /usr/bin/srmcp
%attr(0755,root,root) /usr/bin/srmls
%attr(0755,root,root) /usr/bin/srmrm
%attr(0755,root,root) /usr/bin/srmmkdir
%attr(0755,root,root) /usr/bin/srmmv
%attr(0755,root,root) /usr/bin/srmrmdir
%attr(0755,root,root) /usr/bin/srmstage
%attr(0755,root,root) /usr/bin/srm-advisory-delete
%attr(0755,root,root) /usr/bin/srm-storage-element-info
%attr(0755,root,root) /usr/bin/srm-get-metadata
%attr(0755,root,root) /usr/bin/srm-bring-online
%attr(0755,root,root) /usr/bin/srm-extend-file-lifetime
%attr(0755,root,root) /usr/bin/srm-get-permissions
%attr(0755,root,root) /usr/bin/srm-get-space-metadata
%attr(0755,root,root) /usr/bin/srm-get-space-tokens
%attr(0755,root,root) /usr/bin/srm-check-permissions
%attr(0755,root,root) /usr/bin/srmping
%attr(0755,root,root) /usr/bin/srm-release-space
%attr(0755,root,root) /usr/bin/srm-reserve-space
%attr(0755,root,root) /usr/bin/srm-set-permissions
%attr(0755,root,root) /usr/bin/srm-abort-files
%attr(0755,root,root) /usr/bin/srm-abort-request
%attr(0755,root,root) /usr/bin/srm-release-files
%attr(0755,root,root) /usr/share/srm/lib/srm
%attr(0755,root,root) /usr/share/srm/lib/url-copy.sh
/usr/share/srm/lib
/usr/share/srm/conf
%changelog
