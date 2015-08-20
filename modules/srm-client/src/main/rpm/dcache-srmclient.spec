Summary: dCache Client
Vendor: dCache.ORG
Name: dcache-srmclient
Version: @Version@
Release: @Release@
BuildArch: noarch
Prefix: /usr
AutoReqProv: no

License: Free
Group: Applications/System

#  The following four definitions are to enforce RHEL/CentOS/SL-5
#  compatibility when building on SL-6 machines.
%define _source_payload w9.gzdio
%define _binary_payload w9.gzdio
%define _source_filedigest_algorithm 1
%define _binary_filedigest_algorithm 1

%description
dCache is a distributed mass storage system.

This package contains the client components.

%pre

%preun

%files
%defattr(-,root,root)
%attr(0755,root,root) /usr/bin/adler32
%attr(0755,root,root) /usr/bin/delegation
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
%attr(0755,root,root) /usr/bin/srmfs
%attr(0755,root,root) /usr/share/srm/lib/srm
%attr(0755,root,root) /usr/share/srm/lib/url-copy.sh
/usr/share/srm/lib
/usr/share/srm/conf
%changelog
