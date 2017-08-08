Summary: dCache Server
Vendor: dCache.org
Name: dcache
Version: @Version@
Release: @Release@
BuildArch: noarch
Prefix: /
Packager: dCache.org <support@dcache.org>.

Obsoletes: dCacheConfigure
Obsoletes: dcache-server
Provides: dCachePostInstallConfigurationScripts
AutoReqProv: no
Requires(pre): shadow-utils
Requires(post): chkconfig
Requires(preun): chkconfig
# This is for /sbin/service
Requires(preun): initscripts
Requires: which

License: Distributable
Group: Applications/System

#  The following four definitions are to enforce RHEL/CentOS/SL-5
#  compatibility when building on SL-6 machines.
%define _source_payload w9.gzdio
%define _binary_payload w9.gzdio
%define _source_filedigest_algorithm 1
%define _binary_filedigest_algorithm 1

%description
dCache is a distributed mass storage system.

This package contains the server components.

%pre
if [ -d /opt/d-cache/classes ]; then
   echo "Can't update package in /opt (/opt/d-cache/classes exists)"
   exit 1
fi

/sbin/service dcache-server stop >/dev/null 2>&1

# Make sure the administrative user exists
getent group dcache >/dev/null || groupadd -r dcache
getent passwd dcache >/dev/null || \
    useradd -r -g dcache -d /var/lib/dcache -s /bin/bash \
    -c "dCache administrator" dcache

# check validity of dcache user and group
if [ "`id -u dcache`" -eq 0 ]; then
    echo "The dcache system user must not have uid 0 (root).
Please fix this and reinstall this package." >&2
    exit 1
fi
if [ "`id -g dcache`" -eq 0 ]; then
    echo "The dcache system user must not have root as primary group.
Please fix this and reinstall this package." >&2
    exit 1
fi

exit 0

%post
# generate admin door ssh2 server key
if [ ! -f /etc/dcache/admin/ssh_host_rsa_key ]; then
    ssh-keygen -q -t rsa -f /etc/dcache/admin/ssh_host_rsa_key -N ""
    chmod 640 /etc/dcache/admin/ssh_host_rsa_key
    chgrp dcache /etc/dcache/admin/ssh_host_rsa_key
fi

# fix file /var/lib/dcache directory ownership
chown dcache:dcache /var/lib/dcache

if [ ! -f /usr/share/dcache/lib/services.sh ]; then
    ln -s /usr/share/dcache/lib/services-daemon.sh /usr/share/dcache/lib/services.sh
fi

%preun
if [ $1 -eq 0 ] ; then
    /sbin/service dcache-server stop >/dev/null 2>&1
    /sbin/chkconfig --del dcache-server
fi

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root)
/usr/sbin/dcache-pool-meta-preupgrade
/usr/sbin/dcache-info-provider
/usr/sbin/dcache-billing-indexer
/usr/sbin/dcache-wait-for-cells
/usr/bin/chimera
/usr/bin/dcache
/usr/bin/dcache-star
/usr/share/doc/dcache
/usr/share/dcache
/usr/share/man/man8/dcache-bootloader.8
/usr/share/man/man8/dcache-billing-indexer.8
/usr/share/man/man8/dcache.8

%attr(-,dcache,dcache) /var/log/dcache
%attr(-,dcache,dcache) /var/lib/dcache/config
%attr(700,dcache,dcache) /var/lib/dcache/alarms
%attr(700,dcache,dcache) /var/lib/dcache/credentials
%attr(700,dcache,dcache) /var/lib/dcache/httpd
%attr(700,dcache,dcache) /var/lib/dcache/plots
%attr(700,dcache,dcache) /var/lib/dcache/pool-history
%attr(700,dcache,dcache) /var/lib/dcache/resilience
%attr(700,dcache,dcache) /var/lib/dcache/statistics
%attr(750,dcache,dcache) /var/lib/dcache/billing
%attr(770,dcache,dcache) /var/lib/dcache/star
%attr(755,dcache,dcache) /var/spool/dcache/star

%attr(0755,root,root) /etc/rc.d/init.d/dcache-server
%attr(0755,root,root) /etc/bash_completion.d/dcache
%config(noreplace) %attr(0644,root,root) /etc/security/limits.d/92-dcache.conf
%config(noreplace) %attr(0644,root,root) /etc/logrotate.d/dcache

%docdir /usr/share/doc/dcache
%config(noreplace) /etc/dcache
%config(noreplace) /var/lib/dcache

%changelog
