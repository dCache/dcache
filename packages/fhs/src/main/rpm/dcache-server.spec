Summary: dCache Server
Vendor: dCache.org
Name: dcache
Version: @Version@
Release: @Release@
BuildArch: noarch
Prefix: /
Packager: dCache.org <support@dcache.org>.

Obsoletes: dCacheConfigure
Provides: dCachePostInstallConfigurationScripts
Provides: dcache-server
AutoReqProv: no
Requires(pre): shadow-utils
Requires(post): chkconfig
Requires(preun): chkconfig
# This is for /sbin/service
Requires(preun): initscripts

License: Distributable
Group: Applications/System
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
# generate admin door server key
if [ ! -f /etc/dcache/admin/server_key ]; then
    ssh-keygen -q -b 768 -t rsa1 -f /etc/dcache/admin/server_key -N ""
    chmod 640 /etc/dcache/admin/server_key
    chgrp dcache /etc/dcache/admin/server_key
fi

# generate admin door host key
if [ ! -f /etc/dcache/admin/host_key ]; then
    ssh-keygen -q -b 1024 -t rsa1 -f /etc/dcache/admin/host_key -N ""
    chmod 640 /etc/dcache/admin/host_key
    chgrp dcache /etc/dcache/admin/host_key
fi

# generate admin door ssh2 server key
if [ ! -f /etc/dcache/admin/ssh_host_dsa_key ]; then
    ssh-keygen -q -t dsa -f /etc/dcache/admin/ssh_host_dsa_key -N ""
    chmod 640 /etc/dcache/admin/ssh_host_dsa_key
    chgrp dcache /etc/dcache/admin/ssh_host_dsa_key
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
/usr/bin/chimera-cli
/usr/bin/dcache
/usr/bin/dcache-star
/usr/share/doc/dcache
/usr/share/dcache
/usr/share/man/man8/dcache-bootloader.8
/usr/share/man/man8/dcache-billing-indexer.8
/usr/share/man/man8/dcache.8
/var/log/dcache

%attr(-,dcache,dcache) /var/lib/dcache/alarms
%attr(-,dcache,dcache) /var/lib/dcache/config
%attr(-,dcache,dcache) /var/lib/dcache/billing
%attr(-,dcache,dcache) /var/lib/dcache/httpd
%attr(-,dcache,dcache) /var/lib/dcache/plots
%attr(-,dcache,dcache) /var/lib/dcache/statistics
%attr(-,dcache,dcache) /var/lib/dcache/star
%attr(-,dcache,dcache) /var/spool/dcache/star
%attr(700,dcache,dcache) /var/lib/dcache/credentials

%attr(0755,root,root) /etc/rc.d/init.d/dcache-server
%attr(0755,root,root) /etc/bash_completion.d/dcache
%config(noreplace) %attr(0644,root,root) /etc/security/limits.d/92-dcache.conf

%docdir /usr/share/doc/dcache
%config(noreplace) /etc/dcache
%config(noreplace) /var/lib/dcache

%changelog
