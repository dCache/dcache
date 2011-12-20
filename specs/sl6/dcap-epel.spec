Name:		dcap
Version:	2.47.6
Release:	1%{?dist}
Summary:	Client Tools for dCache

Group:		Applications/Internet
#		plugins/gssapi/{base64.[ch],gssIoTunnel.c,util.c} - BSD license
#		the rest - LGPLv2+ license
License:	LGPLv2+ and BSD
URL:		http://www.dcache.org/manuals/libdcap.shtml
#		The source tarfile is created from a subversion checkout:
#		svn export http://svn.dcache.org/dCache/tags/dcap-2.47.6 \
#		    dcap-2.47.6
#		tar -z -c -f dcap-2.47.6.tar.gz dcap-2.47.6
Source0:	%{name}-%{version}.tar.gz
#		Allow loading of plugins outside default library search path
Patch0:		%{name}-dlopen.patch
#		Reduce overlinking
#		http://rb.dcache.org/r/1646
Patch1:		%{name}-libs.patch.2.47.6
#		Fix strict aliasing warnings
#		http://rb.dcache.org/r/2632
Patch2:		%{name}-aliasing.patch
BuildRoot:	%{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

Requires:	%{name}-libs%{?_isa} = %{version}-%{release}
BuildRequires:	globus-gssapi-gsi-devel%{?_isa}
BuildRequires:	krb5-devel%{?_isa}
BuildRequires:	openssl-devel%{?_isa}
BuildRequires:	autoconf
BuildRequires:	automake
BuildRequires:	libtool

%description
dCache is a distributed mass storage system.
This package contains the client tools.

%package libs
Summary:	Client Libraries for dCache
Group:		System Environment/Libraries
License:	LGPLv2+

%description libs
dCache is a distributed mass storage system.
This package contains the client libraries.

%package devel
Summary:	Client Development Files for dCache
Group:		Development/Libraries
License:	LGPLv2+
Requires:	%{name}-libs%{?_isa} = %{version}-%{release}

%description devel
dCache is a distributed mass storage system.
This package contains the client development files.

%package tunnel-gsi
Summary:	GSI tunnel for dCache
Group:		System Environment/Libraries
License:	LGPLv2+ and BSD
Requires:	%{name}-libs%{?_isa} = %{version}-%{release}

%description tunnel-gsi
This package contains the gsi tunnel plugin library used by dcap-libs.
This library is dynamically loaded at runtime.

%package tunnel-krb
Summary:	Kerberos tunnel for dCache
Group:		System Environment/Libraries
License:	LGPLv2+ and BSD
Requires:	%{name}-libs%{?_isa} = %{version}-%{release}

%description tunnel-krb
This package contains the kerberos tunnel plugin library used by dcap-libs.
This library is dynamically loaded at runtime.

%package tunnel-ssl
Summary:	SSL tunnel for dCache
Group:		System Environment/Libraries
License:	LGPLv2+
Requires:	%{name}-libs%{?_isa} = %{version}-%{release}

%description tunnel-ssl
This package contains the ssl tunnel plugin library used by dcap-libs.
This library is dynamically loaded at runtime.

%package tunnel-telnet
Summary:	Telnet tunnel for dCache
Group:		System Environment/Libraries
License:	LGPLv2+
Requires:	%{name}-libs%{?_isa} = %{version}-%{release}

%description tunnel-telnet
This package contains the telnet tunnel plugin library used by dcap-libs.
This library is dynamically loaded at runtime.

%prep
%setup -q
%patch0 -p1
%patch1 -p0
%patch2 -p1

for f in Copyright base64.c base64.h util.c ; do
    iconv -f iso-8859-1 -t utf-8 plugins/gssapi/$f -o plugins/gssapi/$f.new
    mv plugins/gssapi/$f.new plugins/gssapi/$f
done

sed 's!@@LIBDIR@@!%{_libdir}!' -i src/tunnelManager.c

%build
chmod +x bootstrap.sh
./bootstrap.sh

# The defaults optflags creates a broken binary
export CFLAGS="%optflags"
%configure \
    --with-globus-include="%{_includedir}/globus -I%{_libdir}/globus/include" \
    --with-globus-lib=/dummy
make %{?_smp_mflags}

%install
rm -rf $RPM_BUILD_ROOT

make install DESTDIR=$RPM_BUILD_ROOT

# Remove static libraries and libtool archive files
rm -rf $RPM_BUILD_ROOT/%{_libdir}/*.a
rm -rf $RPM_BUILD_ROOT/%{_libdir}/*.la

# Move plugins out of the default library path
mkdir $RPM_BUILD_ROOT/%{_libdir}/%{name}
mv $RPM_BUILD_ROOT/%{_libdir}/lib*Tunnel* $RPM_BUILD_ROOT/%{_libdir}/%{name}

# We are installing the docs in the files sections
rm -rf $RPM_BUILD_ROOT/%{_docdir}

%clean
rm -rf $RPM_BUILD_ROOT

%post libs -p /sbin/ldconfig

%postun libs -p /sbin/ldconfig

%files
%defattr(-,root,root,-)
%{_bindir}/dccp
%_mandir/man1/dccp.1.gz
#### These seem to be removed in dcap-2.47.6 tag
#%{_bindir}/dcap_test
#%{_bindir}/dcap_url_test
#%{_bindir}/dcsuck
#%{_bindir}/readv_test
#%{_bindir}/wdccp

%files libs
%defattr(-,root,root,-)
%{_libdir}/libdcap.so.*
%{_libdir}/libpdcap.so.*
%dir %{_libdir}/%{name}
%doc LICENSE COPYING.LIB AUTHORS

%files devel
%defattr(-,root,root,-)
%{_libdir}/libdcap.so
%{_libdir}/libpdcap.so
%{_includedir}/dc_hack.h
%{_includedir}/dcap.h
%{_includedir}/dcap_errno.h

%files tunnel-gsi
%defattr(-,root,root,-)
%{_libdir}/%{name}/libgsiTunnel.so
%doc plugins/gssapi/Copyright

%files tunnel-krb
%defattr(-,root,root,-)
%{_libdir}/%{name}/libgssTunnel.so
%doc plugins/gssapi/Copyright

%files tunnel-ssl
%defattr(-,root,root,-)
%{_libdir}/%{name}/libsslTunnel.so

%files tunnel-telnet
%defattr(-,root,root,-)
%{_libdir}/%{name}/libtelnetTunnel.so

%changelog
* Fri Feb 11 2011 Fedora Release Engineering <rel-eng@lists.fedoraproject.org> - 2.47.5-2
- Rebuilt for https://fedoraproject.org/wiki/Fedora_15_Mass_Rebuild

* Sat Nov 06 2010 Mattias Ellert <mattias.ellert@fysast.uu.se> - 2.47.5-1
- New upstream release
- Drop dcap-docs.patch - implemented upstream
- Put CFLAGS back to default - the issue causing problem is fixed upstream

* Thu Jun 17 2010 Mattias Ellert <mattias.ellert@fysast.uu.se> - 2.47.2-2
- Adjust CFLAGS so that the compiled program works correctly

* Wed Apr 07 2010 Mattias Ellert <mattias.ellert@fysast.uu.se> - 2.47.2-1
- New upstream release
- Drop dcap-adler32.patch - implemented upstream

* Thu Mar 11 2010 Mattias Ellert <mattias.ellert@fysast.uu.se> - 2.44.0-3
- Add missing build requires on autotools
- Fix configure to look for functions in the right libraries

* Wed Mar 10 2010 Mattias Ellert <mattias.ellert@fysast.uu.se> - 2.44.0-2
- Use the adler32 function from zlib and drop the bundled source file
- Drop the zlib license tag again

* Wed Mar 10 2010 Mattias Ellert <mattias.ellert@fysast.uu.se> - 2.44.0-1
- Major revision of spec file - upstream has started using autotools
- Add zlib license tag due to the adler32 source

* Sun Jan  3 2010 Mattias Ellert <mattias.ellert@fysast.uu.se> - 1.2.44-2
- Porting to additional architectures
- Add BSD license tags for the tunnel-gsi and tunnel-krb sub packages

* Thu Dec 17 2009 Mattias Ellert <mattias.ellert@fysast.uu.se> - 1.2.44-1
- Update to version 1.2.44 (svn tag 1.9.3-7)

* Thu Sep 17 2009 Mattias Ellert <mattias.ellert@fysast.uu.se> - 1.2.42-2
- Update to new svn tag 1.9.3-3

* Thu Aug 13 2009 Mattias Ellert <mattias.ellert@fysast.uu.se> - 1.2.42-1
- Initial Fedora package based on svn tag 1.9.3-1
