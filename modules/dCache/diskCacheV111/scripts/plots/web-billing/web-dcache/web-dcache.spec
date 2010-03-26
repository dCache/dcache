Summary: A monitoring webapplication for dCache.
Name: web-dcache
Version: 1.1.3
Release: 1
License: GPL
Group: Applications/Utilities
Source: http://www.dcache.org/download/%{name}-%{version}.tar.gz
Source1: lps.tgz
Source2: http://jdbc.postgresql.org/download/postgresql-8.1-405.jdbc3.jar
Source3: plots-%{version}.tar
URL: http://www.dcache.org/
BuildRoot: %{_tmppath}/%{name}-root
#Requires: java ant jar
#BuildRequires: javac ant

%description
A monitoring webapplication for dCache that uses the Tomcat and OpenLaszlo servers.
Tomcat servlet gets the data from the billing database and prepares the files with plots.
OpenLaszlo server is used as a presentation layer.

# Here we use %setup macro to unpack the source code and to change directory to the place where the code was unpacked
%prep
%setup -q -n %{name} # Unpack the source code, do not suppress tar output

# Do everything to build the product
%build
ant
echo "Build done"

# Now install the build product
%install
echo "Install"
rm -rf %{buildroot}
# make install DESTDIR=%{buildroot} # Some ant command should be here to install the package into a virtual directory structure
mkdir -p %{buildroot}/%{prefix}/%{name}
echo buildroot=%{buildroot}
## echo RPM_BUILD_ROOT=$RPM_BUILD_ROOT  # same as %{buildroot}
## echo RPM_BUILD_DIR=$RPM_BUILD_DIR    #
echo PWD=$PWD
rm -rf src
rm -f Images/*
cd ..
tar cvfz %{buildroot}/%{prefix}/%{name}/%{name}.tgz %{name}
install -d $RPM_BUILD_ROOT/%{prefix}/share/java
install -m 644 %{SOURCE1} $RPM_BUILD_ROOT/%{prefix}/%{name}/lps.tgz
install -m 644 %{SOURCE2} $RPM_BUILD_ROOT/%{prefix}/share/java
install -m 644 %{SOURCE3} $RPM_BUILD_ROOT/%{prefix}/%{name}/plots.tar

cat << EOF > $RPM_BUILD_ROOT/%{prefix}/%{name}/INSTALL
To install the monitoring package manually do the following:
   0. Stop tomcat
   1. Untar web-dcache.tgz archive into tomcat webapps/ directory using 'tar xfz /opt-dcache/web-dcache/web-dcache.tgz' command
   2. Install lps servlet into tomcat webapps/ directory
     2.1 Untar lps.tgz archive using 'tar xfz /opt-dcache/web-dcache/lps.tgz' command
     2.3 Untar plots.tar archive using 'tar xf /opt-dcache/web-dcache/plots.tar' command
   3. Copy /opt/d-cache/share/java/postgresql-8.1-405.jdbc3.jar into tomcat common/lib/ directory
   4. In the file ...webapps/web-dcache/META-INF/context.xml put your real database name, DB username and the password. Set its protection to 0600
   5. Start tomcat and wait for a few minutes
   6. Go to URL: http://<your_server_name>:<tomcat_port_number>/lps/plots/src/plots.lzx
EOF

cat << EOF > $RPM_BUILD_ROOT/%{prefix}/%{name}/install.sh
#!/bin/sh
if [ x\$CATALINA_HOME == 'x' ]
 then echo "FATAL: CATALINA_HOME is not set! Exiting..."; exit 1
fi
cd /%{prefix}
(cd \$CATALINA_HOME/webapps; tar xfz /%{prefix}/%{name}/web-dcache.tgz)
(cd \$CATALINA_HOME/webapps; tar xfz /%{prefix}/%{name}/lps.tgz)
(cd \$CATALINA_HOME/webapps; tar xf  /%{prefix}/%{name}/plots.tar)
cp share/java/postgresql-8.1-405.jdbc3.jar \$CATALINA_HOME/common/lib/
echo "Don't forget to put your real database name, DB username and the password in the \$CATALINA_HOME/webapps/web-dcache/META-INF/context.xml"
echo
echo "Now restart tomcat to activate the application"
echo "Wait for a few minutes..."
echo "Now go to URL: http://<your_server_name>:<tomcat_port_number>/lps/plots/src/plots.lzx"
exit 0
EOF
chmod 755 $RPM_BUILD_ROOT/%{prefix}/%{name}/install.sh

echo "Install done"

%clean
rm -rf %{buildroot}

# List the files and directories, relative to BuildRoot, which the RPM should archive into the package.
# Wildcards can be used as /usr/bin/*
%files
%defattr(-, root, root)
# %doc AUTHORS COPYING ChangeLog NEWS README TODO
/%{prefix}/*
# %{_bindir}/*
# %{_libdir}/*.so.*
# %{_datadir}/%{name}
# %{_mandir}/man8/*

%changelog
* Fri Mar 26 2010 Vladimir Podstavkov <podstvkv@fnal.gov>
- Clean up the code. Prepare for SVN rep.
* Mon Aug 20 2007 Vladimir Podstavkov <podstvkv@fnal.gov>
- Change installation procedure to make more filexible.
* Mon Apr  3 2006 Vladimir Podstavkov <podstvkv@fnal.gov>
- Initial revision.

