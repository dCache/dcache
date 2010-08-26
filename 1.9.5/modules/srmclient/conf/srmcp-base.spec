#
# Spec file for Fermilab Srm Client Tools, OSG release
#
Version: %{MajVersion}.%{MinVersion}
Summary: Fermilab Srm Client Tools
Name: srmcp
License: FNAL/DOE (BSD-like Open Source licensing)
Group: Applications/System
Source: srmcp_v%{MajVersion}_%{MinVersion}_NULL.tar
URL: https://srm.fnal.gov/twiki/bin/view/SrmProject/WebHome
Distribution: RedHat/Fedora Linux
Vendor: Fermilab
Packager: Rob Kennedy <kennedy AT fnal DOT gov> and  Timur Perelmutov <timur AT fnal DOT gov>
BuildRoot: %{_topdir}/BUILD/%{name}-%{version}
Prefix: /opt/%{name}-%{version}

#
# M. Ernst's dcache.org spec file has this commented out.
# May be difficult to enforce as not all java REs are installed via RPMs.
# May not be appropriate, as this may not run under j2re 1.5.0
#
# Requires: j2re >= 1.4.2
# Prereq:   j2re
#
%description
Fermilab Srm Client Tools implement SRM v1.1 and v1.2 functionality. Also provided are
some related Data Movement and Storage tools we have found useful.
#
%prep
#
%build
#
%install
#
%clean
#
%post
#
%files
%defattr(-,root,root)
# %doc --- nothing appropriate to put into /usr/doc yet.
/opt/%{name}-%{version}
# NOTE: This will package every file in the above directory into the RPM!
#
%changelog
* Mon Apr 3 2006 timur
- Made package buildable in user space and relocatable
* Wed Jun  1 2005 root <root@ncdf134.fnal.gov>
- Initial build.
