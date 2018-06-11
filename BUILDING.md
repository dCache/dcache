Building dCache
===============

dCache uses Maven as a build system and as a repository of Maven
artifacts. A top level aggregator project allows all Maven modules to
be build in one operation:

    mvn package

Generated artifacts are to be found in the target directories inside
each component.

Specific modules can be build using the -am and -pl options of Maven,
e.g.:

    mvn package -am -pl packages/tar

will build the _packages/tar_ module and all modules required by
_packages/tar_.

It is not possible to cd into a subdirectory and execute Maven on a
single module only. Such an attempt would fail because internal
dependencies cannot be found in any Maven repository. Instead Maven
must be invoked on the top-level aggregator project with the
appropriate options to compile specific modules.

All generated files can be removed through the clean phase:

    mvn clean

This phase can also be combined with other phases, e.g.:

    mvn clean package

Packaging dCache
----------------

RPM and DEB packages can be build by compiling the _packages/fhs_
module with the _rpm_ and _deb_ profiles, respectively, i.e.:

    mvn clean package -am -pl packages/fhs -P rpm

or

    mvn clean package -am -pl packages/fhs -P deb

The two profiles must not be invoked in the same run. Note that the
platform native RPM and DEB build tools must be installed. For RPM,
installing the rpm and rpmbuild tools suffices. For DEB, the dpkg-dev,
debhelper, quilt, and fakeroot packages must be installed.

The package revision number can be customized by defining the
build.number property, e.g.:

    mvn clean package -am -pl packages/fhs -P deb -Dbuild.number=2

RPM packages of the srmclient can be created by running the package
phase with the _rpm_ profile on _modules/srmclient_ module, i.e.:

    mvn clean package -am -pl modules/srmclient -P rpm

DEB packages have not been defined for srmclient.

The generated packages can be found in the target directory of the
respectively module. RPMs are in the RPMS subdirectory.


The dCache tarball package is build by packaging the _packages/tar_
module:

    mvn clean package -am -pl modules/tar

It is a separate module from the RPM and DEB packages because the file
layout is significantly different in the tarball.

The system-test module
----------------------

The _packages/system-test_ module generates a ready-to-run single domain dCache
instance suitable for testing. It is configured to use an embedded database and
thus to run without postgresql. To build it simply run:

    mvn clean package -am -pl packages/system-test

This entails a completely self-contained dCache instance in
`packages/system-test/target/dcache`. It can be started using:

    packages/system-test/target/bin/ctlcluster start

To use GSI and TLS protocols, you may have to copy the test CA certificates
into your `/etc/grid-security/certificates/` directory. Follow the instructions
shown at the end of the build.

A test script is provided in `packages/system-test/target/bin/test` to execute
various test transfers. This script is not intended for automatic testing. It
requires that various grid-related tools are available on the host.


An interactive database console for the embedded database can be started using

    packages/system-test/target/bin/hsqldb DATABASE

where DATABASE is the database to inspect, usually a service name, eg. space
manager or pin manager. The embedded database is single process, and thus the
console can only be started if dCache is not running.


Multible versions of dCache can be tested for compatibility by installing
older versions using

    packages/system-test/target/bin/ctlcluster install VERSION

This will download a tarball release of that version and install it in
`packages/system-test/target`. The `ctlcluster` utility can be used to control
all versions at once. Using the switch subcommand, particular services can
be moved between versions, eg.

    packages/system-test/target/bin/ctlcluster switch pool.name=pool_write 2.7.5

will move the pool_write pool to version 2.7.5. The utility can however not
compensate for changes in configuration properties or database schemas between
versions. Such incompatibilities have to be resolved manually.


Unit tests
----------

By default Maven executes all unit tests while building. This can be
time consuming and will fail if no internet connection is
available. The unit tests can be disabled by appending the `-DskipTests`
option to any mvn command.
