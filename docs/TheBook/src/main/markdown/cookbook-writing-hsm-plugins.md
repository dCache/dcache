Nearline Storage Plugins
========================

Pools may copy files to nearline storage such as tape (a process also refered to
as "flushing") and recall those files at a later point (aka "staging").

To interface with such nearline storage systems, a driver is needed. The default
driver shipped with dCache merely calls out to a configurable script. Although
such scripts are easy to develop, the requirement to call out to an external
script for every file being flushed to nearline storage or staged from nearline
storage significantly limits the scalability of this approach. In particular for
tape systems it is crucial to batch together recalls from tapes to minimize tape
mounts, however a script invocation per file limits how many files can be
recalled at a time.

dCache has a third party accessible plugin system. One of the pluggable
components is the nearline storage interface.

-----
[TOC bullet hierarchy]
-----

## Nearline Requests

A nearline storage drive must support three types of requests: flush to the
nearline storage, stage from the nearline storage, and removal of a file from a
nearline storage. dCache calls out to the driver providing a corresponding
request object, either
[org.dcache.pool.nearline.spi.FlushRequest](https://github.com/dCache/dcache/blob/master/modules/dcache-nearline-spi/src/main/java/org/dcache/pool/nearline/spi/FlushRequest),
[org.dcache.pool.nearline.spi.StageRequest](https://github.com/dCache/dcache/blob/master/modules/dcache-nearline-spi/src/main/java/org/dcache/pool/nearline/spi/StageRequest.java),
or
[org.dcache.pool.nearline.spi.RemoveRequest](https://github.com/dCache/dcache/blob/master/modules/dcache-nearline-spi/src/main/java/org/dcache/pool/nearline/spi/RemoveRequest.java).
The request object describes the file that is to be flushed, staged or removed,
and acts as a callback through which the driver reports progress, completion or
failure of the request.

Each request object has a unique ID and dCache may cancel a nearline request at
any time using this ID.

## Identifying Replicas

In dCache, a physical copy of a logical file is called a *replica*. Thus pools
contain replicas. It is the responsibility of the nearline storage driver to
copy replicas from the pool to the nearline storage and to copy replicas back
from the nearline storage into the pool.

Traditionally, dCache pools have stored replicas in the local file system, and a
flush or stage request would refer to the replica in the local file system by
path. Starting with dCache 3.0, *replica store plugins* may provide alternative
backends such as CEPH and thus replicas in a pool may not be accessible through
the local file system. For this reason, dCache 3.0 and newer identify the
replica in the pool by URI. Unless an alternative replica store is used, such a
URI will always use the `file:` scheme. If compatibility with versions earlier
than 2.17 is desired, a driver should refrain from using the `getReplicaUri`
method.

Once flushed to a nearline storage, the replica is identified by a URI generated
by the driver. This URI should use the nearline storage type (typically
`enstore` or `osm`) as the scheme and the nearline storage instance name as the
authority. The rest of the URI can be used to encode other information needed by
the driver to identify the file. dCache stores the generated URI in its name
space. Upon recalling a replica from nearline storage or removing it from
nearline storage, the URI is used to identify it.

## Request Lifecycle

From the point of view of dCache, the request goes through three steps: QUEUED,
ACTIVE and then either COMPLETED or FAILED. It is the responsibility of the
driver to queue and schedule requests as appropriate. This is because only the
driver knows e.g. the tape layout and can schedule requests to minimize tape
mounts. The driver must however issue the appropriate callbacks to dCache to
signal the transition from QUEUED to ACTIVE and from ACTIVE to either COMPLETED
or FAILED.

A nearline request has a completion deadline. Once the deadline is reached, the
pool will likely cancel the request. This is not a guarantee that it will be
cancelled, nor is this a guarantee that it will not be cancelled earlier. A
driver may use the deadline in it scheduling decision.

Upon activating a request, the driver must be prepared that dCache aborts the
operation. E.g. upon flushing a file, dCache will verify that the file still
exists in the name space and if not will abort the flush. Similarly, a driver
must be prepared that the activation is not instantaneous. E.g. upon staging a
file from tape, the pool does not reserves space for the file until the stage is
activated and thus activation may block until space becomes available (files are
garbage collected or previously staged files are migrated away from the pool).
For this reason the activation callback is asynchronous, providing a
`ListenableFuture` as a result. The driver can obtain the result of the
activation from this future once it is available.

## The Nearline Storage SPI

A third party plugin must implement two interfaces: An implementation of
[org.dcache.pool.nearline.spi.NearlineStorageProvider](https://github.com/dCache/dcache/blob/master/modules/dcache-nearline-spi/src/main/java/org/dcache/pool/nearline/spi/NearlineStorageProvider.java)
acts as a loader or factory for the nearline storage, while
[org.dcache.pool.nearline.spi.NearlineStorage](https://github.com/dCache/dcache/blob/master/modules/dcache-nearline-spi/src/main/java/org/dcache/pool/nearline/spi/NearlineStorage.java)
is that actual driver instance.

dCache uses the Java service loader facility to discover plugins. To make the
plugin discoverable by dCache, the compiled classes and a file called
`META-INF/services/org.dcache.pool.nearline.spi.NearlineStorageProvider` have to
be packaged as a jar file. The
`org.dcache.pool.nearline.spi.NearlineStorageProvider` file must contain the
fully qualified class name of the `NearlineStorageProvider` implementation.
Place this jar in a sub-directory underneath `/usr/local/share/dcache/plugins/`.
After restarting the dCache pool, the driver should be visible in the admin
shell.

To simplify setting up the basic structure of a nearline storage plugin, we
provide a Maven archetype. See below for further details.

## AbstractBlockingNearlineStorage

Although it appears simple, the `NearlineStorageProvider` interface can be
difficult to implement as it is an asynchronous interface. Only an asynchronous
interface allows the nearline storage SPI to scale to tens or hundreds of
thousands simulatenous requests. Compared to the classic callout to a script,
even a blocking implementation will provide significant advantages.

To simplify development, dCache provides the
[AbstractBlockingNearlineStorage](https://github.com/dCache/dcache/blob/master/modules/dcache/src/main/java/org/dcache/pool/nearline/AbstractBlockingNearlineStorage.java)
base class. Subclasses implement the abstract flush, stage and remove methods in
a blocking fashion and the base class deal with request activation, cancellation
and completion or failure callbacks. Subclasses provide three thread pools to
use, allowing full control over concurrency and request queueing.

In fact, the `script` driver shipped with dCache is a subclass of
`AbstractBlockingNearlineStorage`.

## Maven Archetype

A Maven Archetype is a template for new Maven projects. We provide an archetype
as a starting point for writing nearline storage plugins for dCache. To
instantiate the archetype, run

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
$ mvn archetype:generate
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Maven will prompt you to select an archetype from a list. Select
`org.dcache:dcache-nearline-plugin-archetype`. Maven will then prompt you to
enter a group ID (typically starts with a domain name under your control in
reverse order, e.g. `org.example`), an artifact ID (which identifies your Maven
project), a project version number, a Java package name (often starts with the
group ID), a descriptive string, and a driver name that identifies it in the
admin interface.

The dCache version defaults to the archetype version. It can be altered by
refusing to accept the entered values. This will cause Maven to prompt for the
values a second time and this time is also prompts for the dCache version. This
should rarely be necessary for a nearline storage plugin, as the SPI does not
change often. There is a good chance that your plugin will work with many
different versions of dCache (we provide no guarantees though!).

Example:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
$ mvn archetype:generate
[INFO] Scanning for projects...
[INFO]
[INFO] ------------------------------------------------------------------------
[INFO] Building Maven Stub Project (No POM) 1
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO] >>> maven-archetype-plugin:2.2:generate (default-cli) > generate-sources @ standalone-pom >>>
[INFO]
[INFO] <<< maven-archetype-plugin:2.2:generate (default-cli) < generate-sources @ standalone-pom <<<
[INFO]
[INFO] --- maven-archetype-plugin:2.2:generate (default-cli) @ standalone-pom ---
[INFO] Generating project in Interactive mode
[INFO] No archetype defined. Using maven-archetype-quickstart (org.apache.maven.archetypes:maven-archetype-quickstart:1.0)
Choose archetype:
1: https://download.dcache.org/nexus/content/repositories/releases/ -> org.dcache:xrootd4j-channelhandler-plugin-archetype (-)
2: https://download.dcache.org/nexus/content/repositories/releases/ -> org.dcache:xrootd4j-authz-plugin-archetype (-)
3: https://download.dcache.org/nexus/content/repositories/releases/ -> org.dcache:dcache-nearline-plugin-archetype (-)
Choose a number or apply filter (format: [groupId:]artifactId, case sensitive contains): : 3
Define value for property 'groupId': : org.example
Define value for property 'artifactId': : test-nearlinestorage
Define value for property 'version':  1.0-SNAPSHOT: :
Define value for property 'package':  org.example: : org.example.dcache
[INFO] Using property: dcache = 2.16.5
Define value for property 'description': : Test NearlineStorage to demonstrate the archetype
Define value for property 'name':  org.example.dcache.test-nearlinestorage: : org.example.test
Confirm properties configuration:
groupId: org.example
artifactId: test-nearlinestorage
version: 1.0-SNAPSHOT
package: org.example.dcache
dcache: 2.16.5
description: Test NearlineStorage to demonstrate the archetype
name: org.example.test
 Y: : y
[INFO] ----------------------------------------------------------------------------
[INFO] Using following parameters for creating project from Archetype: dcache-nearline-plugin-archetype:2.16.5
[INFO] ----------------------------------------------------------------------------
[INFO] Parameter: groupId, Value: org.example
[INFO] Parameter: artifactId, Value: test-nearlinestorage
[INFO] Parameter: version, Value: 1.0-SNAPSHOT
[INFO] Parameter: package, Value: org.example.dcache
[INFO] Parameter: packageInPathFormat, Value: org/example/dcache
[INFO] Parameter: package, Value: org.example.dcache
[INFO] Parameter: version, Value: 1.0-SNAPSHOT
[INFO] Parameter: name, Value: org.example.test
[INFO] Parameter: dcache, Value: 2.16.5
[INFO] Parameter: groupId, Value: org.example
[INFO] Parameter: description, Value: Test NearlineStorage to demonstrate the archetype
[INFO] Parameter: artifactId, Value: test-nearlinestorage
[INFO] project created from Archetype in dir: /private/tmp/test-nearlinestorage
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time: 08:50 min
[INFO] Finished at: 2016-07-14T10:28:50+02:00
[INFO] Final Memory: 16M/304M
[INFO] ------------------------------------------------------------------------
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The result is a ready to compile Maven project:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
$ cd test-nearlinestorage/
$ tree
.
├── LICENSE.txt
├── README.md
├── pom.xml
└── src
    └── main
        ├── assembly
        │   └── plugin.xml
        ├── defaults
        │   └── org.example.test.properties
        ├── docs
        │   └── README.md
        ├── java
        │   └── org
        │       └── example
        │           └── dcache
        │               ├── PluginNearlineStorage.java
        │               └── PluginNearlineStorageProvider.java
        └── resources
            └── META-INF
                └── services
                    └── org.dcache.pool.nearline.spi.NearlineStorageProvider

10 directories, 8 files
$ mvn package
[INFO] Scanning for projects...
[INFO]
[INFO] ------------------------------------------------------------------------
[INFO] Building Test NearlineStorage to demonstrate the archetype 1.0-SNAPSHOT
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO] --- maven-resources-plugin:2.6:resources (default-resources) @ test-nearlinestorage ---
[WARNING] Using platform encoding (UTF-8 actually) to copy filtered resources, i.e. build is platform dependent!
[INFO] Copying 0 resource
[INFO]
[INFO] --- maven-compiler-plugin:3.1:compile (default-compile) @ test-nearlinestorage ---
[INFO] Changes detected - recompiling the module!
[WARNING] File encoding has not been set, using platform encoding UTF-8, i.e. build is platform dependent!
[INFO] Compiling 2 source files to /private/tmp/test-nearlinestorage/target/classes
[INFO]
[INFO] --- maven-resources-plugin:2.6:testResources (default-testResources) @ test-nearlinestorage ---
[WARNING] Using platform encoding (UTF-8 actually) to copy filtered resources, i.e. build is platform dependent!
[INFO] skip non existing resourceDirectory /private/tmp/test-nearlinestorage/src/test/resources
[INFO]
[INFO] --- maven-compiler-plugin:3.1:testCompile (default-testCompile) @ test-nearlinestorage ---
[INFO] No sources to compile
[INFO]
[INFO] --- maven-surefire-plugin:2.12.4:test (default-test) @ test-nearlinestorage ---
[INFO] No tests to run.
[INFO]
[INFO] --- maven-jar-plugin:2.4:jar (default-jar) @ test-nearlinestorage ---
[INFO] Building jar: /private/tmp/test-nearlinestorage/target/test-nearlinestorage-1.0-SNAPSHOT.jar
[INFO]
[INFO] --- maven-assembly-plugin:2.6:single (make-assembly) @ test-nearlinestorage ---
[INFO] Reading assembly descriptor: src/main/assembly/plugin.xml
[INFO] Copying files to /private/tmp/test-nearlinestorage/target/test-nearlinestorage-1.0-SNAPSHOT
[WARNING] Assembly file: /private/tmp/test-nearlinestorage/target/test-nearlinestorage-1.0-SNAPSHOT is not a regular file (it may be a directory). It cannot be attached to the project build for installation or deployment.
[INFO] Building tar: /private/tmp/test-nearlinestorage/target/test-nearlinestorage-1.0-SNAPSHOT.tar.gz
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time: 1.510 s
[INFO] Finished at: 2016-07-14T10:33:29+02:00
[INFO] Final Memory: 23M/306M
[INFO] ------------------------------------------------------------------------
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The resulting tar file can be shipped to customers and unpackaged in
`/usr/local/share/dcache/plugins`. Obviously you need to modify the generated
files to do something useful first.

## Examples

The simplest functional nearline storage driver is probably the `copy` driver
that ships with dCache. It sequentially copies the replicas to and from another
directory in the local file system. The main class is
[CopyNearlineStorage](https://github.com/dCache/dcache/blob/2.16/modules/dcache/src/main/java/org/dcache/pool/nearline/filesystem/CopyNearlineStorage.java),
although the bulk of the code is in the base class
[FileSystemNearlineStorage](https://github.com/dCache/dcache/blob/2.16/modules/dcache/src/main/java/org/dcache/pool/nearline/filesystem/FileSystemNearlineStorage.java),
itself a subclass of `AbstractBlockingNearlineStorage`.

For a more complicated example, check out the `script` driver. It demonstrates
configurable concurrency. The main class is
[ScriptNearlineStorage](https://github.com/dCache/dcache/blob/2.16/modules/dcache/src/main/java/org/dcache/pool/nearline/script/ScriptNearlineStorage.java).

For a full blown asynchronous driver, check out the [`endit` driver]
(https://github.com/gbehrmann/dcache-endit-provider) developed at NDGF to
interface with the [Endit](https://github.com/maswan/endit) tape integration
system.
