CHAPTER 12. dCache STORAGE RESOURCE MANAGER
===========================================

Storage Resource Managers (SRMs) are middleware components whose function is to
provide dynamic space allocation and file management on shared storage components on the Grid.
SRMs support protocol negotiation and a reliable replication mechanism.
The [SRM specification](https://sdm.lbl.gov/srm-wg/doc/SRM.v2.2.html) standardizes the interface,
thus allowing for a uniform access to heterogeneous storage elements.

The SRM utilizes the Grid Security Infrastructure (GSI) for authentication.
The SRM is a Web Service implementing a published WSDL document.
Please visit the [SRM Working Group Page](https://sdm.lbl.gov/srm-wg/)
to check out SRM Version 2.2 protocol specification documents.

The SRM protocol uses HTTP over GSI as a transport.
The dCache SRM implementation added HTTPS as a transport layer option.
The main benefits of using HTTPS rather than HTTP over GSI is that HTTPS is a standard protocol
and has support for sessions, improving latency in case a client needs to connect to the
same server multiple times.

-----
[TOC bullet hierarchy]
-----

## Configuring the srm service

### Basic setup

The SRM service is split between a front end `srm` and a backend `srmmanager` for scalability. To
instantiate the SRM service both cells need to be started, not necessarily on the same host.
The `srmmanager` is covered in more detail [here](config-srmmanager.md).

Like other services, the srm service can be enabled in the layout
file `/etc/dcache/layouts/mylayout` of your dCache installation. For an overview of the layout file
format, please see the
section [“Creating a minimal dCache configuration”](install.md#creating-a-minimal-dcache-configuration).

Example:

To enable SRM in dCache, add the following lines to your layout file:

```ini
[<srm-${host.name}Domain>]
[<srm-${host.name}Domain>/srm]

[srmmanager-${host.name}Domain]
[srmmanager-${host.name}Domain/srmmanager]
[srmmanager-${host.name}Domain/transfermanagers]
```

The additional `transfermanagers` service is required to perform 3rd party copy transfers initiated
by SRM or WebDAV.
This service is not required to be co-located with th SRM service (domain or host).

The srm service requires an authentication setup,
see [Chapter 10, Authorization in dCache](config-gplazma.md) for a general description or the
section [“Authentication and Authorization in dCache”](intouch.md#authentication-and-authorization-in-dcache)
for an example setup with X.509 certificates.

You can now copy a file into your dCache using the SRM,

> **NOTE**
>
> Please make sure to use the latest srmcp client, otherwise you will need to specify `-2` in order
> to use the right version.

```console-user
srmcp file:////bin/sh srm://dcache.example.org/data/world-writable/srm-test-file
```

copy it back

```console-user
srmcp srm://dcache.example.org/data/world-writable/srm-test-file file:////tmp/srmtestfile.tmp
```

and delete it

```console-user
srmrm srm://dcache.example.org/data/world-writable/srm-test-file
```

### Important SRM configuration options

The defaults for the following configuration parameters can be found
in the `srmmanager.properties`, `srm.properties` and
`transfermanagers.properties` files, which are all located in the directory
`/usr/share/dcache/defaults`.

If you want to modify parameters, copy them to
`/etc/dcache/dcache.conf` or to your layout file
`/etc/dcache/layouts/mylayout` and update their value.

Example:

Change the value for `srmmanager.db.host` in the layout file.

```ini
[srm-${host.name}Domain]
[srm-${host.name}Domain/srmmanager]
srmmanager.db.host=hostname
```

If a dCache instance contains more than one `srmmanager`, it is necessary that each one has a
_distinct_ database.

The property `srmmanager.request.copy.max-inprogress` controls number of copy requests in the
running state. Copy requests are 3-rd party srm transfers and therefore the
property `transfermanagers.limits.external-transfers` is best to be set to the same value as shown
below.

```ini
srmmanager.request.copy.max-inprogress=250
transfermanagers.limits.external-transfers=${srm.request.copy.threads}
```

The common value should be roughly equal to the maximum number of the SRM - to - SRM copies your
system can sustain.

Example:

So if you think about 3 gridftp transfers per pool and you have 30 pools then the number should be
3x30=90.

```ini
srmmanager.request.copy.max-inprogress=90
transfermanagers.limits.external-transfers=90
```


## Utilization of space reservations for data storage

`SRM` version 2.2 introduced the concept of space reservation. Space reservation guarantees that the
requested amount of storage space of a specified type is made available by the storage system for a
specified amount of time. Users can create space reservations using an appropriate `SRM` client,
although it is more common for the dCache administrator to make space reservations for VOs.

Space reservation is managed by the `spacemanager` service which is
detailed [here](config-spacemanager.md).

## Configuring the PostgreSQL database

We highly recommend to make sure that PostgreSQL database files are stored on a separate disk that
is not used for anything else (not even PSQL logging). BNL Atlas Tier 1 observed a great improvement
in srm-database communication performance after they deployed PSQL on a separate dedicated machine.

### SRM or SRM monitoring on a separate node

If `SRM` or srm monitoring is going to be installed on a separate
node, you need to add an entry in the file
`/var/lib/pgsql/data/pg_hba.conf` for this node as well:

    host    all         all       <monitoring node>    trust
    host    all         all       <srm node>           trust

The file `postgresql.conf` should contain the following:

    #to enable network connection on the default port
    max_connections = 100
    port = 5432
    ...
    shared_buffers = 114688
    ...
    work_mem = 10240
    ...
    #to enable autovacuuming
    stats_row_level = on
    autovacuum = on
    autovacuum_vacuum_threshold = 500  # min # of tuple updates before
                                       # vacuum
    autovacuum_analyze_threshold = 250      # min # of tuple updates before
                                            # analyze
    autovacuum_vacuum_scale_factor = 0.2    # fraction of rel size before
                                            # vacuum
    autovacuum_analyze_scale_factor = 0.1   # fraction of rel size before
    #
    # setting vacuum_cost_delay might be useful to avoid
    # autovacuum penalize general performance
    # it is not set in US-CMS T1 at Fermilab
    #
    # In IN2P3 add_missing_from = on
    # In Fermilab it is commented out

    # - Free Space Map -
    max_fsm_pages = 500000

    # - Planner Cost Constants -
    effective_cache_size = 16384            # typically 8KB each

## General SRM concepts (for developers)

### The SRM service

dCache `SRM` is implemented as a Web Service running in a Jetty servlet container and an Axis Web
Services engine. The Jetty server is executed as a cell, embedded in dCache and started
automatically by the `SRM` service. Other cells started automatically by `SRM`
are `SpaceManager`, `PinManager` and `RemoteGSIFTPTransferManager`. Of these services only `SRM` and
SpaceManager require special configuration.

The `SRM` concept consists of the five categories of functions:

- [Space Management](#space-management-functions)
- [Data Transfer Functions](#data-transfer-functions)
- [Request Status Functions](#request-status-functions)
- [Directory Functions](#directory-functions)
- [Permission Functions](#permission-functions)

### Space management functions

`SRM` version 2.2 introduces a concept of space reservation. Space reservation guarantees that the
requested amount of storage space of a specified type is made available by the storage system for a
specified amount of time. It is detailed in [this chapter](config-spacemanager.md).

We use three functions for space management:

- srmReserveSpace
- SrmGetSpaceMetadata
- srmReleaseSpace

Space reservation is made using the `srmReserveSpace` function. In case of successful reservation, a
unique name, called space token is assigned to the reservation. A space token can be used during the
transfer operations to tell the system to put the files being manipulated or transferred into an
associated space reservation. A storage system ensures that the reserved amount of the disk space is
indeed available, thus providing a guarantee that a client does not run out of space until all space
promised by the reservation has been used. When files are deleted, the space is returned to the
space reservation.

dCache only manages write space, i.e. space on disk can be reserved only for write operations. Once
files are migrated to tape, and if no copy is required on disk, space used by these files is
returned back into space reservation. When files are read back from tape and cached on disk, they
are not counted as part of any space. SRM space reservation can be assigned a non-unique description
that can be used to query the system for space reservations with a given description.

Properties of the SRM space reservations can be
discovered using the `SrmGetSpaceMetadata` function.

Space Reservations might be released with the function `srmReleaseSpace`.

For a complete description of the available space management functions please see
the [SRM Version 2.2 Specification](https://sdm.lbl.gov/srm-wg/doc/SRM.v2.2.html#_Toc241633085).

### Data transfer functions

#### SURLs and TURLs

`SRM` defines a protocol named `SRM`, and introduces a way to address the files stored in the `SRM`
managed storage by site URL (SURL of the format `srm://<host>:<port>/[<web service
path>?SFN=]<path>`.

Example:
Examples of the SURLs a.k.a. SRM URLs are:

    srm://fapl110.fnal.gov:8443/srm/managerv2?SFN=//pnfs/fnal.gov/data/test/file1
    srm://fapl110.fnal.gov:8443/srm/managerv1?SFN=/pnfs/fnal.gov/data/test/file2
    srm://srm.cern.ch:8443/castor/cern.ch/cms/store/cmsfile23

A transfer URL (TURL) encodes the file transport protocol in the URL.

Example:
gsiftp://gridftpdoor.fnal.gov:2811/data/test/file1

`SRM` version 2.2 provides three functions for performing data transfers:

- srmPrepareToGet
- srmPrepareToPut
- srmCopy

(in `SRM` version 1.1 these functions were called `get`, `put` and `copy`).

All three functions accept lists of SURLs as parameters. All data transfer functions perform
file/directory access verification and `srmPrepareToPut` and `srmCopy` check if the receiving
storage element has sufficient space to store the files.

`srmPrepareToGet` prepares files for read. These files are specified as a list of source SURLs,
which are stored in an SRM managed storage element. `srmPrepareToGet` is used to bring source files
online and assigns transfer URLs (TURLs) that are used for actual data transfer.

`srmPrepareToPut` prepares an SRM managed storage element to receive data into the list of
destination SURLs. It prepares a list of TURLs where the client can write data into.

Both functions support transfer protocol negotiation. A client supplies a list of transfer protocols
and the SRM server computes the TURL using the first protocol from the list that it supports.
Function invocation on the Storage Element depends on implementation and may range from simple SURL
to TURL translation to stage from tape to disk cache and dynamic selection of transfer host and
transfer protocol depending on the protocol availability and current load on each of the transfer
server load.

The function `srmCopy` is used to copy files between `SRM` managed storage elements. If both source
and target are local to the `SRM`, it performes a local copy. There are two modes of remote copies:

- PULL mode : The target `SRM` initiates an `srmCopy` request. Upon the
  client\\u0411\\u2500\\u2265s `srmCopy` request, the target `SRM` makes a space at the target
  storage, executes `srmPrepareToGet` on the source `SRM`. When the TURL is ready at the
  source `SRM`, the target `SRM` transfers the file from the source TURL into the prepared target
  storage. After the file transfer completes, `srmReleaseFiles` is issued to the source `SRM`.

- PUSH mode : The source `SRM` initiates an `srmCopy` request. Upon the
  client\\u0411\\u2500\\u2265s `srmCopy` request, the source `SRM` prepares a file to be transferred
  out to the target `SRM`, executes `srmPrepareToPut` on the target `SRM`. When the TURL is ready at
  the target `SRM`, the source SRM transfers the file from the prepared source into the prepared
  target TURL. After the file transfer completes, `srmPutDone` is issued to the target `SRM`.

When a specified target space token is provided, the files will be located in the space associated
with the space token.

`SRM` Version 2.2 `srmPrepareToPut` and `srmCopy` PULL mode transfers allow the user to specify a
space reservation token or a retention policy and access latency. Any of these parameters are
optional, and it is up to the implementation to decide what to do, if these properties are not
specified. The specification requires that if a space reservation is given, then the specified
access latency or retention policy must match those of the space reservation.

The Data Transfer Functions are asynchronous, an initial `SRM` call starts a request execution on
the server side and returns a request status that contains a unique request token. The status of
request is polled periodically by `SRM` get request status functions. Once a request is completed
and the client receives the TURLs the data transfers are initiated. When the transfers are completed
the client notifies the `SRM` server by executing `srmReleaseFiles` in case of `srmPrepareToGet`
or `srmPutDone` in case of `srmPrepareToPut`. In case of `srmCopy`, the system knows when the
transfers are completed and resources can be released, so it requires no special function at the
end.

Clients are free to cancel the requests at any time by execution of `srmAbortFiles`
or `srmAbortRequest`.

### Request status functions

The functions for checking the request status are:

- srmStatusOfReserveSpaceRequest
- srmStatusOfUpdateSpaceRequest
- srmStatusOfChangeSpaceForFilesRequest
- srmStatusOfChangeSpaceForFilesRequest
- srmStatusOfBringOnlineRequest
- srmStatusOfPutRequest
- srmStatusOfCopyRequest

### Directory functions

`SRM` Version 2.2, interface provides a complete set of directory management functions. These are

- srmLs
  ,
  srmRm
- srmMkDir
  ,
  srmRmDir
- srmMv

### Permission functions

SRM Version 2.2 supports the following three file permission functions:

- srmGetPermission
- srmCheckPermission
  and
- srmSetPermission

dCache contains an implementation of these functions that allows setting and checking of Unix file
permissions.

 <!-- [SRM specification]: https://sdm.lbl.gov/srm-wg/doc/SRM.v2.2.html
  [SRM Working Group Page]: http://sdm.lbl.gov/srm-wg/
  [???]: #in-install-layout
  [1]: #cf-gplazma
  [2]: #intouch-certificates
  [section\_title]: #cf-srm-space
  [3]: #cf-srm-expert-config
  [link groups]: #cf-pm-linkgroups
  [admin interface]: #intouch-admin
  [4]: #cf-pm-links-units
  [Webadmin Interface]: #cf-webadmin
  [Web Interface]: #intouch-web
  [`voms-proxy-init`]: #cf-gplazma-certificates-voms-proxy-init
  [above]: #cf-srm-space-linkgroups
  [`SpaceManagerLinkGroupAuthorizationFile`]: #cf-srm-linkgroupauthfile
  []: http://www.postgresql.org/docs/8.1/static/libpq-pgpass.html
  [5]: http://www.postgresql.org/docs/8.1/static/encryption-options.html
  [Properties of the SRM space reservations]: #cf-srm-intro-spaceReservation
  [SRM Version 2.2 Specification]: http://sdm.lbl.gov/srm-wg/doc/SRM.v2.2.html#_Toc241633085
--!>
