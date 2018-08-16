CHAPTER 13. dCache STORAGE RESOURCE MANAGER
===========================================


Table of Contents 
------------------

+ [Introduction](#introduction)
+ [Configuring the srm service](#configuring-the-srm-service)

	 [The Basic Setup](#the-basic-setup)
	 [Important srm configuration options](#important-srm-configuration-options)
	   
+ [Utilization of Space Reservations for Data Storage](#utilization-of-space-reservations-for-data-storage)

	 [Properties of Space Reservation](#properties-of-space-reservation)
+ [dCache specific concepts](#dcache-specific-concepts)

	 [Activating SRM SpaceManager](#activating-srm-spacemanager)
	 [Explicit and Implicit Space Reservations for Data Storage in dCache](#explicit-and-implicit-space-reservations-for-data-storage-in-dcache)
	   
+ [SpaceManager configuration](#spacemanager-configuration)

	 [SRM SpaceManager and Link Groups](#srm-spacemanager-and-link-groups)
         [Making a Space Reservation](#making-a-space-reservation)
	 [SRM configuration for experts](#srm-configuration-for-experts)
		
+ [Configuring the PostgreSQL Database](#configuring-the-postgresql-database)

	 [SRM or srm monitoring on a separate node](#srm-or-srm-monitoring-on-a-separate-node)
	     
+ [General SRM Concepts (for developers)](#general-srm-concepts-(for-developers))

	 [The SRM service](#the-srm-service)  
	 [Space Management Functions](#space-management-functions)  
	 [Data Transfer Functions](#data-transfer-functions)  
	 [Request Status Functions](#request-status-functions)  
	 [Directory Functions](#directory-functions)  
	 [Permission functions](#permission-functions)  

Introduction
============

Storage Resource Managers (SRMs) are middleware components whose function is to provide dynamic space allocation and file management on shared storage components on the Grid. SRMs support protocol negotiation and a reliable replication mechanism. The [SRM specification](https://sdm.lbl.gov/srm-wg/doc/SRM.v2.2.html) standardizes the interface, thus allowing for a uniform access to heterogeneous storage elements.

The SRM utilizes the Grid Security Infrastructure (GSI) for authentication. The SRM is a Web Service implementing a published WSDL document. Please visit the [SRM Working Group Page](https://sdm.lbl.gov/srm-wg/) to see the SRM Version 1.1 and SRM Version 2.2 protocol specification documents.

The SRM protocol uses HTTP over GSI as a transport. The dCache SRM implementation added HTTPS as a transport layer option. The main benefits of using HTTPS rather than HTTP over GSI is that HTTPS is a standard protocol and has support for sessions, improving latency in case a client needs to connect to the same server multiple times. The current implementation does not offer a delegation service. Hence `srmCopy` will not work with SRM over HTTPS. A separate delegation service will be added in a later release.

CONFIGURING THE SRM SERVICE
============================

THE BASIC SETUP
---------------

Like other services, the srm service can be enabled in the layout file **/etc/dcache/layouts/mylayout** of your dCache installation. For an overview of the layout file format, please see [the section called “Defining domains and services”](install.md#defining-domains-and-services).

Example:  

To enable SRM in a separate <srm-${host.name}Domain> in dCache, add the following lines to your layout file:

    [<srm-${host.name}Domain>]
    [<srm-${host.name}Domain>/srm]

The use of the srm service requires an authentication setup, see [Chapter 10, Authorization in dCache](config-gplazma.md) for a general description or the [section called “Authentication and Authorization in dCache”](intouch.md#authentication-and-authorization-in-dcache) for an example setup with X.509 certificates.

You can now copy a file into your dCache using the SRM,

> **NOTE**
>
> Please make sure to use latest srmcp client otherwise you will need to specify `-2` in order to use the right version.

    [user] $ srmcp file:////bin/sh srm://<dcache.example.org>:<8443>/data/world-writable/srm-test-file

copy it back

    [user] $ srmcp srm://<dcache.example.org>:<8443>/data/world-writable/srm-test-file file:////tmp/srmtestfile.tmp

and delete it

    [user] $ srmrm srm://<dcache.example.org>:<8443>/data/world-writable/srm-test-file

IMPORTANT SRM CONFIGURATION OPTIONS
----------------------------------------

The defaults for the following configuration parameters can be found in the **.properties** files in the directory **/usr/share/dcache/defaults**.

If you want to modify parameters, copy them to **/etc/dcache/dcache.conf** or to your layout file **/etc/dcache/layouts/mylayout** and update their value.

Example:  

Change the value for `srm.db.host` in the layout file.

    [srm-${host.name}Domain]
    [srm-${host.name}Domain/srm]
    srm.db.host=hostname

The property `srm.request.copy.threads` controls number of copy requests in the running state. Copy requests are 3-rd party srm transfers and therefore the property `transfermanagers.limits.external-transfers` is best to be set to the same value as shown below.

    srm.request.copy.threads=250
    transfermanagers.limits.external-transfers=${srm.request.copy.threads}

The common value should be the roughly equal to the maximum number of the SRM - to -SRM copies your system can sustain.

Example:  

So if you think about 3 gridftp transfers per pool and you have 30 pools then the number should be 3x30=90.

    srm.request.copy.threads=90
    transfermanagers.limits.external-transfers=90

Example:  

US-CMS T1 has:

    srm.request.copy.threads=2000
    transfermanagers.limits.external-transfers=2000

> **NOTE**
>
> `SRM` might produce a lot of log entries, especially if it runs in debug mode. It is recommended to make sure that logs are redirected into a file on a large disk.

UTILIZATION OF SPACE RESERVATIONS FOR DATA STORAGE
==================================================

`SRM` version 2.2 introduced a concept of space reservation. Space reservation guarantees that the requested amount of storage space of a specified type is made available by the storage system for a specified amount of time.

Users can create space reservations using an appropriate `SRM` client, although it is more common for the dCache administrator to make space reservations for VOs (see [the section called “SpaceManager configuration”](#spacemanager-configuration). Each space reservation has an associated ID (or space token). VOs then can copy directly into space tokens assigned to them by the dCache administrator.

When a file is about to be transferred to a storage system, the space available in the space reservation is checked if it can accomodate the entire file. If yes, this chunk of space is marked as allocated, so that it can not be taken by another, concurrently transferred file. If the file is transferred successfully the allocated space becomes used space within the space reservation, else the allocated space is released back to the space reservation as free space.

`SRM` space reservation can be assigned a non-unique description which can be used to query the system for space reservations with a given description.

dCache only manages write space, i.e. space on disk can be reserved only for write operations. Once files are migrated to tape, and if no copy is required on disk, space used by these files is returned back into space reservation. When files are read back from tape and cached on disk, they are not counted as part of any space.

PROPERTIES OF SPACE RESERVATION
-------------------------------

A space reservation has a retention policy and an access latency.

Retention policy describes the quality of the storage service that will be provided for the data (files) stored in the space reservation and access latency describes the availability of this data. The `SRM` specification requires that if a space reservation is given on upload, then the specified retention policy and access latency must match those of the space reservation.

The default values for the retention policy and access latency can be changed in the file **/etc/dcache/dcache.conf**.

**Retention policy**  
The values of retention policy supported by dCache are `REPLICA` and `CUSTODIAL`.

-   `REPLICA` corresponds to the lowest quality of the service, usually associated with storing a single copy of each file on the disk.

-   `CUSTODIAL` is the highest quality service, usually interpreted as storage of the data on tape.

Once a file is written into a given space reservation, it inherits the reservation's retention policy.

If the space reservation request does not specify a retention policy, we will assign a value given by `spacemanager.default-retention-policy`. The default value is `CUSTODIAL`.

Edit the file **/etc/dcache/dcache.conf** to change the default value.

Example:  
Change the default value to `REPLICA`.

    spacemanager.default-retention-policy=REPLICA

> **NOTE**
>
> `spacemanager.default-retention-policy` merely specifies to value to use while allocating space reservations when no value was given by the client or dCache admin. It is not to be confused with `pnfsmanager.default-retention-policy` which specifies the default retention policy of files uploaded outside of any space reservation.

**Access latency**  
The two values allowed for access latency are `NEARLINE` and `ONLINE`.

-   `NEARLINE` means that data stored in this reservation is allowed to migrate to permanent media. Retrieving these data may result in delays associated with preparatory steps that the storage system has to perform to make these data available for the user I/O (e.g., staging data from tape to a disk cache).

-   `ONLINE` means that data is readily available allowing for faster access.

In case of dCache `ONLINE` means that there will always be a copy of the file on disk, while `NEARLINE` does not provide such guarantee. As with retention policy, once a file is written into a given space reservation, it inherits the reservation's access latency.

If a space reservation request does not specify an access latency, we will assign a value given by `spacemanager.default-access-latency`. The default value is `NEARLINE`.

Edit the file **/etc/dcache/dcache.conf** to change the default value.

Example:
Change the default value to `ONLINE`.

    spacemanager.default-access-latency=ONLINE

> **NOTE**
>
> `spacemanager.default-access-latency` merely specifies to value to use while allocating space reservations when no value was given by the client or dCache admin. It is not to be confused with `pnfsmanager.default-access-latency` which specifies the default retention policy of files uploaded outside of any space reservation.

> **IMPORTANT**
>
> Please make sure to use capital letters for `REPLICA`, `CUSTODIAL`, `ONLINE` and `NEARLINE` otherwise you will receive an error message.

dCache SPECIFIC CONCEPTS
========================

ACTIVATING SRM SPACEMANAGER
-----------------------------

In order to enable the `SRM SpaceManager` you need to add the `spacemanager` service to your layout file

   [dCacheDomain]
   [dCacheDomain/spacemanager]

Unless you have reason not to, we recommend placing the `spacemanager` service in the same domain as the `poolmanager` service.

In order to use the just defined service, add the following definition in the file **/etc/dcache/dcache.conf**

    dcache.enable.space-reservation=true

followed by a restart of the domain containing the SRM service as well as all active doors.


EXPLICIT AND IMPLICIT SPACE RESERVATIONS FOR DATA STORAGE IN dCache
-------------------------------------------------------------------

### Explicit Space Reservations  

Each SRM space reservation is made against the total available disk space of a particular link group. If dCache is configured correctly each byte of disk space, that can be reserved, belongs to one and only one link group. See [the section called “SpaceManager configuration”](spacemanager-configuration) for a detailed description.

> **IMPORTANT**
>
> Make sure that no pool belongs to more than one pool group, no pool group belongs to more than one link and no link belongs to more than one link group.

If a space reservation is specified during upload, the file will be stored in it.

Files written into a space made within a particular link group will end up on one of the pools belonging to this link group. The difference between the link group's free space and the sum of all its space reservation's unused space is the available space of the link group. The available space of a link group is the space that can be allocated for new space reservations.

The total space in dCache that can be reserved is the sum of the available spaces of all link groups. Note however that a space reservation can never span more than a single link group.

### Implicit Space Reservations

dCache can perform implicit space reservations for non-`SRM` transfers, `SRM` Version 1 transfers and for `SRM` Version 2.2 data transfers that are not given the space token explicitly. The parameter that enables this behavior is srm.enable.space-reservation.implicit, which is described in [the section called “SRM configuration for experts”](srm-configuration-for-experts). If no implicit space reservation can be made, the transfer will fail.

Implicit space reservation means that the `srm` will create a space reservation for a single upload while negotiating the transfer parameters with the client. The space reservation will be created in a link group for which the user is authorized to create space reservations, which has enough available space, and which is able to hold the type of file being uploaded. The space reservation will be short lived. Once it expires, it will be released and the file it held will live on outside any space reservation, but still within the link group to which it was uploaded. Implicit space reservations are thus a technical means to upload files to link groups without using explicit space reservations.

The reason dCache cannot just allow the file to be uploaded to the link group without any space reservation at all is, that we have to guarantee, that space already allocated for other reservations isn’t used by the file being uploaded. The best way to guarantee that there is enough space for the file is to make a space reservation to which to upload it.

In case of `SRM` version 1.1 data transfers, where the access latency and retention policy cannot be specified, and in case of `SRM` V2.2 clients, when the access latency and retention policy are not specified, default values will be used. First `SRM` will attempt to use the values of access latency and retention policy tags from the directory to which a file is being written. If the tags are not present, then the access latency and retention policy will be set on basis of `pnfsmanager` defaults controlled by `pnfsmanager.default-retention-policy` and `pnfsmanager.default-access-latency` variables in **/etc/dcache/dcache.conf**.

You can check if the `AccessLatency` and `RetentionPolicy` tags are present by using the following command:  

        [root] # /usr/bin/chimera lstag /path/to/directory
	Total: numberOfTags
	tag1
	tag2
	..
	AccessLatency
	RetentionPolicy

If the output contains the lines AccessLatency and RetentionPolicy then the tags are already present and you can get the actual values of these tags by executing the following commands, which are shown together with example outputs:

     Example:  
     
     [root] # /usr/bin/chimera readtag /data/experiment-a AccessLatency
     ONLINE
     [root] # /usr/bin/chimera readtag /data/experiment-a RetentionPolicy
     CUSTODIAL

The valid `AccessLatency` values are `ONLINE` and `NEARLINE`, valid `RetentionPolicy` values are `REPLICA` and `CUSTODIAL`.

To create/change the values of the tags, please execute :

    [root] # /usr/bin/chimera writetag /path/to/directory AccessLatency "<New AccessLatency>"
    [root] # /usr/bin/chimera writetag /path/to/directory RetentionPolicy "<New RetentionPolicy>"


> **NOTE**
>
> Some clients also have default values, which are used when not explicitly specified by the user. In this case server side defaults will have no effect.

> **NOTE**
> 
> If the implicit space reservation is not enabled, pools in link groups will be excluded from consideration and only the remaining pools will be considered for storing the incoming data, and classical pool selection mechanism will be used.


SPACEMANAGER CONFIGURATION
==========================

SRM SPACEMANAGER AND LINK GROUPS
--------------------------------

`SpaceManager` is making reservations against free space available in [link groups](config-PoolManager.md#link-groups). The total free space in the given link group is the sum of available spaces in all links. The available space in each link is the sum of all sizes of available space in all pools assinged to a given link. Therefore for the space reservation to work correctly it is essential that each pool belongs to one and only one link, and each link belongs to only one link group. Link groups are assigned several parameters that determine what kind of space the link group corresponds to and who can make reservations against this space.


MAKING A SPACE RESERVATION
--------------------------

Now that the `SRM SpaceManager` is activated you can make a space reservation. As mentioned above you need link groups to make a space reservation.

### Prerequisites for Space Reservations

Login to the [admin interface](intouch.md#the-admin-interface) and `cd` to the cell `SrmSpaceManager`.

    [user] $ ssh -p 22224 -l admin admin.example.org
    (local) admin > cd SrmSpaceManager

Type `ls link groups` to get information about link groups.

    (SrmSpaceManager) admin > ls link groups

The lack of output tells you that there are no link groups. As there are no link groups, no space can be reserved.

#### The Link Groups

For a general introduction about link groups see [the section called “Link Groups”][link groups](config-PoolManager.md#link-groups).

Example:  

In this example we will create a link group for the VO desy. In order to do so we need to have a pool, a pool group and a link. Moreover, we define unit groups named any-store, world-net and any-protocol. (See [the section called “Types of Units”](#types-of-units).)

Define a pool in your layout file, add it to your pool directory and restart the `poolDomain`.

    [poolDomain]
    [poolDomain/pool]
    path=/srv/dcache/spacemanager-pool
    name=spacemanager-pool

    [root] # mkdir -p /srv/dcache/spacemanager-pool
    [root] # /usr/bin/dcache restart

In the admin interface, `cd` to the CELL-POOLMNGR and create a pool group, a link and a link group.

    (SrmSpaceManager) admin > ..
    (local) admin > cd PoolManager
    (PoolManager) admin > psu create pgroup spacemanager_poolGroup
    (PoolManager) admin > psu addto pgroup spacemanager_poolGroup spacemanager-pool
    (PoolManager) admin > psu removefrom pgroup default spacemanager-pool
    (PoolManager) admin > psu create link spacemanager_WriteLink any-store world-net any-protocol
    (PoolManager) admin > psu set link spacemanager_WriteLink -readpref=10 -writepref=10 -cachepref=0 -p2ppref=-1
    (PoolManager) admin > psu add link spacemanager_WriteLink  spacemanager_poolGroup
    (PoolManager) admin > psu create linkGroup spacemanager_WriteLinkGroup
    (PoolManager) admin > psu set linkGroup custodialAllowed spacemanager_WriteLinkGroup true
    (PoolManager) admin > psu set linkGroup replicaAllowed spacemanager_WriteLinkGroup true
    (PoolManager) admin > psu set linkGroup nearlineAllowed spacemanager_WriteLinkGroup true
    (PoolManager) admin > psu set linkGroup onlineAllowed spacemanager_WriteLinkGroup true
    (PoolManager) admin > psu addto linkGroup spacemanager_WriteLinkGroup spacemanager_WriteLink
    (PoolManager) admin > save
    (PoolManager) admin > ..
	
 	
Check whether the link group is available. Note that this can take several minutes to propagate to SPACEMNGR.

    (local) admin > cd SrmSpaceManager
    (SrmSpaceManager) admin > ls link groups
    FLAGS CNT RESVD        AVAIL         FREE             UPDATED NAME
    --rc:no 0     0 + 7278624768 = 7278624768 2011-11-28 12:12:51 spacemanager_WriteLinkGroup
    
        
The link group `spacemanager_WriteLinkGroup` was created. Here the flags indicate first the status (- indicates that neither the expired \[e\] nor the released flags \[r\] are set), followed by the type of reservations allowed in the link group (here replica \[r\], custodial \[c\], nearline \[n\] and online \[o\] files; output \[o\] files are not allowed - see `help ls
      link groups` for details on the format). No space reservations have been created, as indicated by the count field. Since no space reservation has been created, no space in the link group is reserved.

#### The `SpaceManagerLinkGroupAuthorizationFile`

Now you need to edit the **LinkGroupAuthorization.conf** file. This file contains a list of the link groups and all the VOs and the VO Roles that are permitted to make reservations in a given link group.

Specify the location of the **LinkGroupAuthorization.conf** file in the **/etc/dcache/dcache.conf** file.

    spacemanager.authz.link-group-file-name=/path/to/LinkGroupAuthorization.conf

The file **LinkGroupAuthorization.conf** has following syntax:

LinkGroup <NameOfLinkGroup> followed by the list of the Fully Qualified Attribute Names (FQANs). Each FQAN is on a separate line, followed by an empty line, which is used as a record separator, or by the end of the file.

FQAN is usually a string of the form <VO>/Role=<VORole>. Both <VO> and <VORole> can be set to *, in this case all VOs or VO Roles will be allowed to make reservations in this link group. Any line that starts with # is a comment and may appear anywhere.

Rather than an FQAN, a mapped user name can be used. This allows clients or protocols that do not provide VOMS attributes to make use of space reservations.


    #SpaceManagerLinkGroupAuthorizationFile

    LinkGroup NameOfLinkGroup
    /VO/Role=VORole

> **NOTE**
>
> You do not need to restart the DOMAIN-SRM or dCache after changing the `LinkGroupAuthorization.conf` file. The changes will be applied automatically after a few minutes.
>
> Use `update link groups` to be sure that the `LinkGroupAuthorization.conf` file and the link groups have been updated.
>
>     (SrmSpaceManager) admin > update link groups
>     Update started.

Example:  

In the example above you created the link group `spacemanager_WriteLinkGroup`. Now you want to allow members of the VO `desy` with the role `production` to make a space reservation in this link group.

    #SpaceManagerLinkGroupAuthorizationFile
    # this is comment and is ignored

    LinkGroup spacemanager_WriteLinkGroup
    #
    /desy/Role=production

Example:  

In this more general example for a `SpaceManagerLinkGroupAuthorizationFile` members of the VO `desy` with role `test` are authorized to make a space reservation in a link group called `desy-test-LinkGroup`. Moreover, all members of the VO `desy` are authorized to make a reservation in the link group called `desy-anyone-LinkGroup` and anyone is authorized to make a space reservation in the link group called `default-LinkGroup`.

    #SpaceManagerLinkGroupAuthorizationFile  
    # this is a comment and is ignored  

    LinkGroup desy-test-LinkGroup  
    /desy/Role=test  

    LinkGroup desy-anyone-LinkGroup  
    /desy/Role=*  

    LinkGroup default-LinkGroup  
    # allow anyone :-)  
    */Role=*  

### Making and Releasing a Space Reservation as dCache Administrator  

#### Making a Space Reservation  

Example:  

Now you can make a space reservation for the VO `desy`.  

   (SrmSpaceManager) admin > reserve space -owner=/desy/Role=production -desc=DESY_TEST -lifetime=10000 -lg=spacemanager_WriteLinkGroup 5MB
110000 voGroup:/desy voRole:production retentionPolicy:CUSTODIAL accessLatency:NEARLINE linkGroupId:0 size:5000000 created:Fri Dec 09 12:43:48 CET 2011 lifetime:10000000ms expiration:Fri Dec 09 15:30:28 CET 2011 description:DESY_TEST state:RESERVED used:0 allocated:0  

The space token of the reservation is `110000`.
Check the status of the reservation by

    (SrmSpaceManager) admin > ls spaces -e -h  
 TOKEN RETENTION LATENCY FILES ALLO   USED   FREE   SIZE             EXPIRES DESCRIPTION  
110000 CUSTODIAL NEARLINE    0   0B +   0B + 5.0M = 5.0M 2011-12-09 12:43:48 DESY_TEST  

(SrmSpaceManager) admin > ls link groups -h  
FLAGS CNT RESVD   AVAIL   FREE             UPDATED NAME  
--rc:no 1  5.0M +  7.3G = 7.3G 2011-11-28 12:12:51 spacemanager_WriteLinkGroup  

Here the `-h` option indicates that approximate, but human readable, byte sizes are to be used, and `-e` indicates that ephemeral (time limited) reservations should be displayed too (by default time limited reservations are not displayed as they are often implicit reservations). As can be seen, 5 MB are now reserved in the link group, although with approximate byte sizes, 5 MB do not make a visible difference in the 7.3 GB total size.
You can now copy a file into that space token.

   [user] $ srmcp file:////bin/sh srm://<dcache.example.org>:8443/data/world-writable/space-token-test-file -space_token=110000

Now you can check via the [Webadmin Interface](config-webadmin.md) or the [Web Interface](intouch.md#the-web-interface-for-monitoring-dcache) that the file has been copied to the pool `spacemanager-pool`.

There are several parameters to be specified for a space reservation.

    (SrmSpaceManager) admin > reserve space [-al=online|nearline] [-desc=<string>] -lg=<name>  
    [-lifetime=<seconds>] [-owner=<user>|<fqan>] [-rp=output|replica|custodial] <size>  

[-owner=<user>|<fqan>]  
The owner of the space is identified by either mapped user name or FQAN. The owner must be authorized to reserve space in the link group in which the space is to be created. Besides the dCache admin, only the owner can release the space. Anybody can however write into the space (although the link group may only allow certain storage groups and thus restrict which file system paths can be written to space reservation, which in turn limits who can upload files to it).

[-al=<AccessLatency>]  
`AccessLatency` needs to match one of the access latencies allowed for the link group.

[-rp=<RetentionPolicy>]  
`RetentionPolicy` needs to match one of the retention policies allowed for the link group.

[-desc=<Description>]   
You can chose a value to describe your space reservation.

-lg=<LinkGroupName>  
Which link group to create the reservation in.

<size>  
The size of the space reservation should be specified in bytes, optionally using a byte unit suffix using either SI or IEEE prefixes.

[-lifetime=<lifetime]>  
The life time of the space reservation should be specified in seconds. If no life time is specified, the space reservation will not expire automatically.

#### Releasing a Space Reservation

If a space reservation is not needed anymore it can be released with

    (SrmSpaceManager) admin > release space <spaceTokenId>

Example:    

       (SrmSpaceManager) admin > reserve space -owner=/desy -desc=DESY_TEST -lifetime=600 5000000
        110042 voGroup:/desy voRole:production retentionPolicy:CUSTODIAL accessLatency:NEARLINE linkGroupId:0 size:5000000    
	created:Thu Dec 15 12:00:35 CET 2011 lifetime:600000ms expiration:Thu Dec 15 12:10:35 CET 2011 description:DESY_TEST   
	state:RESERVED used:0 allocated:0
       (SrmSpaceManager) admin > release space 110042
        110042 voGroup:/desy voRole:production retentionPolicy:CUSTODIAL accessLatency:NEARLINE linkGroupId:0 size:5000000 
	created:Thu Dec 15 12:00:35 CET 2011 lifetime:600000ms expiration:Thu Dec 15 12:10:35 CET 2011 description:DESY_TEST   
	state:RELEASED used:0 allocated:0

You can see that the value for `state` has changed from `RESERVED` to `RELEASED`.

### Making and Releasing a Space Reservation as a User

If so authorized, a user can make a space reservation through the SRM protocol. A user is authorized to do so using the **LinkGroupAuthorization.conf** file.

#### VO based Authorization Prerequisites

In order to be able to take advantage of the virtual organization (VO) infrastructure and VO based authorization and VO based access control to the space in dCache, certain things need to be in place:

-   User needs to be registered with the VO.

-   User needs to use [`voms-proxy-init`](config-gplazma.md#creating-a-voms-proxy) to create a VO proxy.

-   dCache needs to use gPlazma with modules that extract VO attributes from the user’s proxy. (See [Chapter 10, Authorization in dCache](config-gplazma.md), have a look at `voms` plugin and see the [section called “Authentication and Authorization in dCache”](config-gplazma.md#authentication-and-authorization-in-dcache) for an example with voms.

Only if these 3 conditions are satisfied the VO based authorization of the SpaceManager will work.

#### VO based Access Control Configuration

As mentioned [above](#spacemanager-configuration) dCache space reservation functionality access control is currently performed at the level of the link groups. Access to making reservations in each link group is controlled by the [`SpaceManagerLinkGroupAuthorizationFile`](#the-spacemanagerlinkgroupauthorizationfile).

This file contains a list of the link groups and all the VOs and the VO Roles that are permitted to make reservations in a given link group.

When a `SRM` Space Reservation request is executed, its parameters, such as reservation size, lifetime, access latency and retention policy as well as user's VO membership information is forwarded to the `SRM SpaceManager.

Once a space reservation is created, no access control is performed, any user can store the files in this space reservation, provided he or she knows the exact space token.

#### Making and Releasing a Space Reservation

A user who is given the rights in the `SpaceManagerLinkGroupAuthorizationFile` can make a space reservation by

    [user] $ srm-reserve-space -retention_policy=<RetentionPolicy> -lifetime=<lifetimeInSecs> -desired_size=<sizeInBytes> -guaranteed_size=<sizeInBytes>  srm://<example.org>:8443  
Space token =SpaceTokenId  

and release it by

    [user] $ srm-release-space srm://<example.org>:8443 -space_token=SpaceTokenId

> **NOTE**
>
> Please note that it is obligatory to specify the retention policy while it is optional to specify the access latency.

Example:
    [user] $ srm-reserve-space -retention_policy=REPLICA -lifetime=300 -desired_size=5500000 -guaranteed_size=5500000  srm://srm.example.org:8443  
Space token =110044  
  
The space reservation can be released by:

    [user] $ srm-release-space srm://srm.example.org:8443 -space_token=110044



#### Space Reservation without VOMS certificate

If a client uses a regular grid proxy, created with `grid-proxy-init`, and not a VO proxy, which is created with the `voms-proxy-init`, when it is communicating with `SRM` server in dCache, then the VO attributes can not be extracted from its credential. In this case the name of the user is extracted from the Distinguished Name (DN) to use name mapping. For the purposes of the space reservation the name of the user as mapped by `gplazma` is used as its VO Group name, and the VO Role is left empty. The entry in the `SpaceManagerLinkGroupAuthorizationFile` should be:

    #LinkGroupAuthorizationFile
    #
    <userName>  

#### Space Reservation for non SRM Transfers

Edit the file **/etc/dcache/dcache.conf** to enable space reservation for non `SRM` transfers.

    spacemanager.enable.reserve-space-for-non-srm-transfers=true

If the `spacemanager` is enabled, `spacemanager.enable.reserve-space-for-non-srm-transfers` is set to true, and if the transfer request comes from a door, and there was no prior space reservation made for this file, the `SpaceManager` will try to reserve space before satisfying the request.

Possible values are `true` or `false` and the default value is false.

This is analogous to implicit space reservations performed by the srm, except that these reservations are created by the `spacemanager` itself. Since an `SRM` client uses a non-`SRM` protocol for the actual upload, setting the above option to true while disabling implicit space reservations in the `srm`, will still allow files to be uploaded to a link group even when no space token is provided. Such a configuration should however be avoided: If the srm does not create the reservation itself, it has no way of communicating access latency, retention policy, file size, nor lifetime to `spacemanager`.

SRM CONFIGURATION FOR EXPERTS
------------------------------

There are a few parameters in **/usr/share/dcache/defaults/*.properties** that you might find useful for nontrivial `SRM` deployment.

### dcache.enable.space-reservation  

`dcache.enable.space-reservation` tells if the space management is activated in `SRM`.

Possible values are `true` and `false`. Default is `true`.

Usage example:

    dcache.enable.space-reservation=true

### srm.enable.space-reservation.implicit

`srm.enable.space-reservation.implicit` tells if the space should be reserved for SRM Version 1 transfers and for SRM Version 2 transfers that have no space token specified.

Possible values are `true` and `false`. This is enabled by default. It has no effect if `dcache.enable.space-reservation` is set to `true`.

Usage example:  


    srm.enable.space-reservation.implicit=true

### dcache.enable.overwrite

`dcache.enable.overwrite` tells SRM and GRIDFTP servers if the overwrite is allowed. If enabled on the SRM node, should be enabled on all GRIDFTP nodes.

Possible values are `true` and `false`. Default is `false`.

Usage example:

    dcache.enable.overwrite=true

### srm.enable.overwrite-by-default

`srm.enable.overwrite-by-default` Set this to `true` if you want overwrite to be enabled for SRM v1.1 interface as well as for SRM v2.2 interface when client does not specify desired overwrite mode. This option will be considered only if `dcache.enable.overwrite` is set to `true`.

Possible values are `true` and `false`. Default is `false`.

Usage example:  


    srm.enable.overwrite-by-default=false 

### srm.db.host

`srm.db.host` tells `SRM` which database host to connect to.

Default value is `localhost`.

Usage example:  


    srm.db.host=database-host.example.org

### spaceManagerDatabaseHost

`spaceManagerDatabaseHost` tells SpaceManager which database host to connect to.

Default value is `localhost`.

Usage example:  

    spaceManagerDatabaseHost=database-host.example.org

### pinmanager.db.host

`pinmanager.db.host` tells PinManager which database host to connect to.

Default value is `localhost`.

Usage example:  


    pinmanager.db.host=database-host.example.org

### srm.db.name

`srm.db.name` tells `SRM` which database to connect to.

Default value is `srm`.

Usage example:  


    srm.db.name=srm

### srm.db.user

`srm.db.user` tells `SRM` which database user name to use when connecting to database. Do not change unless you know what you are doing.

Default value is `dcache`.

Usage example:  


    srm.db.user=dcache

### srm.db.password

`srm.db.password` tells SRM which database password to use when connecting to database. The default value is an `empty` value (no password).

Usage example:  


    srm.db.password=NotVerySecret

### srm.db.password.file

`srm.db.password.file` tells `SRM` which database password file to use when connecting to database. Do not change unless you know what you are doing. It is recommended that MD5 authentication method is used. To learn about file format please see http://www.postgresql.org/docs/8.1/static/libpq-pgpass.html. To learn more about authentication methods please visit http://www.postgresql.org/docs/8.1/static/encryption-options.html, Please read "Encrypting Passwords Across A Network" section.

This option is not set by default.

Usage example:  


    srm.db.password.file=/root/.pgpass

### srm.request.enable.history-database

`srm.request.enable.history-database` enables logging of the transition history of the `SRM` request in the database. The request transitions can be examined through the command line interface. Activation of this option might lead to the increase of the database activity, so if the PSQL load generated by `SRM` is excessive, disable it.

Possible values are `true` and `false`. Default is `false`.

Usage example:  

    srm.request.enable.history-database=true

### transfermanagers.enable.log-to-database

`transfermanagers.enable.log-to-database` tells `SRM` to store the information about the remote (copy, srmCopy) transfer details in the database. Activation of this option might lead to the increase of the database activity, so if the PSQL load generated by SRM is excessive, disable it.

Possible values are `true` and `false`. Default is `false`.

Usage example:  

    transfermanagers.enable.log-to-database=false

### srmVersion

`srmVersion` is not used by `SRM` it was mentioned that this value is used by some publishing scripts.

Default is `version1`.

### srm.root

`srm.root` tells `SRM` what the root of all `SRM` paths is in pnfs. `SRM` will prepend path to all the local SURL paths passed to it by `SRM` client. So if the `srm.root` is set to `/pnfs/fnal.gov/THISISTHEPNFSSRMPATH` and someone requests the read of <srm://srm.example.org:8443/file1>, `SRM` will translate the SURL path `/file1` into `/pnfs/fnal.gov/THISISTHEPNFSSRMPATH/file1`. Setting this variable to something different from `/` is equivalent of performing Unix `chroot` for all `SRM` operations.

Default value is `/`.

Usage example:  

    srm.root="/pnfs/fnal.gov/data/experiment"

### srm.limits.parallel-streams

`srm.limits.parallel-streams` specifies the number of the parallel streams that `SRM` will use when performing third party transfers between this system and remote GSIFTP servers, in response to `SRM` v1.1 copy or SRM V2.2 srmCopy function. This will have no effect on srmPrepareToPut and srmPrepareToGet command results and parameters of GRIDFTP transfers driven by the `SRM` clients.

Default value is `10`.

Usage example:  

    srm.limits.parallel-streams=20

### srm.limits.transfer-buffer.size

`srm.limits.transfer-buffer.size` specifies the number of bytes to use for the in memory buffers for performing third party transfers between this system and remote GSIFTP servers, in response to SRM v1.1 copy or SRM V2.2 srmCopy function. This will have no effect on srmPrepareToPut and srmPrepareToGet command results and parameters of GRIDFTP transfers driven by the `SRM` clients.

Default value is `1048576`.

Usage example:  

    srm.limits.transfer-buffer.size=1048576

### srm.limits.transfer-tcp-buffer.size

`srm.limits.transfer-tcp-buffer.size` specifies the number of bytes to use for the tcp buffers for performing third party transfers between this system and remote GSIFTP servers, in response to `SRM` v1.1 copy or `SRM` V2.2 srmCopy function. This will have no effect on srmPrepareToPut and srmPrepareToGet command results and parameters of GRIDFTP transfers driven by the `SRM` clients.

Default value is `1048576`.

Usage example:  

    srm.limits.transfer-tcp-buffer.size=1048576

### srm.service.gplazma.cache.timeout

`srm.service.gplazma.cache.timeout` specifies the duration that authorizations will be cached. Caching decreases the volume of messages to the `gplazma` cell or other authorization mechanism. To turn off caching, set the value to `0`.

Default value is `120`.

Usage example:  

    srm.service.gplazma.cache.timeout=60

### srm.limits.request.bring-online.lifetime, srm.limits.request.put.lifetime and srm.limits.request.copy.lifetime

`srm.limits.request.bring-online.lifetime`, `srm.limits.request.put.lifetime` and `srm.limits.request.copy.lifetime` specify the lifetimes of the srmPrepareToGet (srmBringOnline) srmPrepareToPut and srmCopy requests lifetimes in millisecond. If the system is unable to fulfill the requests before the request lifetimes expire, the requests are automatically garbage collected.

Default value is `14400000` (4 hours)

Usage example:  

    srm.limits.request.bring-online.lifetime=14400000
    srm.limits.request.put.lifetime=14400000
    srm.limits.request.copy.lifetime=14400000

### srm.limits.request.scheduler.ready.max, srm.limits.request.put.scheduler.ready.max, srm.limits.request.scheduler.ready-queue.size and srm.limits.request.put.scheduler.ready-queue.size

`srm.limits.request.scheduler.ready.max` and `srm.limits.request.put.scheduler.ready.max` specify the maximum number of the files for which the transfer URLs will be computed and given to the users in response to SRM get (srmPrepareToGet) and put (srmPrepareToPut) requests. The rest of the files that are ready to be transfered are put on the `Ready` queues, the maximum length of these queues are controlled by `srm.limits.request.scheduler.ready-queue.size` and `srm.limits.request.put.scheduler.ready-queue.size` parameters. These parameters should be set according to the capacity of the system, and are usually greater than the maximum number of the GRIDFTP transfers that this dCache instance GRIDFTP doors can sustain.

Usage example:  

    srm.limits.request.scheduler.ready-queue.size=10000
    srm.limits.request.scheduler.ready.max=2000
    srm.limits.request.put.scheduler.ready-queue.size=10000
    srm.limits.request.put.scheduler.ready.max=1000

### srm.limits.request.copy.scheduler.thread.pool.size and transfermanagers.limits.external-transfers

`srm.limits.request.copy.scheduler.thread.pool.size` and `transfermanagers.limits.external-transfers`. `srm.limits.request.copy.scheduler.thread.pool.size` is used to specify how many parallel srmCopy file copies to execute simultaneously. Once the `SRM` contacted the remote `SRM` system, and obtained a Transfer URL (usually GSI-FTP URL), it contacts a Copy Manager module (usually RemoteGSIFTPTransferManager), and asks it to perform a GRIDFTP transfer between the remote GRIDFTP server and a dCache pool. The maximum number of simultaneous transfers that RemoteGSIFTPTransferManager will support is `transfermanagers.limits.external-transfers`, therefore it is important that `transfermanagers.limits.external-transfers` is greater than or equal to `srm.limits.request.copy.scheduler.thread.pool.size`.

Usage example:  

    srm.limits.request.copy.scheduler.thread.pool.size=250
    transfermanagers.limits.external-transfers=260

### srm.enable.custom-get-host-by-address

`srm.enable.custom-get-host-by-address` `srm.enable.custom-get-host-by-address` enables using the BNL developed procedure for host by IP resolution if standard InetAddress method failed.

Usage example:  

    srm.enable.custom-get-host-by-address=true

### srm.enable.recursive-directory-creation

`srm.enable.recursive-directory-creation` allows or disallows automatic creation of directories via SRM. Set this to `true` or `false`.

Automatic directory creation is allowed by default.

Usage example:  

    srm.enable.recursive-directory-creation=true

### hostCertificateRefreshPeriod

This option allows you to control how often the SRM door will reload the server's host certificate from the filesystem. For the specified period, the host certificate will be kept in memory. This speeds up the rate at which the door can handle requests, but also causes it to be unaware of changes to the host certificate (for instance in the case of renewal).

By changing this parameter you can control how long the host certificate is cached by the door and consequently how fast the door will be able to detect and reload a renewed host certificate.

Please note that the value of this parameter has to be specified in seconds.

Usage example:  

    hostCertificateRefreshPeriod=86400

### trustAnchorRefreshPeriod

The `trustAnchorRefreshPeriod` option is similar to `hostCertificateRefreshPeriod`. It applies to the set of CA certificates trusted by the SRM door for signing end-entity certificates (along with some metadata, these form so called trust anchors). The trust anchors are needed to make a decision about the trustworthiness of a certificate in X.509 client authentication. The GSI security protocol used by SRM builds upon X.509 client authentication.

By changing this parameter you can control how long the set of trust anchors remains cached by the door. Conversely, it also influences how often the door reloads the set of trusted certificates.

Please note that the value of this parameter has to be specified in seconds.

> **TIP**
>
> Trust-anchors usually change more often than the host certificate. Thus, it might be sensible to set the refresh period of the trust anchors lower than the refresh period of the host certificate.

Usage example:  

    trustAnchorRefreshPeriod=3600

CONFIGURING THE POSTGRESQL DATABASE
===================================

We highly recommend to make sure that PSQL database files are stored on a separate disk that is not used for anything else (not even PSQL logging). BNL Atlas Tier 1 observed a great improvement in srm-database communication performance after they deployed PSQL on a separate dedicated machine.

SRM OR SRM MONITORING ON A SEPARATE NODE
----------------------------------------

If `SRM` or srm monitoring is going to be installed on a separate node, you need to add an entry in the file **/var/lib/pgsql/data/pg_hba.conf** for this node as well:

    host    all         all       <monitoring node>    trust
    host    all         all       <srm node>           trust

The file **postgresql.conf** should contain the following:

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

GENERAL SRM CONCEPTS (FOR DEVELOPERS)
=====================================

THE SRM SERVICE
---------------

dCache `SRM` is implemented as a Web Service running in a Jetty servlet container and an Axis Web Services engine. The Jetty server is executed as a cell, embedded in dCache and started automatically by the `SRM` service. Other cells started automatically by `SRM` are `SpaceManager`, `PinManager` and `RemoteGSIFTPTransferManager`. Of these services only `SRM` and SpaceManager require special configuration.

The `SRM` consists of the five categories of functions:

-   [Space Management Functions](#space-management-functions)
-   [Data Transfer Functions](#data-transfer-functions)
-   [Request Status Functions](#request-status-functions)
-   [Directory Functions](#directory-functions)
-   [Permission Functions](#permission-functions)

SPACE MANAGEMENT FUNCTIONS
--------------------------

`SRM` version 2.2 introduces a concept of space reservation. Space reservation guarantees that the requested amount of storage space of a specified type is made available by the storage system for a specified amount of time.

We use three functions for space management:

-   srmReserveSpace
-   SrmGetSpaceMetadata
-   srmReleaseSpace

Space reservation is made using the `srmReserveSpace` function. In case of successful reservation, a unique name, called space token is assigned to the reservation. A space token can be used during the transfer operations to tell the system to put the files being manipulated or transferred into an associated space reservation. A storage system ensures that the reserved amount of the disk space is indeed available, thus providing a guarantee that a client does not run out of space until all space promised by the reservation has been used. When files are deleted, the space is returned to the space reservation.

dCache only manages write space, i.e. space on disk can be reserved only for write operations. Once files are migrated to tape, and if no copy is required on disk, space used by these files is returned back into space reservation. When files are read back from tape and cached on disk, they are not counted as part of any space. SRM space reservation can be assigned a non-unique description that can be used to query the system for space reservations with a given description.

[Properties of the SRM space reservations](#properties-of-the-srm-space-reservations) can be discovered using the `SrmGetSpaceMetadata` function.

Space Reservations might be released with the function `srmReleaseSpace`.

For a complete description of the available space management functions please see the [SRM Version 2.2 Specification](https://sdm.lbl.gov/srm-wg/doc/SRM.v2.2.html#_Toc241633085).

DATA TRANSFER FUNCTIONS
-----------------------

### SURLs and TURLs

`SRM` defines a protocol named `SRM`, and introduces a way to address the files stored in the `SRM` managed storage by site URL (SURL of the format `srm://<host>:<port>/[<web service
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

-   srmPrepareToGet
-   srmPrepareToPut
-   srmCopy

(in `SRM` version 1.1 these functions were called `get`, `put` and `copy`).

All three functions accept lists of SURLs as parameters. All data transfer functions perform file/directory access verification and `srmPrepareToPut` and `srmCopy` check if the receiving storage element has sufficient space to store the files.

`srmPrepareToGet` prepares files for read. These files are specified as a list of source SURLs, which are stored in an SRM managed storage element. `srmPrepareToGet` is used to bring source files online and assigns transfer URLs (TURLs) that are used for actual data transfer.

`srmPrepareToPut` prepares an SRM managed storage element to receive data into the list of destination SURLs. It prepares a list of TURLs where the client can write data into.

Both functions support transfer protocol negotiation. A client supplies a list of transfer protocols and the SRM server computes the TURL using the first protocol from the list that it supports. Function invocation on the Storage Element depends on implementation and may range from simple SURL to TURL translation to stage from tape to disk cache and dynamic selection of transfer host and transfer protocol depending on the protocol availability and current load on each of the transfer server load.

The function `srmCopy` is used to copy files between `SRM` managed storage elements. If both source and target are local to the `SRM`, it performes a local copy. There are two modes of remote copies:

-   PULL mode : The target `SRM` initiates an `srmCopy` request. Upon the client\\u0411\\u2500\\u2265s `srmCopy` request, the target `SRM` makes a space at the target storage, executes `srmPrepareToGet` on the source `SRM`. When the TURL is ready at the source `SRM`, the target `SRM` transfers the file from the source TURL into the prepared target storage. After the file transfer completes, `srmReleaseFiles` is issued to the source `SRM`.

-   PUSH mode : The source `SRM` initiates an `srmCopy` request. Upon the client\\u0411\\u2500\\u2265s `srmCopy` request, the source `SRM` prepares a file to be transferred out to the target `SRM`, executes `srmPrepareToPut` on the target `SRM`. When the TURL is ready at the target `SRM`, the source SRM transfers the file from the prepared source into the prepared target TURL. After the file transfer completes, `srmPutDone` is issued to the target `SRM`.

When a specified target space token is provided, the files will be located in the space associated with the space token.

`SRM` Version 2.2 `srmPrepareToPut` and `srmCopy` PULL mode transfers allow the user to specify a space reservation token or a retention policy and access latency. Any of these parameters are optional, and it is up to the implementation to decide what to do, if these properties are not specified. The specification requires that if a space reservation is given, then the specified access latency or retention policy must match those of the space reservation.

The Data Transfer Functions are asynchronous, an initial `SRM` call starts a request execution on the server side and returns a request status that contains a unique request token. The status of request is polled periodically by `SRM` get request status functions. Once a request is completed and the client receives the TURLs the data transfers are initiated. When the transfers are completed the client notifies the `SRM` server by executing `srmReleaseFiles` in case of `srmPrepareToGet` or `srmPutDone` in case of `srmPrepareToPut`. In case of `srmCopy`, the system knows when the transfers are completed and resources can be released, so it requires no special function at the end.

Clients are free to cancel the requests at any time by execution of `srmAbortFiles` or `srmAbortRequest`.

REQUEST STATUS FUNCTIONS
------------------------ 

The functions for checking the request status are:

-   srmStatusOfReserveSpaceRequest
-   srmStatusOfUpdateSpaceRequest
-   srmStatusOfChangeSpaceForFilesRequest
-   srmStatusOfChangeSpaceForFilesRequest
-   srmStatusOfBringOnlineRequest
-   srmStatusOfPutRequest
-   srmStatusOfCopyRequest

DIRECTORY FUNCTIONS
-------------------

`SRM` Version 2.2, interface provides a complete set of directory management functions. These are

-   srmLs
    ,
    srmRm
-   srmMkDir
    ,
    srmRmDir
-   srmMv

PERMISSION FUNCTIONS
--------------------

SRM Version 2.2 supports the following three file permission functions:

-   srmGetPermission
-   srmCheckPermission
    and
-   srmSetPermission

dCache contains an implementation of these functions that allows setting and checking of Unix file permissions.

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
