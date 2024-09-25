CHAPTER 12. dCache SPACE MANAGER
===========================================

The `spacemanager` is a component in dCache that manages space reservations, a concept that was
first introduced in `SRM` version 2.2. Space reservation guarantees that the requested amount of
storage space of a specified type is made available by the storage system for a specified amount of
time.

-----
[TOC bullet hierarchy]
-----

## Utilization of space reservations for data storage

Users can create space reservations using an appropriate `SRM` client, although it is more common
for the dCache administrator to make space reservations for VOs (see the
section [“SpaceManager configuration”](#spacemanager-configuration). Each space reservation has an
associated ID (or space token). VOs then can copy directly into space tokens assigned to them by the
dCache administrator.

When a file is about to be transferred to a storage system, it is checked whether the space
available in the space reservation can accomodate the entire file. If yes, this chunk of space is
marked as allocated, so that it can not be taken by another, concurrently transferred file. If the
file is transferred successfully, the allocated space becomes used space within the space
reservation, else the allocated space is released back to the space reservation as free space.

`SRM` space reservation can be assigned a non-unique description which can be used to query the
system for space reservations with a given description.

dCache only manages write space, i.e. space on disk can be reserved only for write operations. Once
files are migrated to tape, and if no copy is required on disk, space used by these files is
returned back into space reservation. When files are read back from tape and cached on disk, they
are not counted as part of any space.

## Properties of space reservation

A space reservation has a retention policy and an access latency.

Retention policy describes the quality of the storage service that will be provided for the data (
files) stored in the space reservation, and access latency describes the availability of this data.
The `SRM` specification requires that if a space reservation is given on upload, then the specified
retention policy and access latency must match those of the space reservation.

The default values for the retention policy and access latency can be
changed in the file `/etc/dcache/dcache.conf`.

**Retention policy**
The values of retention policy supported by dCache are `REPLICA` and `CUSTODIAL`.

- `REPLICA` corresponds to the lowest quality of the service, usually associated with storing a
  single copy of each file on disk.

- `CUSTODIAL` is the highest quality service, usually interpreted as storage of the data on tape.

Once a file is written into a given space reservation, it inherits the reservation's retention
policy.

If the space reservation request does not specify a retention policy, we will assign a value given
by `dcache.default-retention-policy`. The default value is `CUSTODIAL`.

Edit the file `/etc/dcache/dcache.conf` to change the default value.

Example:
Change the default value to `REPLICA`.

```ini
dcache.default-retention-policy=REPLICA
```

**Access latency**
The two values allowed for access latency are `NEARLINE` and `ONLINE`.

- `NEARLINE` means that data stored in this reservation is allowed to migrate to permanent media.
  Retrieving this data may result in delays associated with preparatory steps that the storage
  system has to perform to make this data available for user I/O (e.g., staging data from tape to a
  disk cache).

- `ONLINE` means that data is readily available allowing for faster access.

In case of dCache, `ONLINE` means that there will always be a copy of the file on disk,
while `NEARLINE` does not provide such guarantee. As with retention policy, once a file is written
into a given space reservation, it inherits the reservation's access latency.

If a space reservation request does not specify an access latency, we will assign a value given
by `dcache.default-access-latency`. The default value is `NEARLINE`.

Edit the file `/etc/dcache/dcache.conf` to change the default value.

Example:
Change the default value to `ONLINE`.

```ini
dcache.default-access-latency=ONLINE
```

## Activating Spacemanager

In order to enable the `SRM SpaceManager` you need to add the `spacemanager` service to your layout
file

```ini
[dCacheDomain]
[dCacheDomain/spacemanager]
```

Unless you have reason not to, we recommend placing the `spacemanager` service in the same domain as
the `poolmanager` service.

## Explicit and implicit space reservations for data storage in dCache

### Explicit Space Reservations

Each SRM space reservation is made against the total available disk space of a particular link
group. If dCache is configured correctly, each byte of disk space, that can be reserved, belongs to
one and only one link group. See the
section [“SpaceManager configuration”](#spacemanager-configuration) for a detailed description.

> **IMPORTANT**
>
> Make sure that no pool belongs to more than one pool group, no pool group belongs to more than one
> link and no link belongs to more than one link group.

If a space reservation is specified during upload, the file will be stored in it.

Files written into a space made within a particular link group will end up on one of the pools
belonging to this link group. The difference between the link group's free space and the sum of all
its space reservation's unused space is the available space of the link group. The available space
of a link group is the space that can be allocated for new space reservations.

The total space in dCache that can be reserved is the sum of the available spaces of all link
groups. Note however that a space reservation can never span more than a single link group.

### Implicit Space Reservations

dCache can perform implicit space reservations for non-`SRM` transfers, `SRM` Version 1 transfers
and for `SRM` Version 2.2 data transfers that are not given the space token explicitly. The
parameter that enables this behavior is srm.enable.space-reservation.implicit, which is described in
the section [“SRM configuration for experts”](#srm-configuration-for-experts). If no implicit space
reservation can be made, the transfer will fail.

Implicit space reservation means that the `srm` will create a space reservation for a single upload
while negotiating the transfer parameters with the client. The space reservation will be created in
a link group for which the user is authorized to create space reservations, which has enough
available space, and which is able to hold the type of file being uploaded. The space reservation
will be short lived. Once it expires, it will be released and the file it held will live on outside
any space reservation, but still within the link group to which it was uploaded. Implicit space
reservations are thus a technical means to upload files to link groups without using explicit space
reservations.

The reason dCache cannot just allow the file to be uploaded to the link group without any space
reservation at all is, that we have to guarantee, that space already allocated for other
reservations isn’t used by the file being uploaded. The best way to guarantee that there is enough
space for the file is to make a space reservation to which to upload it.

In case of `SRM` version 1.1 data transfers, where the access latency
and retention policy cannot be specified, and in case of `SRM` V2.2
clients, when the access latency and retention policy are not
specified, default values will be used. First `SRM` will attempt to
use the values of access latency and retention policy tags from the
directory to which a file is being written. If the tags are not
present, then the access latency and retention policy will be set on
basis of `pnfsmanager` defaults controlled by
`pnfsmanager.default-retention-policy` and
`pnfsmanager.default-access-latency` variables in
`/etc/dcache/dcache.conf`.

You can check if the `AccessLatency` and `RetentionPolicy` tags are
present by using the following command:

```console-root
chimera lstag /path/to/directory
|Total: numberOfTags
|tag1
|tag2
|..
|AccessLatency
|RetentionPolicy
```

If the output contains the lines AccessLatency and RetentionPolicy
then the tags are already present and you can get the actual values of
these tags by executing the following commands, which are shown
together with example outputs:

```console-root
chimera readtag /data/experiment-a AccessLatency
|ONLINE
chimera readtag /data/experiment-a RetentionPolicy
|CUSTODIAL
```

The valid `AccessLatency` values are `ONLINE` and `NEARLINE`, valid
`RetentionPolicy` values are `REPLICA` and `CUSTODIAL`.

To create/change the values of the tags, please execute :

```console-root
chimera writetag /path/to/directory AccessLatency "<New AccessLatency>"
chimera writetag /path/to/directory RetentionPolicy "<New RetentionPolicy>"
```

> **NOTE**
>
> Some clients also have default values, which are used when not explicitly specified by the user.
> In this case server side defaults will have no effect.

> **NOTE**
>
> If the implicit space reservation is not enabled, pools in link groups will be excluded from
> consideration and only the remaining pools will be considered for storing the incoming data, and
> classical pool selection mechanism will be used.

## Spacemanager configuration

### SRM Spacemanager and Link Groups

`SpaceManager` is making reservations against free space available
in [link groups](config-PoolManager.md#link-groups). The total free space in the given link group is
the sum of available spaces in all links. The available space in each link is the sum of all sizes
of available space in all pools assinged to a given link. Therefore for the space reservation to
work correctly it is essential that each pool belongs to one and only one link, and each link
belongs to only one link group. Link groups are assigned several parameters that determine what kind
of space the link group corresponds to and who can make reservations against this space.

### Making a Space Reservation

Now that the `SRM SpaceManager` is activated you can make a space reservation. As mentioned above
you need link groups to make a space reservation.

#### Prerequisites for Space Reservations

Login to the [admin interface](intouch.md#the-admin-interface) and connect to the
cell `SrmSpaceManager`.

```console-user
ssh -p 22224 -l admin admin.example.org
|(local) admin > \c SrmSpaceManager
```

Type `ls link groups` to get information about link groups.

    (SrmSpaceManager) admin > ls link groups

The lack of output tells you that there are no link groups. As there are no link groups, no space
can be reserved.

##### The Link Groups

For a general introduction about link groups see the section
called [“Link Groups”](config-PoolManager.md#link-groups).

Example:

In this example we will create a link group for the VO desy. In order to do so we need to have a
pool, a pool group and a link. Moreover, we define unit groups named any-store, world-net and
any-protocol. (See the section called [“Types of Units”](#types-of-units).)

Define a pool in your layout file, add it to your pool directory and restart the `poolDomain`.

```ini
[poolDomain]
[poolDomain/pool]
path=/srv/dcache/spacemanager-pool
name=spacemanager-pool
```

```console-root
mkdir -p /srv/dcache/spacemanager-pool
dcache restart
```

In the admin interface, `\c` to the CELL-POOLMNGR and create a pool group, a link and a link group.

    (local) admin > \c PoolManager
    (PoolManager) admin > psu create pgroup spacemanager_poolGroup
    (PoolManager) admin > psu addto pgroup spacemanager_poolGroup spacemanager-pool
    (PoolManager) admin > psu removefrom pgroup default spacemanager-pool
    (PoolManager) admin > psu create link spacemanager_WriteLink any-store world-net any-protocol
    (PoolManager) admin > psu set link spacemanager_WriteLink -readpref=10 -writepref=10 -cachepref=0 -p2ppref=-1
    (PoolManager) admin > psu addto link spacemanager_WriteLink  spacemanager_poolGroup
    (PoolManager) admin > psu create linkGroup spacemanager_WriteLinkGroup
    (PoolManager) admin > psu set linkGroup custodialAllowed spacemanager_WriteLinkGroup true
    (PoolManager) admin > psu set linkGroup replicaAllowed spacemanager_WriteLinkGroup true
    (PoolManager) admin > psu set linkGroup nearlineAllowed spacemanager_WriteLinkGroup true
    (PoolManager) admin > psu set linkGroup onlineAllowed spacemanager_WriteLinkGroup true
    (PoolManager) admin > psu addto linkGroup spacemanager_WriteLinkGroup spacemanager_WriteLink
    (PoolManager) admin > save

Check whether the link group is available. Note that this can take several minutes to propagate to
SPACEMNGR.

    (local) admin > \c SrmSpaceManager
    (SrmSpaceManager) admin > ls link groups
    FLAGS CNT RESVD        AVAIL         FREE             UPDATED NAME
    --rc:no 0     0 + 7278624768 = 7278624768 ##LASTMONTH_YEAR##-##LASTMONTH_2MONTH##-##LASTMONTH_2DAY_OF_MONTH## 12:12:51 spacemanager_WriteLinkGroup

The link group `spacemanager_WriteLinkGroup` was created. Here the flags indicate first the
status (- indicates that neither the expired \[e\] nor the released flags \[r\] are set), followed
by the type of reservations allowed in the link group (here replica \[r\], custodial \[c\], nearline
\[n\] and online \[o\] files; output \[o\] files are not allowed - see `help ls
link groups` for details on the format). No space reservations have been created, as indicated by
the count field. Since no space reservation has been created, no space in the link group is
reserved.

##### The `SpaceManagerLinkGroupAuthorizationFile`

Now you need to edit the `LinkGroupAuthorization.conf` file. This file
contains a list of the link groups and all the VOs and the VO Roles
that are permitted to make reservations in a given link group.

Specify the location of the `LinkGroupAuthorization.conf` file in the
`/etc/dcache/dcache.conf` file.

```ini
spacemanager.authz.link-group-file-name=/path/to/LinkGroupAuthorization.conf
```

The file `LinkGroupAuthorization.conf` has following syntax:

LinkGroup <NameOfLinkGroup> followed by the list of the Fully Qualified Attribute Names (FQANs).
Each FQAN is on a separate line, followed by an empty line, which is used as a record separator, or
by the end of the file.

FQAN is usually a string of the form <VO>/Role=<VORole>. Both <VO> and <VORole> can be set to *, in
this case all VOs or VO Roles will be allowed to make reservations in this link group. Any line that
starts with # is a comment and may appear anywhere.

Rather than an FQAN, a mapped user name can be used. This allows clients or protocols that do not
provide VOMS attributes to make use of space reservations.

    #SpaceManagerLinkGroupAuthorizationFile

    LinkGroup NameOfLinkGroup
    /VO/Role=VORole

> **NOTE**
>
> You do not need to restart the DOMAIN-SRM or dCache after changing
> the `LinkGroupAuthorization.conf` file. The changes will be applied automatically after a few
> minutes.
>
> Use `update link groups` to be sure that the `LinkGroupAuthorization.conf` file and the link
> groups have been updated.
>
>     (SrmSpaceManager) admin > update link groups
>     Update started.

Example:

In the example above you created the link group `spacemanager_WriteLinkGroup`. Now you want to allow
members of the VO `desy` with the role `production` to make a space reservation in this link group.

    #SpaceManagerLinkGroupAuthorizationFile
    # this is comment and is ignored

    LinkGroup spacemanager_WriteLinkGroup
    #
    /desy/Role=production

Example:

In this more general example for a `SpaceManagerLinkGroupAuthorizationFile` members of the VO `desy`
with role `test` are authorized to make a space reservation in a link group
called `desy-test-LinkGroup`. Moreover, all members of the VO `desy` are authorized to make a
reservation in the link group called `desy-anyone-LinkGroup` and anyone is authorized to make a
space reservation in the link group called `default-LinkGroup`.

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
    110000 voGroup:/desy voRole:production retentionPolicy:CUSTODIAL accessLatency:NEARLINE
    linkGroupId:0 size:5000000 created:##TODAY_DAY_OF_WEEK## ##TODAY_MONTH_NAME## ##TODAY_2DAY_OF_MONTH## 12:43:48 ##TODAY_TIMEZONE## ##TODAY_YEAR## lifetime:10000000ms expiration:##TODAY_DAY_OF_WEEK## ##TODAY_MONTH_NAME## ##TODAY_2DAY_OF_MONTH## 15:30:28 ##TODAY_TIMEZONE## ##TODAY_YEAR## description:DESY_TEST state:RESERVED used:0 allocated:0

The space token of the reservation is `110000`.
Check the status of the reservation by

    (SrmSpaceManager) admin > ls spaces -e -h
     TOKEN RETENTION LATENCY FILES ALLO   USED   FREE   SIZE             EXPIRES DESCRIPTION
    110000 CUSTODIAL NEARLINE    0   0B +   0B + 5.0M = 5.0M ##TODAY_YEAR##-##TODAY_2MONTH##-##TODAY_2DAY_OF_MONTH## 12:43:48 DESY_TEST

    (SrmSpaceManager) admin > ls link groups -h
    FLAGS CNT RESVD   AVAIL   FREE             UPDATED NAME
    --rc:no 1  5.0M +  7.3G = 7.3G ##TODAY_YEAR##-##TODAY_2MONTH##-##TODAY_2DAY_OF_MONTH## 12:12:51 spacemanager_WriteLinkGroup

Here the `-h` option indicates that approximate, but human readable, byte sizes are to be used,
and `-e` indicates that ephemeral (time limited) reservations should be displayed too (by default
time limited reservations are not displayed as they are often implicit reservations). As can be
seen, 5 MB are now reserved in the link group, although with approximate byte sizes, 5 MB do not
make a visible difference in the 7.3 GB total size.
You can now copy a file into that space token.

```console-user
srmcp -space_token=110000 file://bin/sh \
|    srm://dcache.example.org/data/mydata
```

Now you can check via the [Webadmin Interface](config-frontend.md) or
the [Web Interface](intouch.md#the-web-interface-for-monitoring-dcache) that the file has been
copied to the pool `spacemanager-pool`.

There are several parameters to be specified for a space reservation.

    (SrmSpaceManager) admin > reserve space [-al=online|nearline] [-desc=<string>] -lg=<name>
    [-lifetime=<seconds>] [-owner=<user>|<fqan>] [-rp=output|replica|custodial] <size>

[-owner=<user>|<fqan>]
The owner of the space is identified by either mapped user name or FQAN. The owner must be
authorized to reserve space in the link group in which the space is to be created. Besides the
dCache admin, only the owner can release the space. Anybody can however write into the space (
although the link group may only allow certain storage groups and thus restrict which file system
paths can be written to space reservation, which in turn limits who can upload files to it).

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
The life time of the space reservation should be specified in seconds. If no life time is specified,
the space reservation will not expire automatically.

#### Releasing a Space Reservation

If a space reservation is not needed anymore it can be released with

    (SrmSpaceManager) admin > release space <spaceTokenId>

Example:

    (SrmSpaceManager) admin > reserve space -owner=/desy -desc=DESY_TEST -lifetime=600 5000000
    110042 voGroup:/desy voRole:production retentionPolicy:CUSTODIAL accessLatency:NEARLINE
    linkGroupId:0 size:5000000 created:##TODAY_DAY_OF_WEEK## ##TODAY_MONTH_NAME## ##TODAY_2DAY_OF_MONTH## 12:00:35 ##TODAY_TIMEZONE## ##TODAY_YEAR## lifetime:600000ms
    expiration:##TODAY_DAY_OF_WEEK## ##TODAY_MONTH_NAME## ##TODAY_2DAY_OF_MONTH## 12:10:35 ##TODAY_TIMEZONE## ##TODAY_YEAR## description:DESY_TEST state:RESERVED used:0
    allocated:0

    (SrmSpaceManager) admin > release space 110042
    110042 voGroup:/desy voRole:production retentionPolicy:CUSTODIAL accessLatency:NEARLINE
    linkGroupId:0 size:5000000 created:##TODAY_DAY_OF_WEEK## ##TODAY_MONTH_NAME## ##TODAY_2DAY_OF_MONTH## 12:00:35 ##TODAY_TIMEZONE## ##TODAY_YEAR## lifetime:600000ms
    expiration:##TODAY_DAY_OF_WEEK## ##TODAY_MONTH_NAME## ##TODAY_2DAY_OF_MONTH## 12:10:35 ##TODAY_TIMEZONE## ##TODAY_YEAR## description:DESY_TEST state:RELEASED used:0
    allocated:0

You can see that the value for `state` has changed from `RESERVED` to `RELEASED`.

### Making and Releasing a Space Reservation as a User

If so authorized, a user can make a space reservation through the SRM
protocol. A user is authorized to do so using the
`LinkGroupAuthorization.conf` file.

#### VO based Authorization Prerequisites

In order to be able to take advantage of the virtual organization (VO) infrastructure and VO based
authorization and VO based access control to the space in dCache, certain things need to be in
place:

- User needs to be registered with the VO.

- User needs to use [`voms-proxy-init`](config-gplazma.md#creating-a-voms-proxy) to create a VO
  proxy.

- dCache needs to use gPlazma with modules that extract VO attributes from the user’s proxy. (
  See [Chapter 10, Authorization in dCache](config-gplazma.md), have a look at `voms` plugin and see
  the section called [“VOMS Proxy Certificate”](config-gplazma.md#voms-proxy-certificate) for an
  example with voms.

Only if these 3 conditions are satisfied the VO based authorization of the SpaceManager will work.

#### VO based Access Control Configuration

As mentioned [above](#spacemanager-configuration) dCache space reservation functionality access
control is currently performed at the level of the link groups. Access to making reservations in
each link group is controlled by
the [`SpaceManagerLinkGroupAuthorizationFile`](#the-spacemanagerlinkgroupauthorizationfile).

This file contains a list of the link groups and all the VOs and the VO Roles that are permitted to
make reservations in a given link group.

When a `SRM` Space Reservation request is executed, its parameters, such as reservation size,
lifetime, access latency and retention policy as well as user's VO membership information is
forwarded to the `SRM SpaceManager.

Once a space reservation is created, no access control is performed, any user can store the files in
this space reservation, provided he or she knows the exact space token.

#### Making and Releasing a Space Reservation

A user who is given the rights in the `SpaceManagerLinkGroupAuthorizationFile` can make a space
reservation by

```console-user
srm-reserve-space -retention_policy=<RetentionPolicy> -lifetime=<lifetimeInSecs> -desired_size=<sizeInBytes> -guaranteed_size=<sizeInBytes>  srm://example.dcache.org/
|Space token =SpaceTokenId
```

and release it by

```console-user
srm-release-space srm://dcache.example.org/ -space_token=SpaceTokenId
```

> **NOTE**
>
> Please note that it is obligatory to specify the retention policy while it is optional to specify
> the access latency.

Example:

```console-user
srm-reserve-space -retention_policy=REPLICA -lifetime=300 -desired_size=5500000 -guaranteed_size=5500000  srm://dcache.example.org
|Space token =110044
```

The space reservation can be released by:

```console-user
srm-release-space srm://dcache.example.org -space_token=110044
```

#### Space Reservation without VOMS certificate

If a client uses a regular grid proxy, created with `grid-proxy-init`, and not a VO proxy, which is
created with the `voms-proxy-init`, when it is communicating with `SRM` server in dCache, then the
VO attributes can not be extracted from its credential. In this case the name of the user is
extracted from the Distinguished Name (DN) to use name mapping. For the purposes of the space
reservation the name of the user as mapped by `gplazma` is used as its VO Group name, and the VO
Role is left empty. The entry in the `SpaceManagerLinkGroupAuthorizationFile` should be:

    #LinkGroupAuthorizationFile
    #
    <userName>

#### Space Reservation for non SRM Transfers

Edit the file `/etc/dcache/dcache.conf` to enable space reservation
for non-SRM transfers.

```ini
spacemanager.enable.reserve-space-for-non-srm-transfers=true
```

If the `spacemanager` is enabled, `spacemanager.enable.reserve-space-for-non-srm-transfers` is set
to true, and if the transfer request comes from a door, and there was no prior space reservation
made for this file, the `SpaceManager` will try to reserve space before satisfying the request.

Possible values are `true` or `false` and the default value is false.

This is analogous to implicit space reservations performed by the srm, except that these
reservations are created by the `spacemanager` itself. Since an `SRM` client uses a non-`SRM`
protocol for the actual upload, setting the above option to true while disabling implicit space
reservations in the `srm`, will still allow files to be uploaded to a link group even when no space
token is provided. Such a configuration should however be avoided: If the srm does not create the
reservation itself, it has no way of communicating access latency, retention policy, file size, nor
lifetime to `spacemanager`.

#### dcache.enable.space-reservation

`dcache.enable.space-reservation` tells if the space management is activated.

Possible values are `true` and `false`. Default is `true`.

#### dcache.enable.overwrite

`dcache.enable.overwrite` tells SRM and GRIDFTP servers if overwriting is allowed. If enabled on
the SRM node, it should be enabled on all GRIDFTP nodes.

Possible values are `true` and `false`. Default is `false`.

#### spaceManagerDatabaseHost

`spacemanager.db.host` tells SpaceManager which database host to connect to.

Default value is the property value of `dcache.db.host`, which is the default host of RDBMS used by
various services.
