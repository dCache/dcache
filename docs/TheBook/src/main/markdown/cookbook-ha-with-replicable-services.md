Highly Available dCache Services
================================

This document describes how to configure dCache servics in a high availability
mode, avoiding single points of failure and thus enabling rolling upgrades and
in some cases horizontal scalability.

For this purpose, dCache services may be grouped into four categories:

-   Pools

-   Doors

-   Central services critical for file access

-   Central services not criticical for file access

Services in each of these groups are described separately in the following.

Before considering redundancy of high level services, it is essential that the
low level communication infrastructure used by dCache is redundant. This
involves setting up a [ZooKeeper cluster](config-zookeeper.md) of at least three
nodes, as well as using a multipath topology for [Cell Message passing](config-message-passing.md).

-----
[TOC bullet hierarchy]
-----

## Pools

Pools contain the data files stored in dCache. If a pool is offline, any file
stored on this pool and not available on any other pool or on tape will be
inaccessible. Upload of new files on the other hand is possible as long as at
least one write pool is available.

The only way to avoid that files become inacessible when a pool is offline is to
replicate the files onto a second pool. One common way to do this is using the
`resilience` service. That service is described in a separate document.

The development team is working on means to expose the same pool storage through
several pool cells, e.g. when using a cluster file system or a third party
object store as the data backend. We can, however, not provide any guarantees
when that work will be completed.

## Doors

Doors in dCache are the enduser endpoints. These expose the files in dCache
through one of several supported protocols, e.g. FTP, HTTP, or NFS.

dCache allows an arbitrary number of instances of a door, but these will all
appear as separate endpoints to the enduser. There are several possibilities to
balance load over these endpoints and to allow rolling upgrades:

-   Use the `srm` service. The SRM protocol acts as a redirector for transfers
    and the implementation in dCache provides means of balancing load over
    several doors as well as drain particular doors to allow them to be
    upgraded.

-   Let clients discover endpoints through an information service. If clients
    can discover endpoints through an information service such as BDII, such
    clients can load balance over available endpoints. dCache doors provides
    means to dynamically unpublish themselves from the information service and
    thus to drain a door.

-   Use DNS load balancing. The endpoints are assigned a common DNS name. This
    requires that clients support balancing the load over the resolved IP
    addresses or some other means to make clients choose a random endpoint. To
    drain a door one would remove that door from the DNS record.

-   Use an external load balancing proxy. One can configure an transparent proxy
    in front of the dCache doors to balance the load over all avaiable
    endpoints. Such a proxy may be implemented in a load balancing switch or in
    software such as HA Proxy.

### `srm`

The SRM implementation in dCache is technically a door, even though it merely
redirects a client to one of the other endpoints for the actual transfer. In
dCache 2.16, the SRM implementation was split into two separate services: The
frontend service technically consitutes the door and is called `srm`. The
backend service is called `srmmanager` and is treated as a central dCache
service.

The frontend `srm` service can be scaled like any other door. Each instance will
appear as a separate endpoint and the instances will typically talk to the same
`srmmanager` backend. The same techniques as for other doors may be used to make
these appear as a single endpoint to the enduser.

## Critical Central Services

Several services are critical for file transfers, meaning if any of these
services is unavailable, file transfers may be affected. The way to avoid those
to become a single point of failure, and to allow rolling upgrades is more or
less the same for all services.

The central concept is that of separating the logical service from the physical
service instance. While a physical service instance is always a cell, the
logical service is merely an unqualified logical address that any of the
physical instances may respond to. As long as other services use the logical
service address, any of the physical instances can be unavailable without loss
of service.

In the internal dCache messaging system, two types of logical addresses: Topics
and queues. While physical cell addresses are fully qualified and contain both
the cell name and the dCache domain name, topics and queues lack the domain name
suffix (although they internally are represented with an `@local` suffix). E.g.
`PnfsManager@namespaceDomain` is a fully qualified cell address and identifies a
specific cell instance, while `PnfsManager` is a logical address.

In dCache 6.2, many -- although not all -- services are replicable. A
replicable service is one that supports the above separation between a logical
address and a physical address and which supports having several physical
instances use the same logical address. One may recognize replicable services by
using the `dcache services` command or by inspecting the default properties for
the service and the value of the corresponding `cell.replicable` property.

To drain a particular instance, one typically removes the cell message route to
that instance, effectively decoupling the instance from the logical service
name. This is done in the `System` cell of the domain hosting the instance. E.g.
in the admin shell:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
\c System@namespaceDomain
route delete PnfsManager PnfsManager@namespaceDomain
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

After this, no new requests will reach that `pnfsmanager` instance and once the
service is idle it may be shut down.

The following is a list of critical replicable services in dCache 6.2.

### `spacemanager`

Spacemanager is fully replicable. Several instances must share the same
database as requests from doors will be load balanced over all physical
instances. The configuration should be synchronized such that all instances are
configured the same way.

### `pinmanager`

Pinmanager is fully replicable. Several instances must share the same database
as requests from doors will be load balanced over all physical instances. The
configuration should be synchronized such that all instances are configured the
same way.

All pinmanager instances in an installation will receive and perform pinning
operations. Nevertheless, all pinmanager instances will negotiate a leader by
means of ZooKeeper for unpinning operations. Only this leader will perform
these operations at any point in time. Another pinmanager instance will take
over, should this leader disappear.

This high availability (HA) role and the participants may be queried:
```
\c PinManager@domain
ha get role
ha show participants
```


### `srmmanager`

Srmmanager is fully replicable. Several instance must have separate databases.
The configuration should be synchronized such that with the exception of the
database settings all instances are configured the same way.

Requests are load balanced over all physical instances, but since the SRM
protocol is stateful, stateful requests will be tagged by an instance
identifier. Each instance registers itself in ZooKeeper and `srm` frontends
forward tagged requests to the corresponding backend instance. When querying
requests by token in the backend, one has to strip the tag from the token. E.g.
a client may see a token as `fb1991c5:-1093540442`, where `fb1991c5` is a
backend indentifier and `-1093540442` is the backend token. One may map the
backend identifier to an instance through ZooKeeper:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
\c System
zk get /dcache/srm/backends/fb1991c5
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

It is possible to have several logical `srmmanager` instances, e.g. one per VO.
Each logical instance may have one or more physical instances. This may be
useful to provide VO specific configuration. In such a setup one would have VO
specific `srm` frontends, each configured to talk to a specific logical backend
(which in turn may be implemented by several physical instances).

### `pnfsmanager`

Pnfsmanager is fully replicable. Several instances must share the same database
as requests from doors will be load balanced over all physical instances. The
configuration should be synchronized such that all instances are configured the
same way.

### `gplazma`

gPlazma is fully replicable. The configuration should be synchronized such that
all instances are configured the same way.

## Non-critical Central Services

Non-critical services will not directly affect transfers in case of
unavailabilty. As such, it may be unnecessary to replicate these services if the
primary interest is rolling upgrades.

The following is a list of non-critical replicable services in dCache 6.2.

### `billing`

To replicate the billing service, the underlying store should be shared,
otherwise one risks potentially dispersing text records over several nodes.
Hence, a shared rdbms database instance should be enabled.  Absent a database,
enabling kafka may offer an alternative to centralized record-keeping
without the bottleneck of a single dCache service.

### `cleaner`

`cleaner-disk` and `cleaner-hsm` are both fully replicable. Several instances
must share the same database. The configuration should be synchronized such
that all instances of the same type are configured the same way.

When there are several `cleaner-disk` or `cleaner-hsm` instances in an installation,
they negotiate a leader by means of ZooKeeper. Only this leader is active for all
`cleaner-disk`/`cleaner-hsm` operations at any point in time. Should the leader
disappear, another cleaner instance of the same type will take over.

This high availability (HA) role and the participants may be queried:
```
\c cleaner-disk@domain
ha get role
ha show participants
```

### `admin`

This is a door and one can have multiple instances of `admin` like any other
door.

### `httpd`

This is a door and one can have multiple instances of `httpd` like any other
door.

### `info`

This service is replicable and one can have multiple instances of `info` all
sharing the same logical service name. Each instance will collect the same
information. Requests from `httpd` will be load balanced over available
instances of `info`.

### `topo`

This service is replicable and one can have multiple instances of `topo` all
sharing the same logical service name. Each instance will collect the same
information. Requests will be load balanced over available instances of `topo`.

### `statistics`

This service collects nightly statistics about available pools. One can have
multiple instances of `statistics` and each instance will collect the same
information.
