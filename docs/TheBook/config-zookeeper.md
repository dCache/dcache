ZooKeeper
=========

Since version 2.16, dCache relies on [Apache
ZooKeeper](https://zookeeper.apache.org). ZooKeeper is a distributed directory
and coordination service used by many applications. dCache uses it to ensure
consistent state across multi-node installations.

In this document we outline deployment of dCache with ZooKeeper.

Deployment scenarios
--------------------

### Embedded vs standalone

Apache ZooKeeper may be deployed either embedded inside dCache or as a
standalone installation separate from dCache. Embedded means the ZooKeeper
servers runs as a dCache service with a dCache domain and can be monitored as a
dCache cell. No separate software packages needs to be installed. Standalone
means that ZooKeeper packages are installed independently of dCache packages and
that the ZooKeeper daemon runs independently of any dCache domains.

There is no doubt that the embedded deployment is easier, but we strongly urge
sites to use a standalone ZooKeeper installation for production setups. A
standalone installation may be deployed on the same hardware as dCache
(typically the head nodes). A separate installation has the advantage of
allowing the two products to be upgraded independently. It also makes Zookeeper
more robust in the face of dCache restarts. Since dCache relies on ZooKeeper at
a rather low level, running an embedded ZooKeeper as a regular dCache service
cause several chicken and egg problems.

### Single or clustered

ZooKeeper may be deployed in a single server setup or a clustered multi-server
setup. For clustered deployments, *2n + 1* severs are required to allow *n*
servers to fail. Typically one would have 3 or 5 ZooKeeper servers to allow 1 or
2 to be lost. An embedded deployment is always limited to a single server.

Reads from ZooKeeper scale with the number of servers. Writes however tend to
become slower the more servers are added. This is because each ZooKeeper server
replicates the full state and any update has to be written to a quorum of
servers.

### Persistent data

Data stored in ZooKeeper is stored persistently on disk on each ZooKeeper
server. Currently dCache does not write a lot of data to ZooKeeper and IO
performance is not critical. Any write will however be followed by an *fsync*,
so keep latency in mind, that is, avoid distributed file systems, NFS, and use a
battery backed write back cache for RAID.

### dCache access to ZooKeeper

Every domain in dCache connects to a ZooKeeper server. The list of ZooKeeper
servers must be configured on every dCache server.

*Upgrades from versions older than 2.16*

When upgrading from a version earlier than 2.16, the defaults are such that
dCache assumes the ZooKeeper server runs on the same host as dCacheDomain. If
this is not the case or if a clustered ZooKeeper setup is used, the connection
information have to be set on every dCache server.

### Authorization and firewalls

ZooKeeper may be configured to require authentication, however dCache currently
does not support this. It is of utmost importance that access to your ZooKeeper
instance is protected by a firewall. This is no different that however cell
communication had to be protected by firewalls in previous releases of dCache.

Future versions of dCache may support authenticating with a ZooKeeper server and
may support encrypted communication.

As dCache currently does not support authentication, all information written
into ZooKeeper will have no ACLs. This means that site may switch off the ACL
support (the `skipACL` option), which ZooKeeper documentation claims will result
in some performance benefits. The overall benefit of this will likely be
limited, unless the ZooKeeper nodes are heavily loaded.

Configuration
-------------

### Embedded ZooKeeper

To start a single server ZooKeeper as an embedded dCache service, place the
`zookeeper` service inside a dCache domain:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
[zookeeperDomain]
[zookeeperDomain/zookeeper]
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

FHS packages of dCache default to `/var/lib/dcache/zookeeper` for the ZooKeeper
persistent state.

*2.16*

We recommend deploying this domain on the same server as dCacheDomain and to
place the zookeeperDomain earlier than the dCacheDomain in the layout file. It
is valid to place the `zookeeper` service in a domain with other services, but
for stability when restarting services we recommend decoupling ZooKeeper as much
as possible.

*≥3.0*

We recommend deploying this domain on the same server as your core domain and to
place the zookeeperDomain earlier than the core domain in the layout file. It is
valid to place the `zookeeper` service in a domain with other services, but for
stability when restarting services we recommend decoupling ZooKeeper as much as
possible.

### Standalone ZooKeeper

We recommend reading the [ZooKeeper admin
guide](https://zookeeper.apache.org/doc/current/zookeeperAdmin.html) for details
on deploying ZooKeeper. Many Linux distributions have prepackaged ZooKeeper.

### Configuring dCache to connect to ZooKeeper

The `dcache.zookeeper.connection` property needs to be defined on every dCache
server to identify the ZooKeeper server or servers. In a clustered ZooKeeper
deployment this property is set to a comma separated list of ZooKeeper servers
endpoints. The ZooKeeper admin guide should contain details on further
configuration options that can be specified in the connection string; however,
this seemed to be missing at the time of writing. Instead, the ZooKeeper
programmers guide [ZooKeeper
Sessions](https://zookeeper.apache.org/doc/trunk/zookeeperProgrammers.html#ch_zkSessions)
section contains some details (search for "connection string").

Each domain in dCache will connect to a single ZooKeeper endpoint. If dCache is
configured with multiple ZooKeeper endpoints, one is chosen at random. Should
that fail, another endpoint is used automatically. Note that ZooKeeper places a
limit on the number of concurrent connections from the same IP address (see
`maxClientCnxns` configuration parameter). Be sure that this number is large
enough for the maximum number of domains running on a single dCache node.

*2.16*

The property defaults to `${dcache.broker.host}:${dcache.zookeeper.net.port}`.
Thus upon upgrade you may deploy ZooKeeper on the server currently hosting
dCacheDomain. If ZooKeeper is deployed on a different server or a ZooKeeper
cluster is used, the connection string needs to be altered on all dCache
servers.

*≥3.0*

The property defaults to `localhost:2181`. Except when deploying a single server
installation, the property has to be defined on all dCache nodes to identify the
ZooKeeper servers.

Inspecting ZooKeeper through dCache
-----------------------------------

Every `System` cell offers two new commands, `zk ls` and `zk get` to list and
read the data stored in ZooKeeper. Since all domains use the same ZooKeeper
instance, the behaviors of these commands is the same in all domains. ZooKeeper
data is organized hierarchically. Each element is called a *znode* and may have
children as well as a value. The name space is organized like a POSIX file
system, with slash as a name separator. E.g. `zk ls /` lists the children under
the root znode.

ZooKeeper ships with its own client that allows many more operations. This can
be used to connect to ZooKeeper independently of dCache. If you modify the data
managed by dCache, all warranty is voided.

If the ZooKeeper service is embedded inside dCache, the `zookeeper` cell can be
accessed through the admin interface and various statistics can be queried. The
cell also has a pinboard with the most recent log messages and allows the log
level to be adjusted on the fly.

How ZooKeeper is used in dCache
-------------------------------

In version 2.16, ZooKeeper replaces the *location manager daemon* that used to
listen to UDP port 11111 in the dCacheDomain. This daemon allowed dCache domains
to locate each other and create TCP connections to allow messages to be
exchanged. Domains now use ZooKeeper instead of the *location manager daemon*.
Separate TCP connections are still established for message passing.

dCache 2.16 also still listens to UDP 11111 to allow older pools to interoperate
with dCache 2.16. Support for this has been removed in dCache 3.0.

We expect that future updates will increase the use of ZooKeeper in dCache.

Expected error messages
-----------------------

dCache will connect to one of the configured ZooKeeper servers. If it is
unavailable it will try the next and keep retrying until successful. Error
messages like the following are to be expected:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
28 Apr 2016 13:02:25 (System) [] ZooKeeper connection to localhost/0:0:0:0:0:0:0:1:2181 failed (Connection refused), attempting reconnect.
28 Apr 2016 13:02:27 (System) [] ZooKeeper connection to localhost/127.0.0.1:2181 failed (Connection refused), attempting reconnect.
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This may even happen with a single server deployment if that server is
unavailable. In particular if embedding ZooKeeper inside a dCache service, such
messages will occur in the log whenever the domain hosting ZooKeeper is starting
or stopping (see aforementioned issue about chickens and eggs).
