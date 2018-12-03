Message passing
===============

The dCache system is divided into cells which communicate with each other via messages. Cells run inside domains and cells communicate by passing messages to each other. Domains are connected through cell tunnels which exchange messages over TCP.

Each domain runs in a separate Java virtual machine and each cell is run as a separate thread therein. Domain names have to be unique. The domains communicate with each other via `TCP` using connections that are established at start-up. The topology is controlled by the location manager service. When configured, all domains connect with a core domains, which routes all messages to the appropriate domains. This forms a star topology.

> **ONLY FOR MESSAGE COMMUNICATION**
>
> The `TCP` communication controlled by the location manager service is for the short control messages sent between cells. Any transfer of the data stored within dCache does not use these connections; instead, dedicated `TCP` connections are established as needed.

Within this framework, cells send messages to other cells addressing them in the form cellName@domainName. This way, cells can communicate without knowledge about the host they run on. Some cells are [well known](rf-glossary.md#well-known-cell), i.e. they can be addressed just by their name without @domainName. Evidently, this can only work properly if the name of the cell is unique throughout the whole system. If two well known cells with the same name are present, the system will behave in an undefined way. Therefore it is wise to take care when starting, naming, or renaming the well known cells. In particular this is true for pools, which are well known cells.

A domain is started with a shell script **bin/dcache start** domainName. The routing manager and location manager cells are started in each domain and are part of the underlying cell package structure. Each domain will contain at least one cell in addition to them.

Naming and addressing
---------------------

Domains must have a name unique through the dCache installation. Each cell has a
unique name within the domain in which it is running. A fully qualified cell
address is formed by combining the cell name and the domain name with an
at-sign, e.g. `PoolManager@dCacheDomain`. Unqualified addresses either do not
have a domain suffix or have a `local` suffix, e.g. `PoolManager@local`. It
follows that `local` is an illegal domain name.

Routing
-------

Each domain has a message routing table. This routing table may be inspected and
manipulated through the `System` cell inside that domain. Routing tables are
maintained automatically by dCache and there is usually no need to manipulate
these manually.

There are several different types of routes:

| domain  | Routes to other domains                 |
|---------|-----------------------------------------|
| topic   | Used for publish-subscribe messaging    |
| queue   | Used for named queues                   |
| alias   | A rewriting rule                        |
| default | Default route if no other route matches |

There are a few other route types which are not commonly used.

### Cell tunnels

Tunnels are cells that establish TCP connections to other tunnels. A tunnel may
either be listening or connecting. When a connection is established each end
adds a domain route to the local routing table allowing messages to the other
domain to be routed through this tunnel. When the tunnel cell shuts down the
domain route is removed.

### Core and satellite domains

dCache domains are either designated as either `core` domains or `satellite` domains
in the configuration. Core domains act as message hubs, forwarding messages on
behalf of satellite domains.

Core domains form a fully connected mesh, that is, each core domain has a tunnel
cell connecting it to a tunnel cell in another core domain. Lexicographic
ordering of the domain name is used to determine which domain connects and which
domain listens for a connection.

Satellite domains connect to all core domains.

Other than that, there are no difference between core and satellite domains -
they can both host arbitrary dCache services, including none at all.

### Location manager

The location manager is an implicit service embedded in every dCache domain. The
cell is called `lm` and one can interact with each instance through the dCache
admin shell. Its task is to establish cell tunnels.

On core domains it will start a cell listening for incoming connections (by
default on TCP 11111 - remember to firewall access to this port). Each core
domain registers itself in [Zookeeper](config-zookeeper.md).

On satellite domains the location manager watches the ZooKeeper state and
whenever a core domain registers itself, all satellites will create tunnel cells
connecting to the core.

### Routing manager

Each domain has an embedded routing manager service. The cell is called
`RoutingMgr` and one can interact with each instance through the dCache admin
shell. Its task is to manage the routing table.

The routing manager monitors the addition and removal of tunnel cells and domain
routes. Whenever a domain route is added in a satellite domain, the routing
manager adds a corresponding default route. Similarly, whenever a tunnel cell
dies, it will remove the installed routes going through that tunnel.

Routing manager instances exchange messages with each other to maintain topic
and queue routes.

### Queues

The concept of named queues is borrowed from other messaging systems, even
though as we will see in a moment the name is slightly misleading in dCache.

A named queue has an unqualified cell address. Cells writing to a named queue
are called producers while cells reading from a named queue are called
consumers. A named queue can have multiple producers and multiple consumers, but
each message is only consumed by a single cell.

Producers do not need to do anything special to write a message to a named
queue. Consumers however explicitly have to announce that they want to consume
from a queue. When they do this a queue route is installed in the local routing
table, allowing messages sent to that queue to be routed to the consumer. The
routing manager picks up the new route and forwards this information to other
routing managers. In particular a consumer in a satellite domain will cause a
corresponding queue route to be installed in all core domains. A consumer in a
core domain too will cause queue routes to be installed in satellite domains.
Consumers in satellite domains do not, however, have queue routes in other
satellite domains - messages instead follow a default route to a core domain
which will know where to route the message.

When several queue routes apply, one is chosen randomly. In this way a certain
amount of load balancing is achieved. Note that no effort is made to perfectly
balance the load. Also note that the address space of a named queue and cells is
not separate. A cell with the same name as a named queue is legal and if a local
cell matches, the message is always delivered locally - i.e. local delivery
takes precedence over the routing table.

Many services in dCache consume messages from a named queue that is the same as
their cell name. This way other services do not need to know the fully qualified
address of the service and can merely write messages to the named queue.

As may have been apparent from the description above, a named queue is not
actually a queue. There is no central store of messages and there is no central
queue in a named queue. The latency involved in communication does introduce a
buffer capacity allowing a certain number of messages in transit between sender
and receiver, but this does not constitute a shared queue. A message produced is
routed to a consumer and queued locally at the consumer.

### Topics

The concept of topics is also borrowed from other messaging systems. A topic is
an unqualified cell address. Cells writing to topic are called publishers, while
cells receiving messages on a topic are called subscribers. In contrast to a
named queue, messages published to a topic are received by all subscribers. Thus
topics provide a multicasting ability to cells.

As with named queues, publishers do not need to do anything special before
writing a message to a topic. Subscribers need to explicitly announce that they
want to subscribe to a topic. When they do, a topic route is added to the local
routing table. The routing manager picks up the new route and informs other
routing managers about the subscription. In particular routing managers in cores
contain topic routes to subscribers in other satellite domains and other core
domains. Satellite domains however only contain topic routes to local
subscribers. Subscribers in other domains are informed through the default
route.

This difference in routing logic with respect to queue routes stems from the
fact that a message to a topic is delivered along all topic routes - as well as
one of the default routes. Messages to named queues on the other hand are only
routed along one of the queue routes chosen at random.

Configuration
-------------

All domains default to being satellite domains. Unless some domain is explicitly
marked as a core domain, domains will be disconnected from each other.

In a mulit-domain (and multi node) deployments at least one of the domains **must** be configured as `core` domain.

### Explicit configuration of the core domain

One may alter the default behavior by setting `dcache.broker.scheme` to either
`core` or `satellite` to designate the domain as either a core or satellite
domain.

### Multipath cell communication

A simple star topology obviously makes the central message hub a single point of
failure. Since dCache 2.16 it is perfectly valid to have multiple core domains.
As mentioned above, the core domains form a fully connected mesh. Satellite
domains connect to all core domains such that the distance between two satellite
domains is never longer than two hops (i.e. a maximum of one intermediate
domain).

The configuration of such a setup follows the same approach as above: Each core
domain sets `dcache.broker.scheme` to `core`. Core domains register themselves
in ZooKeeper and other domains locate them that way.

Care is taken to cleanly register and unregister core domains in a multipath
setup to minimize the effect on the running system. Obviously any service that
happened to be running in a core domain being shut down will be unavailable and
any task that service may have been working on may be lost. Other services will
eventually discover such a situation through a timeout.

Without having production experience with such a setup yet (dCache 2.16 has not
(yet) been released when this is being written), our recommendation would be to
have two or three core domains.

### Core only deployment

For small installations it is viable to mark all domains as cores. In such a
setup all domains are connected to all other domains. This doesn't scale, but if
you don't have more than, say, 10 domains, this should work out just fine. The
benefit is that the distance between services is reduced, resulting in lower
latency.

A word of warning though: The cells messaging system is deliberately very
simple. There is no guaranteed delivery and no guaranteed ordering. Although
dCache should be robust against such problems, core only deployments will be in
uncharted territory.
