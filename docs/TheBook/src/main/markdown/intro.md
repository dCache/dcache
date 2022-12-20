Chapter 1. Introduction
=======================

-----
[TOC bullet hierarchy]
-----

dCache is a distributed storage system providing location independent access to data.
The data are stored across multiple data servers as complete files presented to end-user via a single rooted namespace.

As the physical location of the data is not exposed to the user it can be migrated from one data server to another without
interruption of service. Therefore the system can be expanded or contracted by adding / removing data servers at any time.

dCache can be configured to work as fast disk cache in front of tertiary storage systems. Such systems typically store data on magnetic tapes instead of disks, which must be loaded and unloaded using a tape robot. The main reason for using tertiary storage is cost-efficiency
of archiving a very large amount of data on less expensive hardware. Slower media or limited resources (like number of tape drives)
of tertiary storage systems lead to significantly higher access latency for archived data.

dCache supports multiple data transfer protocols. The protocols are implemented as
services that have modular deployment, allowing horizontal scaling by
adding front-end machines without service interruption.

Another performance feature of dCache is hot-spot data migration.
In a situation when a rate of requests to read a file or a group of files
is high resulting in a single data server or a group of data servers becoming
"hot". If this happens dCache detects the condition and attempts to spread the load
by distributing popular files to other, less busy, data servers.


The flow of data within dCache can also be carefully controlled.
This is especially important for large sites as chaotic movement of data
may lead to suboptimal usage. Instead, incoming and outgoing data can be marshaled so they use designated resources guaranteeing better throughput and improving end-user experience.

dCache provides a comprehensive administrative interface for configuring the dCache instance. This is described in the later sections of this book.



The layer model shown in [The dCache Layer Model] gives an overview of the architecture of the dCache system.

  [The dCache Layer Model]: images/layer_model.jpg


## Architecture

As it is shown in the figure [Components of dCache System] dCache has many important components. A minimal dCache instance must have:
- **door**
- **namespace**
- **pool**
- **poolmanager**
- **zookeepr**

A typical deployment include additional services, like the **admin** interface,
**billing** or a web-based **front-end**.

The namespace provides a single rooted hierarchical file system view of the stored data.
The pools contain only data files identified by a unique id which is assigned by the namespace at object creation time. This separation of data and metadata makes dCache extremely scalable.
To grow an instance, just the addition of new pool nodes is required.

In order to interact with the data, a protocol specific entry point called **door** is required. A **door** is a protocol
converter that translates protocol instructions to a sequence of internal dCache message-based call sequences.
Each door implements only one protocol. The system supports running multiple doors of the same protocol for scalability.

The **PoolManager** is the heart of the dCache system. It manages all the pools in the system. It's main
task is data dispatch based on data selection rules, relative pool cost and availability of tertiary system
support.


## Components of dCache System

|      Services                        |Responsible for    |
|:------------------------------------------:|:----|
| [admin](config-admin.md)                   | Admin shell service. Allows to interact with all dCache components.  |
| [alarms](config-alarms.md)                   | Records errors requiring more or less urgent intervention.  |
| [billing](config-billing.md)                 |  Built-in monitoring capabilities, which provide an overview of the activity and performance of the installation’s doors and pools.  |
| [poolmanager](config-PoolManager.md)            | When   a file  reading or writing a transfer request is sent to the dCache system, the poolmanager then decides how to handle this request.     |
| [resilience](config-resilience.md)                      |Controls the number of replicas of a file on the pools. |
| [hoppingmanager](config-hopping.md)               | Service that orchestrates file replica distribution across the pools triggered by a variety of conditions.     |
| [cleaner](config-cleaner.md)                     | Service that periodically cleans (removes) deleted files' replicas from the pools. |
| [info](config-info-provider.md)         | Provides information about the dCache instance in a standard format called GLUE. |
| [missing-files](config-missing-files.md)                | A component designed to react to requests to retrieve missing files.      |
 |[pool](cookbook-pool.md)                      | Data storage (cookbook)    |
| [pnfsmanager](config-PnfsManager.md)                   | Manages the pnfs file system (hierarchy), pnfs database, meta-data.    |
| [srmmanager](config-SRM.md)                      | Storage Resource Manager (SRM) back-end. Provides dynamic space allocation and file management on shared storage components on the Grid.     |
| [srm](config-SRM.md)                   | Front-end to SRM. Provides a client entry point to the SRM sub-system (see above). |
| [spacemanager](config-SRM.md)                        |  A component of SRM responsible for dynamic space allocation. and management.  |
| [zookeeper](config-zookeeper.md)                    |  A distributed directory and coordination service that dCache relies on.|
| [gplazma](config-gplazma.md)                      | Authentication and authorization interface to limit access to data. |
| [dCap](cookbook-dCap.md)                  | Supports all necessary file metadata and namespace manipulation. operations     |
| [xrootd](config-xrootd.md)                     | Allows file transfers in and out of dCache using xrootd.    |
| webdav                      | Offers additional features to admins like sending admin-commands equal to those of the admin interface (CLI).     |
| ftp                      | -     |
| [frontend](config-frontend.md)                      | An HTTP container service delivering the dCache RESTful API as well as the Polymer web components (dCache-View).   |
| [nfs](config-nfs.md)                    |  Allows clients to mount dCache and perform POSIX IO using standard NFSv4.1 clients.   |
| [history](config-history.md)                      | A 'collector' service responsible for gathering and caching monitoring and state information from various dCache backend services and pools. |
| transfermanager                      | -     |
| [pinmanager](config-pinmanager.md)            |  Ensures the presence of specific file replicas on disk by preventing them from being garbage-collected.     |



  [Components of dCache System]: images/test2.svg
  [figure\_title]: #fig-intro-layer-model


## Cells and Domains

dCache, as distributed storage software, can provide a coherent service using multiple computers or nodes (the two terms are used interchangeably). Although dCache can provide a complete storage solution on a single computer, one of its strengths is the ability to scale by spreading the work over multiple nodes.

A cell is dCache's most fundamental executable building block. Even a small dCache deployment will have many cells running. Each cell has a specific task to perform and most will interact with other cells to achieve it.

Cells can be grouped into common types; for example pools, doors. Cells of the same type behave in a similar fashion and have higher-level behaviour (such as storing files, making files available). Later chapters will describe these different cell types and how they interact in more detail.

A *domain* is a container for running cells. Each domain runs in its own Java Virtual Machine (JVM) instance, which it cannot share with any other domain. In essence, a domain *is* a JVM with the additional functionality necessary to run cells (such as system administration and inter-cell communication). This also implies, that a node's resources, such as memory, available CPU and network bandwidth, are shared among several domains running on the same node.

dCache comes with a set of domain definitions, each specifying a useful set of cells to run within that domain to achieve a certain goal. These goals include storing data, providing a front-end to the storage, recording file names, and so on. The list of cells to run within these domains are recommended deployments: the vast majority of dCache deployments do not need to alter these lists.

A node is free to run multiple domains, provided there's no conflicting requirement from the domains for exclusive access to hardware. A node may run just a single domain, but typically a node will run multiple domains. The choice of which domains to run on which nodes will depend on expected load of the dCache instance and on the available hardware. If this sounds daunting, don't worry: starting and stopping a domain is easy and migrating a domain from one node to another is often as easy as stopping the domain on one node and starting it on another.

dCache is scalable storage software. This means that (in most cases) the performance of dCache can be improved by introducing new hardware. Depending on the performance issue, the new hardware may be used by hosting a domain migrated from a overloaded node, or by running an additional instance of a domain to allow load-balancing.

Most cells communicate in such a way that they don't rely on in which domain they are running. This allows a site to move cells from one domain to another or to create new domain definitions with some subset of available cells. Although this is possible, it is rare that redefining domains or defining new domains is necessary. Starting or stopping domains is usually sufficient for managing load.

> **NOTE**
>
> - Each domain **must** have a dCache instance unique name. Therefore each cell has a unique fully qualified name like `cellName@domainName`.
> - In muti-domain setup at least one domain **must** be a `core` domain. In HA mode, 2 cores are sufficient.
> - All domains connected to the same ZooKeeper are part of a single dCache instance.


## Protocols Supported by dCache

|                              |dCap  |FTP   |xrootd|NFSv4.1| WebDAV | SRM |
|:----------------------------:|:----:|:----:|:----:|:-----:|:------:|:---:|
|                              | +    | +    | +    | +     | +      | -   |
| kerberos                     | +    | +    | -    | +     | -      | -   |
| Client Certificate           | +    | +    | +    | -     | +      | +   |
| username/password            | +    | +    | -    | -     | +      | -   |
| Control Connection Encrypted | +    | +    | +    | +     | +      | +   |
| Data Connection Encrypted    | -    | -    | -    | +     | +      | +   |
| passive                      | +    | +    | +    | +     | +      | -   |
| active                       | +    | +    | -    | -     | -      | -   |


The next chapter  describes the installation of  dCache instance [Installation](install.md)


  [figure\_title]: #fig-intro-layer-model
