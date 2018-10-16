Chapter 1. Introduction
=======================

Table of Contents
------------------

* [dCache Architecture](#architecture)
* [Cells and Domains](#cells-and-domains)  
* [Cells Communication](#cells-communication)  
* [Protocols Supported by dCache](#protocols-supported-by-dcache)
* [Logging](#logging)

dCache is a distributed storage solution. It organises storage across computers so the combined storage can be used without the end-users being aware of where their data is stored. They simply see a large amount of storage.

Because end-users do not need to know on which computer their data is stored, it can be migrated from one computer to another without any interruption of service. As a consequence, (new) servers may be added to or taken away from the dCache storage cluster at any time.

dCache supports requesting data from a tertiary storage system. Such systems typically store data on magnetic tapes instead of disks, which must be loaded and unloaded using a tape robot. The main reason for using tertiary storage is the better cost-efficiency, archiving a very large amount of data on rather inexpensive hardware. In turn the access latency for archived data is significantly higher.

dCache also supports many transfer protocols (allowing users to read and write to data). These have a modular deployment, allowing dCache to support expanded capacity by providing additional front-end machines.

Another performance feature of dCache is hot-spot data migration. In this process, dCache will detect when files are requested very often. If this happens, dCache can generate duplicates of the popular files on other computers. This allows the load to be spread across multiple machines, so increasing throughput.

The flow of data within dCache can also be carefully controlled. This is especially important for large sites as chaotic movement of data may lead to suboptimal usage. Instead, incoming and outgoing data can be marshaled so they use designated resources guaranteeing better throughput and improving end-user experience.

dCache provides a comprehensive administrative interface for configuring the dCache instance. This is described in the later sections of this book.  



The layer model shown in [The dCache Layer Model] gives an overview of the architecture of the dCache system.

Architecture
============

As it is shown in the Figure # dCache has many important components. Nonetheless  A minimal dCache instance must consist out of **namespace**, **pool**, **poolmanager** and a **door**. However, in a typical deployment additional service are recommended, like **admin** interface, **billing** or web-based **front-end**.

The namespace provides a single rooted hierarchical file system view of the store data. On an other hand, pools contain only data files identified by unique id which is given by namespace at object creation time. Such separation of data and metadata makes dCache extremely scalable, as to grow the instance only adding a new pool nodes are required.

In addition to namespace and pools to interact with the users a client protocol specific entry points, called **doors** are required. Though pool can serve any supported protocol, a door speak only one.

The coordination of data distribution with a set of pools done by **PoolManager**. It responsible to select appropriate pool to receive the data from a clients as well as to restore offline files from tertiary storage system if required.

Components of dCache System
=============================

|      Services                        |Responsible for    |
|:------------------------------------------:|:----|
| [alarms](config-alarms.md)                   | Records errors requiring more or less urgent intervention.  |
| [billing](config-billing.md)                 |  Built-in monitoring capabilities which provide an overview of the activity and performance of the installation’s doors and pools      |
| [poolmanager](config-PoolManager.md)            | When   a file  reading or writing a transfer request is sent to the dCache system, the poolmanager then decides how to handle this request.     |
| [replicamanager](config-ReplicaManager.md)               |   n/a in services ?  |
| [resilience](congif-Resilience.md)                      |Controls the number of replicas of a file on the pools |
| [hoppingmanager](config-hopping.md)               | ?     |
| cleaner                      | ?    |
| info                         | ?     |
| missing-files                | +      |
 |[pool](cookbook-pool.md)                      | Data storage ? (cookbook)    |
| [pnfsmanager](config-PnfsManager.md)                   | Managing the pnfs file system (hierarchy), pnfs database, meta-data    |
| spacemanager                       | +      |
| [srm](config-SRM.md)                   | Provides dynamic space allocation and file management on shared storage components on the Grid     |
| srmmanager                       | ?   |
| [zookeeper](config-zookeeper.md)                    |  A distributed directory and coordination service on which dCache relies on|
| [gplazma](config-gplazma.md)                      | Authentication and authorization interface to limit access to data |
| [dCap](cookbook-dCap.md)                  | Supports all necessary file metadata and name space manipulation operations     |
| [xrootd](config-xrootd.md)                     | Allows file transfers in and out of dCache using xrootd    |
| webdav                      | Offers additional features to admins like sending admin-commands equal to those of admin interface (CLI)     |
| ftp                      | -     |
| httpd                      | -     |
| [nfs](config-nfs.md)                    |  Allows clients to mount dCache and perform POSIX IO using standard NFSv4.1 clients   |
| frontend                      | -     |
| history                      | -     |
| transfermanager                      | -     |
| pinmanager                      | -     |















  [Components of dCache System]: images/test2.svg
  [figure\_title]: #fig-intro-layer-model


Cells and Domains
=================

dCache, as distributed storage software, can provide a coherent service using multiple computers or nodes (the two terms are used interchangeable). Although dCache can provide a complete storage solution on a single computer, one of its strengths is the ability to scale by spreading the work over multiple nodes.

A cell is dCache's most fundamental executable building block. Even a small dCache deployment will have many cells running. Each cell has a specific task to perform and most will interact with other cells to achieve it.

Cells can be grouped into common types; for example, pools, doors. Cells of the same type behave in a similar fashion and have higher-level behaviour (such as storing files, making files available). Later chapters will describe these different cell types and how they interact in more detail.

A *domain* is a container for running cells. Each domain runs in its own Java Virtual Machine (JVM) instance, which it cannot share with any other domain. In essence, a domain *is* a JVM with the additional functionality necessary to run cells (such as system administration and inter-cell communication). This also implies, that a node's resources, such as memory, available CPU and network bandwidth, are shared among several domains running on the same node.

dCache comes with a set of domain definitions, each specifying a useful set of cells to run within that domain to achieve a certain goal. These goals include storing data, providing a front-end to the storage, recording file names, and so on. The list of cells to run within these domains are recommended deployments: the vast majority of dCache deployments do not need to alter these lists.

A node is free to run multiple domains, provided there's no conflicting requirement from the domains for exclusive access to hardware. A node may run a single domain; but, typically a node will run multiple domains. The choice of which domains to run on which nodes will depend on expected load of the dCache instance and on the available hardware. If this sounds daunting, don't worry: starting and stopping a domain is easy and migrating a domain from one node to another is often as easy as stopping the domain on one node and starting it on another.

dCache is scalable storage software. This means that (in most cases) the performance of dCache can be improved by introducing new hardware. Depending on the performance issue, the new hardware may be used by hosting a domain migrated from a overloaded node, or by running an additional instance of a domain to allow load-balancing.

Most cells communicate in such a way that they don't rely on in which domain they are running. This allows a site to move cells from one domain to another or to create new domain definitions with some subset of available cells. Although this is possible, it is rare that redefining domains or defining new domains is necessary. Starting or stopping domains is usually sufficient for managing load.

Protocols Supported by dCache
=============================

|                              |dCap  |FTP   |xrootd|NFSv4.1| WebDAV | SRM |
|:----------------------------:|:----:|:----:|:----:|:-----:|:------:|:---:|
|                              | +    | +    | +    | +     | +      | -   |
| kerberos                     | +    | +    | -    | +     | -      | -   |
| Client Certificate           | +    | +    | +    | -     | +      | +   |
| username/password            | +    | +    | -    | -     | +      | -   |
| Control Connection Encrypted | +    | +    | +    | +     | +      | +   |
| Data Connection Encrypted    | -    | -    | -    | +     | +      | +   |
| passiv                       | +    | +    | +    | +     | +      | -   |
| active                       | +    | +    | -    | -     | -      | -   |

  [The dCache Layer Model]: images/test2.svg
  [figure\_title]: #fig-intro-layer-model
