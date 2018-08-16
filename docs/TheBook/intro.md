Chapter 1. Introduction
=======================

Table of Contents
------------------

[Cells and Domains](#cells-and-domains)  
[Protocols Supported by dCache](#protocols-supported-by-dcache)

dCache is a distributed storage solution. It organises storage across computers so the combined storage can be used without the end-users being aware of where their data is stored. They simply see a large amount of storage.

Because end-users do not need to know on which computer their data is stored, it can be migrated from one computer to another without any interruption of service. As a consequence, (new) servers may be added to or taken away from the dCache storage cluster at any time.

dCache supports requesting data from a tertiary storage system. Such systems typically store data on magnetic tapes instead of disks, which must be loaded and unloaded using a tape robot. The main reason for using tertiary storage is the better cost-efficiency, archiving a very large amount of data on rather inexpensive hardware. In turn the access latency for archived data is significantly higher.

dCache also supports many transfer protocols (allowing users to read and write to data). These have a modular deployment, allowing dCache to support expanded capacity by providing additional front-end machines.

Another performance feature of dCache is hot-spot data migration. In this process, dCache will detect when files are requested very often. If this happens, dCache can generate duplicates of the popular files on other computers. This allows the load to be spread across multiple machines, so increasing throughput.

The flow of data within dCache can also be carefully controlled. This is especially important for large sites as chaotic movement of data may lead to suboptimal usage. Instead, incoming and outgoing data can be marshaled so they use designated resources guaranteeing better throughput and improving end-user experience.

dCache provides a comprehensive administrative interface for configuring the dCache instance. This is described in the later sections of this book.  

### Figure 1.1. The dCache Layer Model

![The dCache Layer Model](layer_model.jpg "The dCache Layer Model") 

The layer model shown in [The dCache Layer Model] gives an overview of the architecture of the dCache system.


Cells and Domains
=================

dCache, as distributed storage software, can provide a coherent service using multiple computers or nodes (the two terms are used interchangeable). Although dCache can provide a complete storage solution on a single computer, one of its strengths is the ability to scale by spreading the work over multiple nodes.

A cell is dCache's most fundamental executable building block. Even a small dCache deployment will have many cells running. Each cell has a specific task to perform and most will interact with other cells to achieve it.

Cells can be grouped into common types; for example, pools, doors. Cells of the same type behave in a similar fashion and have higher-level behaviour (such as storing files, making files available). Later chapters will describe these different cell types and how they interact in more detail.

There are only a few cells where (at most) only a single instance is required. The majority of cells within a dCache instance can have multiple instances and dCache is designed to allow load-balancing over these cells.

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
|    kerberos                  | +    | +    | -    | +     |  -     | -   |
| Client Certificate           | +    | +    | +    | -     | +      | +   |
| username/password            | +    | +    | -    | -     | +      | -   |
| Control Connection Encrypted | +    | +    | +    | +     | +      | +   |
| Data Connection Encrypted    | -    | -    | -    | +     | -      | -   |
| passiv                       | +    | +    | +    | +     | +      | +   |
| active                       | +    | +    | -    | -     | -      | -   |

  [The dCache Layer Model]: images/test2.svg
  [figure\_title]: #fig-intro-layer-model
