Chapter 30. Glossary
====================

The following terms are used in dCache.


**The dcache.conf File**
------------------------
This is the primary configuration file of a dCache. It is located at **/etc/dcache/dcache.conf.**

The **dcache.conf** file is initially empty. If one of the default configuration values needs to be changed, copy the default setting of this value from one of the [properties files](#properties-files) in **/usr/share/dcache/defaults** to this file and update the value. 



**The layout File**
-------------------
The layout file is located in the directory /etc/dcache/layouts. It contains lists of the [domains](#domain) and the services that are to be run within these domains. 
The properties Files

The properties files are located in the directory /usr/share/dcache/defaults. They contain the default settings of the dCache. 
    
    
Chimera
-------
The Chimera namespace is a core component of dCache. It maps each stored file to a unique identification number and allows storing of metadata against either files or directories.

Chimera includes some features like [levels](file-level), [directory tags](#directory-tag) and many of the [dot commands](#dot-command). 
    
    
Chimera ID
----------
A [Chimera](#chimera) ID is a 36 hexadecimal digit that uniquely defines a file or directory. 
    
    
Domain
------
A domain is a collection of one or more [cells](#cell) that provide a set of related services within a dCache instance. Each domain requires its own Java Virtual Machine. A typical domain might provide external connectivity (i.e., a [door](#door)) or manage the [pools](#pool) hosted on a machine.

Each domain has at least one cell, called the System cell and many tunnel cells for communicating with other Domains. To provide a useful service, a [domain](#domain) will contain other cells that provide specific behaviour. 
    
    
Cell
----
A cell is a collection of Java threads that provide a discrete and simple service within dCache. Each cell is hosted within a domain.

Cells have an address derived from concatenating their name, the @ symbol and their containing domain name. 
    
    
Well Known Cell
----------------
A well-known [cell](#cell) is a cell that registers itself centrally. Within the admin interface, a well-known cell may be referred to by just its cell name. 
    
    
Door
----
Door is the generic name for special [cells](#cell) that provides the first point of access for end clients to communicate with a dCache instance. There are different door implementations (e.g., GSIdCap door and GridFTP door), allowing a dCache instance to support multiple communication protocols.

A door will (typically) bind to a well-known port number depending on the protocol the door supports. This allows for only a single door instance per machine for each protocol.

A door will typically identify which [pool](#pool) will satisfy the end user’s operation and redirect the client to the corresponding pool. In some cases this is not possible; for example, some protocols (such as GridFTP version 1) do not allow servers to redirect end-clients, in other cases pool servers may be behind a firewall, so preventing direct access. When direct end-client access is not possible, the door may act as a data proxy, streaming data to the client.

By default, each door is hosted in a dedicated [domain](#domain). This allows easy control of whether a protocol is supported from a particular machine. 
    
    
Java Virtual Machine (JVM)
--------------------------
Java programs are typically compiled into a binary form called Java byte-code. Byte-code is comparable to the format that computers understand native; however, no mainstream processor understands Java byte-code. Instead compiled Java programs typically require a translation layer for them to run. This translation layer is called a Java Virtual Machine (JVM). It is a standardised execution environment that Java programs may run within. A JVM is typically represented as a process within the host computer. 
    
    
tertiary storage system
-----------------------
A mass storage system which stores data and is connected to the dCache system. Each dCache pool will write files to it as soon as they have been completely written to the pool (if the pool is not configured as a [LFS](#large-file-store-lfs)). The tertiary storage system is not part of dCache. However, it is possible to connect any mass storage system as tertiary storage system to dCache via a simple interface. 
    
    
tape backend
------------
A [tertiary storage system](#tertiary-storage-system) which stores data on magnetic tapes. 
    
    
Hierarchical Storage Manager (HSM)
-----------------------------------
See [tertiary storage system](#tertiary-storage-system).
    
    
HSM Type
---------
The type of HSM which is connected to dCache as a [tertiary storage system](#tertiary-storage-system). The choice of the HSM type influences the communication between dCache and the HSM. Currently there are osm and enstore. osm is used for most HSMs (TSM, HPSS, ...). 
    
    
HSM Instance
-------------


Large File Store (LFS)
----------------------
A Large File Store is the name for a dCache instance that is acting as a filesystem independent to, or in cooperation with, an [HSM](#HSM) system. When dCache is acting as an LFS, files may be stored and later read without involving any HSM system.

Whether a dCache instance provides an LFS depends on whether there are [pools](#pool) configured to do so. The LFS option, specified for each pool within the [layout file](#the-layout-file), describes how that pool should behave. This option can take three possible values:


**none

the pool does not contribute to any LFS capacity. All newly written files are regarded precious and sent to the HSM backend. 

**precious

Newly create files are regarded as precious but are not scheduled for the HSM store procedure. Consequently, these file will only disappear from the pool when deleted in the [Chimera](#chimera) namespace. 


to store
--------
Copying a file from a dCache pool to the [tertiary storage system](#tertiary-storage-system). 
    
    
to restore
----------
Copying a file from the [tertiary storage system](#tertiary-storage-system) to one of the dCache pools. 
    
    
to stage
---------
See [to restore](#to-restore).
    
    
transfer
---------
Any kind of transfer performed by a dCache pool. There are [store](#store), [to restore](#to-restore), pool to pool (client and server), read, and write transfers. The latter two are client transfers.
See Also [mover](#mover).
    
    
mover
------
The process/thread within a [pool](#pool) which performs a [transfer](#transfer). Each pool has a limited number of movers that may be active at any time; if this limit is reached then further requests for data are queued.

In many protocols, end clients connect to a mover to transfer file contents. To support this, movers must speak the protocol the end client is using.
See Also [transfer](rf-glossary.md#transfer).
    
    
Location Manager
-----------------
The location manager is a [cell](#cell) that instructs a newly started [domains](#domain) to which domain they should connect. This allows domains to form arbitrary network topologies; although, by default, a dCache instance will form a star topology with the dCacheDomain domain at the centre. 
    
    
Pinboard
---------
The pinboard is a collection of messages describing events within dCache and is similar to a log file. Each [cell](#cell) will (typically) have its own pinboard. 
    
    
Breakeven Parameter
--------------------
The breakeven parameter has to be a positive number smaller than 1.0. It specifies the impact of the age of the least recently used file on space cost. It the LRU file is one week old, the space cost will be equal to (1 + breakeven). Note that this will not be true, if the breakeven parameter has been set to a value greater or equal to 1. 
    
    
least recently used (LRU) File
-------------------------------
The file that has not be requested for the longest. 
    
    
file level
-----------
In [Chimera](#chimera), each file can have up to eight independent contents; these file-contents, called levels, may be accessed independently. dCache will store some file metadata in levels 1 and 2, but dCache will not store any file data in Chimera. 
    
    
directory tag
--------------
[Chimera](#chimera) includes the concept of tags. A tag is a keyword-value pair associated with a directory. Subdirectories inherit tags from their parent directory. New values may be assigned, but tags cannot be removed. The [dot command](#dot-command) .(tag)(<foo>) may be used to read or write tag <foo>’s value. The dot command .(tags)() may be read for a list of all tags in that file’s subdirectory.

    More details on directory tags are given in [the section called “Directory Tags”](config-chimera.md#directory-tags). 
    
    
dot command
------------
To configure and access some of the special features of the [Chimera namespace](#chimera) , special files may be read, written to or created. These files all start with a dot (or period) and have one or more parameters after. Each parameter is contained within a set of parentheses; for example, the file .(tag)(<foo>) is the Chimera dot command for reading or writing the <foo> [directory tag](#) value.

Care must be taken when accessing a dot command from a shell. Shells will often expand parentheses so the filename must be protected against this; for example, by quoting the filename or by escaping the parentheses. 
    
    
Wormhole
---------
A wormhole is a feature of the Chimera namespace. A wormhole is a file that is accessible in all directories; however, the file is not returned when scanning a directory(e.g., using the ls command). 
    
    
Pool to Pool Transfer
----------------------
A pool-to-pool transfer is one where a file is transferred from one dCache [pool](#pool) to another. This is typically done to satisfy a read request, either as a load-balancing technique or because the file is not available on pools that the end-user has access. 


Storage Class
--------------
The storage class is a string of the form


          <StoreName>:<StorageGroup>@<type-of-storage-system>
        

    containing exactly one @-symbol.

        <StoreName>:<StorageGroup> is a string describing the storage class in a syntax which depends on the storage system.

        <type-of-storage-system> denotes the type of storage system in use.

        In general use <type-of-storage-system>=osm. 

    A storage class is used by a tertiary storage system to decide where to store the file (i.e. on which set of tapes). dCache can use the storage class for a similar purpose, namely to decide on which pools the file can be stored. 


Replica
--------
It is possible that dCache will choose to make a file accessible from more than one [pool](#pool) using a [pool-to-pool](#pool-to-pool) copy. If this happens, then each copy of the file is a replica.

A file is independent of which pool is storing the data whereas a replica is uniquely specified by the pnfs ID and the pool name it is stored on. 


Precious Replica
-----------------
A precious [replica](#replica) is a replica that should be stored on tape. 


Cached Replica
---------------
A cached [replica](#replica) is a replica that should not be stored on tape. 


Replica Manager
----------------
The replica manager keeps track of the number of [replicas](#replica) of each file within a certain subset of pools and makes sure this number is always within a specified range. This way, the system makes sure that enough versions of each file are present and accessible at all times. This is especially useful to ensure resilience of the dCache system, even if the hardware is not reliable. The replica manager cannot be used when the system is connected to a [tertiary storage system](#tertiary-storage-system). The activation and configuration of the replica manager is described [in Chapter 6, The replica Service (Replica Manager)](config-ReplicaManager.md). 


Storage Resource Manager (SRM)
------------------------------
An SRM provides a standardised webservice interface for managing a storage resource (e.g. a dCache instance). It is possible to reserve space, initiate file storage or retrieve, and replicate files to another SRM. The actual transfer of data is not done via the SRM itself but via any protocol supported by both parties of the transfer. Authentication and authorisation is done with the grid security infrastructure. dCache comes with an implementation of an SRM which can turn any dCache instance into a grid storage element. 


Billing/Accounting
------------------
Accounting information is either stored in a text file or in a PostgreSQL database by the billing cell usually started in the httpdDomain [domain](#domain). This is described in [Chapter 15, The billing Service](config-billing.md). 


Pool Manager
------------
The pool manager is the [cell](#cell) running in the dCacheDomain [domain](#domain). It is a central component of a dCache instance and decides which pool is used for an incoming request. 


Cost Module
------------
The cost module is a Java class responsible for combining the different types of cost associated with a particular operation; for example, if a file is to be stored, the cost module will combine the storage costs and CPU costs for each candidate target pool. The pool manager will choose the candidate pool with the least combined cost. 


Pool Selection Unit
--------------------
The pool selection unit is a Java class responsible for determining the set of candidate pools for a specific transaction. A detailed account of its configuration and behaviour is given in [the section called “The Pool Selection Mechanism”](config-PoolManager.md#the-pool-selection-mechanism). 


Pin Manager
-----------
The pin manager is a [cell](#cell) by default running in the utility [domain](#domain). It is a central service that can “pin” files to a pool for a certain time. It is used by the SRM to satisfy prestage requests. 


Space Manager
-------------
The (SRM) Space Manager is a [cell](#cell) by default running in the srm [domain](#domain). It is a central service that records reserved space on pools. A space reservation may be either for a specific duration or never expires. The Space Manager is used by the SRM to satisfy space reservation requests. 


Pool
----
A pool is a [cell](#cell) responsible for storing retrieved files and for providing access to that data. Data access is supported via [movers](#MOVER). A machine may have multiple pools, perhaps due to that machine’s storage being split over multiple partitions.

A pool must have a unique name and all pool cells on a particular machine are hosted in a domain that derives its name from the host machine’s name.

The list of directories that are to store pool data are found in definition of the pools in the [layout Files](#layout-files), which are located on the pool nodes. 


sweeper
-------
A sweeper is an activity located on a [pool](#pool). It is responsible for deleting files on the pool that have been marked for removal. Files can be marked for removal because their corresponding namespace entry has been deleted or because the local file is a [cache copy](#cache-copy) and more disk space is needed. 


HSM sweeper
------------
The HSM sweeper, if enabled, is a component that is responsible for removing files from the [HSM](#hsm) when the corresponding namespace entry has been removed. 


cost
-----
The pool manager determines the pool used for storing a file by calculating a cost value for each available pool. The pool with the lowest cost is used. The costs are calculated by the cost module as described in [the section called “Classic Partitions”](config-PoolManager.md'classic-partitions). The total cost is a linear combination of the performance cost and the space cost. I.e.,

    	    cost = ccf * performance_cost + scf * space_cost	  

    where ccf and scf are configurable with the command [set pool decision](reference.md#set-pool-decision). 


performance cost
----------------
See also [the section called “The Performance Cost”](config-PoolManager.md#the-performance-cost). 


space cost
-----------
See also [the section called “The Space Cost”](config-PoolManager.md#the-space-cost).   


