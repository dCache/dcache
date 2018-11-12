CHAPTER 9. FILE HOPPING
=======================


Table of Contents

* [File Hopping on arrival from outside dCache](#file-hopping-on-arrival-from-outside-dcache)  

    [File mode of replicated files](#file-mode-of-replicated-files)  
    [File Hopping managed by the PoolManager](#file-hopping-managed-by-the-poolmanager)  
    [File Hopping managed by the HoppingManager](#file-hopping-managed-by-the-hoppingmanager)  


File hopping is a collective term in dCache, summarizing the possibility of having files being transferred between dCache pools triggered by a variety of conditions. The most prominent examples are:

-   If a file is requested by a client but the file resides on a pool from which this client, by configuration, is not allowed to read data, the dataset is transferred to an “allowed” pool first.

-   If a pool encounters a steady high load, the system may, if configured, decide to replicate files to other pools to achieve an equal load distribution.  

-   HSM restore operations may be split into two steps. The first one reads data from tertiary storage to an “HSM connected” pool and the second step takes care that the file is replicated to a general read pool. Under some conditions this separation of HSM and non-HSM pools might become necessary for performance reasons.

-   If a dataset has been written into dCache it might become necessary to have this file replicated instantly. The reasons can be, to either have a second, safe copy, or to make sure that clients don't access the file for reading on the write pools.

FILE HOPPING ON ARRIVAL FROM OUTSIDE dCache
===========================================

*File Hopping on arrival* is a term, denoting the possibility of initiating a pool to pool transfer as the result of a file successfully arriving on a pool from some external client. Files restored from HSM or arriving on a pool as the result of a pool to pool transfer will not yet be forwarded.

Forwarding of incoming files can be enabled by setting the `pool.destination.replicate` property in the **/etc/dcache/dcache.conf** file or per pool in the layout file. It can be set to on, `PoolManager` or `HoppingManager`, where on does the same as `PoolManager`.

The pool is requested to send a `replicateFile` message to either the `PoolManager` or to the `HoppingManager`, if available. The different approaches are briefly described below and in more detail in the subsequent sections.

-   The `replicateFile` message is sent to the `PoolManager`. This happens for all files arriving at that pool from outside (no restore or p2p). No intermediate `HoppingManager` is needed. The restrictions are

    -   All files are replicated. No pre-selection, e.g. on the storage class can be done.

    -   The mode of the replicated file is determined by the destination pool and cannot be overwritten. See [the section called “File mode of replicated files”](#file-mode-of-replicated-files)

-   The `replicateFile` message is sent to the `HoppingManager`. The `HoppingManager` can be configured to replicate certain storage classes only and to set the mode of the replicated file according to rules. The file mode of the source file cannot be modified.

FILE MODE OF REPLICATED FILES
-----------------------------

The mode of a replicated file can either be determined by settings in the destination pool or by the `HoppingManager`. It can be `cached` or `precious`.

-   If the `PoolManager` is used for replication, the mode of the replicated file is determined by the destination pool. The default setting is `cached`.

-   If a `HoppingManager` is used for file replication, the mode of the replicated file is determined by the `HoppingManager` rule responsible for this particular replication. If the destination mode is set to `keep` in the rule, the mode of the destination pool determines the final mode of the replicated file.

FILE HOPPING MANAGED BY THE POOLMANAGER
---------------------------------------

To enable replication on arrival by the `PoolManager` set the property `pool.destination.replicate` to `PoolManager` for the particular pool

    [exampleDomain]
    [exampleDomain/pool]
    ...
    pool.destination.replicate=PoolManager

or for several pools in the **/etc/dcache/dcache.conf** file.

    ...
    pool.destination.replicate=PoolManager

File hopping configuration instructs a pool to send a `replicateFile` request to the `PoolManager` as the result of a file arriving on that pool from some external client. All arriving files will be treated the same. The `PoolManager` will process this transfer request by trying to find a matching link (Please find detailed information at [Chapter 7, The poolmanager Service](config-PoolManager.md).

It is possible to configure the CELL-POOLMNGR such that files are replicated from this pool to a special set of destination pools.

Example:
Assume that we want to have all files, arriving on pool `ocean` to be immediately replicated to a subset of read pools. This subset of pools is described by the poolgroup `ocean-copies`. No other pool is member of the poolgroup `ocean-copies`.
Other than that, files arriving at the pool `mountain` should be replicated to all read pools from which farm nodes on the `131.169.10.0/24` subnet are allowed to read.
The layout file defining the pools `ocean` and `mountain` should read like this: 

    [exampleDomain]
    [exampleDomain/pool]

    name=ocean
    path=/path/to/pool-ocean
    pool.wait-for-files=${path}/data
    pool.destination.replicate=PoolManager

    name=mountain
    path=/path/to/pool-mountain
    pool.wait-for-files=${path}/data
    pool.destination.replicate=PoolManager

In the layout file it is defined that all files arriving on the pools `ocean` or `mountain` should be replicated immediately. The following **PoolManager.conf** file contains instructions for the CELL-POOLMNGR how to replicate these files. Files arriving at the `ocean` pool will be replicated to the `ocean-copies` subset of the read pools and files arriving at the pool `mountain` will be replicated to all read pools from which farm nodes on the 131.169.10.0/24 subnet are allowed to read.

    #
    # define the units
    #
    psu create unit -protocol   */*
    psu create unit -net        0.0.0.0/0.0.0.0
    psu create unit -net        131.169.10.0/255.255.255.0
    # create the faked net unit
    psu create unit -net        192.1.1.1/255.255.255.255
    psu create unit -store      *@*
    psu create unit -store      ocean:raw@osm
    #
    #
    #  define unit groups
    #
    psu create ugroup  any-protocol
    psu create ugroup  any-store
    psu create ugroup  ocean-copy-store
    psu create ugroup farm-network
    psu create ugroup ocean-copy-network
    #
    psu addto ugroup any-protocol */*
    psu addto ugroup any-store    *@*
    psu addto ugroup ocean-copy-store ocean:raw@osm
    psu addto ugroup farm-network  131.169.10.0/255.255.255.0
    psu addto ugroup ocean-copy-network  192.1.1.1/255.255.255.255
    psu addto ugroup allnet-cond 0.0.0.0/0.0.0.0
    psu addto ugroup allnet-cond 131.169.10.0/255.255.255.0
    psu addto ugroup allnet-cond 192.1.1.1/255.255.255.255
    #
    #
    #  define the write-pools
    #
    psu create pool ocean
    psu create pool mountain
    #
    #
    #  define the write-pools poolgroup
    #
    psu create pgroup write-pools
    psu addto pgroup write-pools ocean
    psu addto pgroup write-pools mountain
    #
    #
    #  define the write-pools-link, add write pools and set transfer preferences
    #
    psu create link write-pools-link any-store any-protocol allnet-cond
    psu addto link write-pools-link write-pools
    psu set link farm-read-link -readpref=0 -writepref=10 -cachepref=0 -p2ppref=-1
    #
    #
    #  define the read-pools
    #
    psu create pool read-pool-1
    psu create pool read-pool-2
    psu create pool read-pool-3
    psu create pool read-pool-4
    #
    #
    #  define the farm-read-pools poolgroup and add pool members
    #
    psu create pgroup farm-read-pools
    psu addto pgroup farm-read-pools read-pool-1
    psu addto pgroup farm-read-pools read-pool-2
    psu addto pgroup farm-read-pools read-pool-3
    psu addto pgroup farm-read-pools read-pool-4
    #
    #
    #  define the ocean-copy-pools poolgroup and add a pool
    #
    psu create pgroup ocean-copy-pools
    psu addto pgroup ocean-copy-pools  read-pool-1
    #
    #
    # define the farm-read-link, add farm-read-pools and set transfer preferences
    #
    psu create link farm-read-link any-store any-protocol farm-network
    psu addto link farm-read-link farm-read-pools
    psu set link farm-read-link -readpref=10 -writepref=0 -cachepref=10 -p2ppref=-1
    #
    #
    # define the ocean-copy-link, add ocean-copy-pools and set transfer preferences
    #
    psu create link ocean-copy-link ocean-copy-store any-protocol ocean-copy-network
    psu addto link ocean-copy-link ocean-copy-pools
    psu set link ocean-copy-link -readpref=10 -writepref=0 -cachepref=10 -p2ppref=-1
    #
    #

While `131.169.10.1` is a legal IP address e.g. of one of your farm nodes, the `192.1.1.1` IP address must not exist anywhere at your site.

FILE HOPPING MANAGED BY THE HOPPINGMANAGER
------------------------------------------

With the `HoppingManager` you have several configuration options for file `hopping on arrival`, e.g.:

-   With the `HoppingManager` you can define a rule such that only the files with a specific storage class should be replicated.
-   You can specify the protocol the replicated files can be accessed with.
-   You can specify from which ip-adresses it should be possible to access the files.

### Starting the FileHopping Manager service

Add the `hoppingManager` service to a domain in your layout file and restart the domain.

    [<DomainName>]
    [<DomainName>/hoppingmanager]

Initially no rules are configured for the HoppingManager. You may add rules by either edit the file **/var/lib/dcache/config/HoppingManager.conf** and restart the hoppingmanager service, or use the admin interface and `save` the modifications by the save command into the **HoppingManager.conf**



### Configuring pools to use the HoppingManager

To enable replication on arrival by the CELL-HOPMNGR set the property `pool.destination.replicate` to `HoppingManager` for the particular pool

    [exampleDomain]
    [exampleDomain/pool]
    ...
    pool.destination.replicate=HoppingManager

or for several pools in the **/etc/dcache/dcache.conf** file.

    ...
    pool.destination.replicate=HoppingManager

### HoppingManager Configuration Introduction

-   The `HoppingManager` essentially receives `replicateFile` messages from pools, configured to support file hopping, and either discards or modifies and forwards them to the `PoolManager`, depending on rules described below.

-   The `HoppingManager` decides on the action to perform, based on a set of configurable rules. Each rule has a name. Rules are checked in alphabetic order concerning their names.

-   A rule it triggered if the storage class matches the storage class pattern assigned to that rule. If a rule is triggered, it is processed and no further rule checking is performed. If no rule is found for this request the file is not replicated.

-   If for whatever reason, a file cannot be replicated, NO RETRY is being performed.

-   Processing a triggered rule can be :

    -   The message is discarded. No replication is done for this particular storage class.

    -   The rule modifies the `replicateFile` message, before it is forwarded to the `PoolManager`.

        An ip-number of a farm-node of the farm that should be allowed to read the file can be added to the `replicateFile` message.

        The mode of the replicated file can be specified. This can either be `precious`, `cached` or `keep`. `keep` means that the pool mode of the source pool determines the replicated file mode.

        The requested protocol can be specified.

### HoppingManager Configuration Reference

             define hop OPTIONS <name> <pattern> precious|cached|keep
                OPTIONS
                  -destination=<cellDestination> # default : PoolManager
                  -overwrite
                  -continue
                  -source=write|restore|*   #  !!!! for experts only      StorageInfoOptions
                  -host=<destinationHostIp>
                  -protType=dCap|ftp...
                  -protMinor=<minorProtocolVersion>
                  -protMajor=<majorProtocolVersion>

**name**  
This is the name of the hopping rule. Rules are checked in alphabetic order concerning their names.

**pattern**   
`pattern` is a [storage class](config-PoolManager.md#storage-classes) pattern. If the incoming storage class matches this pattern, this rule is processed.

**precious|cached|keep**  
`precious|cached|keep` determines the mode of the replicated file. With `keep` the mode of the file will be determined by the mode of the destination pool.

**-destination**  
This defines which `cell` to use for the pool to pool transfer. By default this is the CELL-POOLMNGR and this should not be changed.

**-overwrite**  
In case, a rule with the same name already exists, it is overwritten.

**-continue**  
If a rule has been triggered and the corresponding action has been performed, no other rules are checked. If the `continue` option is specified, rule checking continues. This is for debugging purposes only.

**-source**  
`-source` defines the event on the pool which has triggered the hopping. Possible values are `restore` and `write`. `restore` means that the rule should be triggered if the file was restored from a tape and `write` means that it should be triggered if the file was written by a client.

**-host**
Choose the id of a node of the farm of worker-nodes that should be allowed to access the file. Configure the POOLMNGR respectively.

**-protType, -protMajor, -protMinor**  
Specify the protocol which should be used to access the replicated files.

### HoppingManager configuration examples

In order to instruct a particular pool to send a `replicateFile` message to the HOPPINGMANAGER service, you need to add the line `pool.destination.replicate=HoppingManager` to the layout file.

    [exampleDomain]
    [exampleDomain/pool]

    name=write-pool
    path=/path/to/write-pool-exp-a
    pool.wait-for-files=${path}/data
    pool.destination.replicate=HoppingManager
    ...

Assume that all files of experiment-a will be written to an expensive write pool and subsequently flushed to tape. Now some of these files need to be accessed without delay. The files that need fast acceess possibility will be given the storage class `exp-a:need-fast-access@osm`.

In this example we will configure the file hopping such that a user who wants to access a file that has the above storage info with the NFSv4.1 protocol will be able to do so.

Define a rule for hopping in the **/var/lib/dcache/config/HoppingManager.conf ** file.

    define hop nfs-hop exp-a:need-fast-access@osm cached -protType=nfs -protMajor=4 -protMinor=1

This assumes that the storage class of the file is `exp-a:nfs@osm`. The mode of the file, which was `precious` on the write pool will have to be changed to `cached` on the read pool.

The corresponding **/var/lib/dcache/config/poolmanager.conf** file could read like this:

    #
    # define the units
    #
    psu create unit -protocol   */*
    psu create unit -net        0.0.0.0/0.0.0.0
    psu create unit -store      exp-a:need-fast-access@osm
    #
    #
    #  define unit groups
    #
    psu create ugroup  any-protocol
    psu create ugroup  exp-a-copy-store
    psu create ugroup allnet-cond
    #
    psu addto ugroup any-protocol */*
    psu addto ugroup exp-a-copy-store    exp-a:need-fast-access@osm
    psu addto ugroup allnet-cond 0.0.0.0/0.0.0.0
    #
    #
    #  define the write-pool
    #
    psu create pool write-pool
    #
    #
    #  define the read-pool
    #
    psu create pool read-pool
    #
    #
    #  define the exp-a-read-pools poolgroup and add a pool
    #
    psu create pgroup exp-a-read-pools
    psu addto pgroup exp-a-read-pools read-pool
    #
    #
    #  define the exp-a-write-pools poolgroup and add a pool
    #
    psu create pgroup exp-a-write-pools
    psu addto pgroup exp-a-write-pools write-pool
    #
    #
    # define the exp-a-read-link, add exp-a-read-pools and set transfer preferences
    #
    psu create link exp-a-read-link exp-a-copy-store any-protocol allnet-cond
    psu addto link exp-a-read-link exp-a-read-pools
    psu set link exp-a-read-link -readpref=10 -writepref=0 -cachepref=10 -p2ppref=-1
    #
    #
    # define the exp-a-write-link, add exp-a-write-pools and set transfer preferences
    #
    psu create link exp-a-write-link exp-a-copy-store any-protocol allnet-cond
    psu addto link exp-a-write-link exp-a-write-pools
    psu set link exp-a-write-link -readpref=0 -writepref=10 -cachepref=0 -p2ppref=-1
    #
    #
    #

  [section\_title]: #cf-hopping-onarrival-file-mode
  [???]: #cf-pm
  [storage class]: #secStorageClass
