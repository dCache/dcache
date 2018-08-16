CHAPTER 7. THE POOLMANAGER SERVICE
==================================

Table of Contents
------------------

* [The Pool Selection Mechanism](#the-pool-selection-mechanism)  
   
     [Links](#links)   
     [Examples](#examples)  
      
* [The Partition Manager](#the-partition-manager)  
  
     [Overview](#overview)  
     [Managing Partitions](#managing-partitions)  
     [Using Partitions](#using-partitions)  
     [Classic Partitions](#classic-partitions)  
     
* [Link Groups](#link-groups)  

The heart of a dCache System is the `poolmanager`. When a user performs an action on a file - reading or writing - a `transfer request` is sent to the dCache system. The `poolmanager` then decides how to handle this request.

If a file the user wishes to read resides on one of the storage-pools within the dCache system, it will be transferred from that pool to the user. If it resides on several pools, the file will be retrieved from one of the pools determined by a configurable load balancing policy. If all pools the file is stored on are busy, a new copy of the file on an idle pool will be created and this pool will answer the request.

A new copy can either be created by a `pool to pool transfer` (p2p) or by fetching it from a connected tertiary storage system (sometimes called HSM - hierarchical storage manager). Fetching a file from a tertiary storage system is called staging. It is also performed if the file is not present on any of the pools in the dCache system. The pool manager has to decide on which pool the new copy will be created, i.e. staged or p2p-copied.

The behaviour of the `poolmanager` service is highly configurable. In order to exploit the full potential of the software it is essential to understand the mechanisms used and how they are configured. The `poolmanager` service creates the `PoolManager` cell, which is a unique cell in dCache and consists of several sub-modules: The important ones are the `pool selection unit` (PSU) and the load balancing policy as defined by the partition manager (PM). 

The `poolmanager` can be configured by either directly editing the file **/var/lib/dcache/config/poolmanager.conf** or via the Admin Interface. Changes made via the [Admin Interface](intouch.md#admin-interface) will be saved in the file **/var/lib/dcache/config/poolmanager.conf** by the `save` command. This file will be parsed, whenever the dCache starts up. It is a simple text file containing the corresponding Admin Interface commands. It can therefore also be edited before the system is started. It can also be loaded into a running system with the `reload` command. In this chapter we will describe the commands allowed in this file.



THE POOL SELECTION MECHANISM
============================

The PSU is responsible for finding the set of pools which can be used for a specific transfer-request. By telling the PSU which pools are permitted for which type of transfer-request, the administrator of the dCache system can adjust the system to any kind of scenario: Separate organizations served by separate pools, special pools for writing the data to a tertiary storage system, pools in a DMZ which serves only a certain kind of data (e.g., for the grid). This section explains the mechanism employed by the PSU and shows how to configure it with several examples.

The PSU generates a list of allowed storage-pools for each incoming transfer-request. The PSU configuration described below tells the PSU which combinations of transfer-request and storage-pool are allowed. Imagine a two-dimensional table with a row for each possible transfer-request and a column for each pool - each field in the table containing either “yes” or “no”. For an incoming transfer-request the PSU will return a list of all pools with “yes” in the corresponding row.

Instead of “yes” and “no” the table really contains a *preference* - a non-negative integer. However, the PSU configuration is easier to understand if this is ignored.

Actually maintaining such a table in memory (and as user in a configuration file) would be quite inefficient, because there are many possibilities for the transfer-requests. Instead, the PSU consults a set of rules in order to generate the list of allowed pools. Each such rule is called a link because it links a set of transfer-requests to a group of pools.

LINKS
-----

A link consists of a set of unit groups and a list of pools. If all the unit groups are matched, the pools belonging to the link are added to the list of allowable pools.

A link is defined in the file **/var/lib/dcache/config/poolmanager.conf** by

      psu create link <link> <unitgroup>
      psu set link <link> -readpref=<rpref> -writepref=<wpref> -cachepref=<cpref> -p2ppref=<ppref>
      psu add link <link> <poolgroup> 

For the preference values see the [section called “Preference Values for Type of Transfer”](#preference-values-for-type-of-transfer).

The main task is to understand how the unit groups in a link are defined. After we have dealt with that, the preference values will be discussed and a few examples will follow.

The four properties of a transfer request, which are relevant for the PSU, are the following:


**Location of the File**

The location of the file in the file system is not used directly. Each file has the following two properties which can be set per directory:

-   **Storage Class.** The storage class is a string. It is used by a tertiary storage system to decide where to store the file (i.e. on which set of tapes) and dCache can use the storage class for a similar purpose (i.e. on which pools the file can be stored.). A detailed description of the syntax and how to set the storage class of a directory in the namespace is given in [the section called “Storage Classes”](#storage-classes).

-   **Cache Class.** The cache class is a string with essentially the same functionality as the storage class, except that it is not used by a tertiary storage system. It is used in cases, where the storage class does not provide enough flexibility. It should only be used, if an existing configuration using storage classes does not provide sufficient flexibility.

**IP Address**  
The IP address of the requesting host.

**Protocol / Type of Door**  
The protocol respectively the type of door used by the transfer.

**Type of Transfer**   
The type of transfer is either read, write, p2p request or cache.

A request for reading a file which is not on a read pool will trigger a p2p request and a subsequent read request. These will be treated as two separate requests.

A request for reading a file which is not stored on disk, but has to be staged from a connected tertiary storage system will trigger a `cache` request to fetch the file from the tertiary storage system and a subsequent `read` request. These will be treated as two separate requests.

Each link contains one or more `unit groups`, all of which have to be matched by the transfer request. Each unit group in turn contains several `units`. The unit group is matched if at least one of the units is matched.

### Types of Units

There are four types of units: network (`-net`), protocol (`-protocol`), storage class (`-store`) and cache class (`-dcache`) units. Each type imposes a condition on the IP address, the protocol, the storage class and the cache class respectively.

For each transfer at most one of each of the four unit types will match. If more than one unit of the same type could match the request then the most restrictive unit matches.

The unit that matches is selected from all units defined in dCache, not just those for a particular unit group. This means that, if a unit group has a unit that could match a request but this request also matches a more restrictive unit defined elsewhere then the less restrictive unit will not match.

**Network Unit**  
A *network unit* consists of an IP address and a net mask written as <IP-address>/<net mask>, say 111.111.111.0/255.255.255.0. It is satisfied, if the request is coming from a host with IP address within the subnet given by the address/netmask pair.

    psu create ugroup <name-of-unitgroup>
    psu create unit -net <IP-address>/<net mask>
    psu addto ugroup <name-of-unitgroup> <IP-address>/<net mask>

**Protocol Unit**   
A *protocol unit* consists of the name of the protocol and the version number written as protocol-name/version-number, e.g., `xrootd/3`.

    psu create ugroup <name-of-unitgroup>
    psu create unit -protocol <protocol-name>/<version-number>
    psu addto ugroup <name-of-unitgroup> <protocol-name>/<version-number>

**Storage Class Unit**  
A *storage class* unit is given by a storage class. It is satisfied if the requested file has this storage class. Simple wild cards are allowed: for this it is important to know that a storage class must always contain exactly one @-symbol as will be explained in [the section called “Storage Classes”](#storage-classes). In a storage class unit, either the part before the @-symbol or both parts may be replaced by a *-symbol; for example, *@osm and *@* are both valid storage class units whereas `something@*` is invalid. The *-symbol represents a limited wildcard: any string that doesn’t contain an @-symbol will match.

    psu create ugroup <name-of-unitgroup>
    psu create unit -store <StoreName>:<StorageGroup>@<type-of-storage-system>
    psu addto ugroup <name-of-unitgroup> <StoreName>:<StorageGroup>@<type-of-storage-system>

**Cache Class Unit**  
A *cache class unit* is given by a cache class. It is satisfied, if the cache class of the requested file agrees with it.

    psu create ugroup <name-of-unitgroup>
    psu create unit -dcache <name-of-cache-class>
    psu addto ugroup <name-of-unitgroup> <name-of-cache-class>

### Preference Values for Type of Transfer

The conditions for the *type of transfer* are not specified with units. Instead, each link contains four attributes `-readpref`, `-writepref`, `-p2ppref` and `-cachepref`, which specify a preference value for the respective types of transfer. If all the unit groups in the link are matched, the corresponding preference is assigned to each pool the link points to. Since we are ignoring different preference values at the moment, a preference of `0` stands for `no` and a non-zero preference stands for `yes`. A negative value for `-p2ppref` means, that the value for `-p2ppref` should equal the one for the `-readpref`.

#### Multiple non-zero Preference Values

> **NOTE**
>
> This explanation of the preference values can be skipped at first reading. It will not be relevant, if all non-zero preference values are the same. If you want to try configuring the pool manager right now without bothering about the preferences, you should only use `0` (for `no`) and, say, `10` (for `yes`) as preferences. You can choose `-p2ppref=-1` if it should match the value for `-readpref`. The first examples below are of this type.

If several different non-zero preference values are used, the PSU will not generate a single list but a set of lists, each containing pools with the same preference. The Pool Manager will use the list of pools with highest preference and select a pool according to the load balancing policy for the transfer. Only if all pools with the highest preference are offline, the next list will be considered by the Pool Manager. This can be used to configure a set of fall-back pools which are used if none of the other pools are available.

### Pool Groups

Pools can be grouped together to pool groups.

    psu create pgroup <name-of-poolgroup>
    psu create pool <name-of-pool>
    psu addto pgroup <name-of-poolgroup> <name-of-pool>

Example:  

Consider a host `pool1` with two pools, `pool1_1` and `pool1_2`, and a host `pool2` with one pool `pool2_1`. If you want to treat them in the same way, you would create a pool group and put all of them in it:

    psu create pgroup normal-pools  
    psu create pool pool1_1  
    psu addto pgroup normal-pools pool1_1  
    psu create pool pool1_2  
    psu addto pgroup normal-pools pool1_2  
    psu create pool pool2_1  
    psu addto pgroup normal-pools pool2_1  

If you later want to treat `pool1_2` differently from the others, you would remove it from this pool group and add it to a new one:

    psu removefrom pgroup normal-pools pool1_2  
    psu create pgroup special-pools  
    psu addto pgroup special-pools pool1_2  

In the following, we will assume that the necessary pool groups already exist. All names ending with `-pools` will denote pool groups.

Note that a pool-node will register itself with the `PoolManager:` The pool will be created within the PSU and added to the pool group `default`, if that exists. This is why the dCache system will automatically use any new pool-nodes in the standard configuration: All pools are in `default` and can therefore handle any request.

### Storage Classes

The storage class is a string of the form `StoreName:StorageGroup@type-of-storage-system`, where `type-of-storage-system` denotes the type of storage system in use, and `StoreName`:`StorageGroup` is a string describing the storage class in a syntax which depends on the storage system. In general use `type-of-storage-system=osm`.

Consider for example the following setup:

    Example:    
    
    [root] # /usr/bin/chimera lstag /data/experiment-a  
    Total: 2  
    OSMTemplate  
    sGroup  
    [root] # /usr/bin/chimera readtag /data/experiment-a OSMTemplate  
    StoreName myStore  
    [root] # /usr/bin/chimera readtag /data/experiment-a sGroup  
    STRING  

This is the setup after a fresh installation and it will lead to the storage class `myStore:STRING@osm`. An adjustment to more sensible values will look like

    [root] # /usr/bin/chimera writetag /data/experiment-a OSMTemplate "StoreName exp-a"  
    [root] # /usr/bin/chimera writetag /data/experiment-a sGroup "run2010"  

and will result in the storage class `exp-a:run2010@osm` for any data stored in the `/data/experiment-a` directory.

To summarize: The storage class depends on the directory the data is stored in and is configurable.

### Cache Class

Storage classes might already be in use for the configuration of a tertiary storage system. In most cases they should be flexible enough to configure the PSU. However, in rare cases the existing configuration and convention for storage classes might not be flexible enough.

Consider for example a situation, where data produced by an experiment always has the same storage class `exp-a:alldata@osm`. This is good for the tertiary storage system, since all data is supposed to go to the same tape set sequentially. However, the data also contains a relatively small amount of meta-data, which is accessed much more often by analysis jobs than the rest of the data. You would like to keep the meta-data on a dedicated set of dCache pools. However, the storage class does not provide means to accomplish that.

The cache class of a directory is set by the tag `cacheClass` as follows:

    Example:  
    
    [root] # /usr/bin/chimera writetag /data/experiment-a cacheClass "metaData"  

    In this example the meta-data is stored in directories which are tagged in this way.  

Check the existing tags of a directory and their content by:  

    [root] # /usr/bin/chimera lstag /path/to/directory    
    Total: numberOfTags    
    tag1  
    tag2  
    ...  
    [root] # /usr/bin/chimera readtag /path/to/directory tag1  
    contentOfTag1  

> **NOTE**
>
> A new directory will inherit the tags from the parent directory. But updating a tag will *not* update the tags of any child directories.

### Define a link

Now we have everything we need to define a link.

    psu create ugroup <name-of-unitgroup>
    psu create unit - <type> <unit>
    psu addto ugroup <name-of-unitgroup> <unit>

    psu create pgroup <poolgroup>
    psu create pool <pool>
    psu addto pgroup <poolgroup> <pool>

    psu create link <link> <name-of-unitgroup>
    psu set link <link> -readpref=<10> -writepref=<0> -cachepref=<10>-p2ppref=<-1>
    psu add link <link>  <poolgroup>
    
[return to top](#the-pool-selection-mechanism)

EXAMPLES
--------

Find some examples for the configuration of the PSU below.

### Separate Write and Read Pools

The dCache we are going to configure receives data from a running experiment, stores the data onto a tertiary storage system, and serves as a read cache for users who want to analyze the data. While the new data from the experiment should be stored on highly reliable and therefore expensive systems, the cache functionality may be provided by inexpensive hardware. It is therefore desirable to have a set of pools dedicated for writing the new data and a separate set for reading.

Example:  

The simplest configuration for such a setup would consist of two links “write-link” and “read-link”. The configuration is as follows:

    psu create pgroup read-pools
    psu create pool pool1
    psu addto pgroup read-pools pool1
    psu create pgroup write-pools
    psu create pool pool2
    psu addto pgroup write-pools pool2

    psu create unit -net 0.0.0.0/0.0.0.0
    psu create ugroup allnet-cond
    psu addto ugroup allnet-cond 0.0.0.0/0.0.0.0

    psu create link read-link allnet-cond 
    psu set link read-link -readpref=10 -writepref=0 -cachepref=10
    psu add link read-link read-pools 

    psu create link write-link allnet-cond
    psu set link write-link -readpref=0 -writepref=10 -cachepref=0
    psu add link write-link write-pools

Why is the unit group `allnet-cond` necessary? It is used as a condition which is always true in both links. This is needed, because each link must contain at least one unit group.

[return to top](#the-pool-selection-mechanism)

### Restricted Access by IP Address

You might not want to give access to the pools for the whole network, as in the previous example ([the section called “Separate Write and Read Pools”](#separate-write-and-read-pools)), though.

Example:
Assume, the experiment data is copied into the cache from the hosts with IP `111.111.111.201`, `111.111.111.202`, and `111.111.111.203`. As you might guess, the subnet of the site is `111.111.111.0/255.255.255.0`. Access from outside should be denied. Then you would modify the above configuration as follows:

    
    psu create pgroup read-pools
    psu create pool pool1
    psu addto pgroup read-pools pool1
    psu create pgroup write-pools
    psu create pool pool2
    psu addto pgroup write-pools pool2

    psu create unit -store *@*

    psu create unit -net 111.111.111.0/255.255.255.0
    psu create unit -net 111.111.111.201/255.255.255.255
    psu create unit -net 111.111.111.202/255.255.255.255
    psu create unit -net 111.111.111.203/255.255.255.255

    psu create ugroup write-cond
    psu addto ugroup write-cond 111.111.111.201/255.255.255.255
    psu addto ugroup write-cond 111.111.111.202/255.255.255.255
    psu addto ugroup write-cond 111.111.111.203/255.255.255.255

    psu create ugroup read-cond
    psu addto ugroup read-cond 111.111.111.0/255.255.255.0
    psu addto ugroup read-cond 111.111.111.201/255.255.255.255
    psu addto ugroup read-cond 111.111.111.202/255.255.255.255
    psu addto ugroup read-cond 111.111.111.203/255.255.255.255

    psu create link read-link read-cond
    psu set link read-link -readpref=10 -writepref=0 -cachepref=10
    psu add link read-link read-pools

    psu create link write-link write-cond
    psu set link write-link -readpref=0 -writepref=10 -cachepref=0
    psu add link write-link write-pools

> **IMPORTANT**
>
> For a given transfer exactly zero or one storage class unit, cache class unit, net unit and protocol unit will match. As always the most restrictive one will match, the IP `111.111.111.201` will match the `111.111.111.201/255.255.255.255` unit and not the `111.111.111.0/255.255.255.0` unit. Therefore if you only add `111.111.111.0/255.255.255.0` to the unit group “read-cond”, the transfer request coming from the IP `111.111.111.201` will only be allowed to write and not to read. The same is true for transfer requests from `111.111.111.202` and `111.111.111.203`.


### Reserving Pools for Storage and Cache Classes

If pools are financed by one experimental group, they probably do not like it if they are also used by another group. The best way to restrict data belonging to one experiment to a set of pools is with the help of storage class conditions. If more flexibility is needed, cache class conditions can be used for the same purpose.

Example:  

Assume, data of experiment A obtained in 2010 is written into subdirectories in the namespace tree which are tagged with the storage class `exp-a:run2010@osm`, and similarly for the other years. (How this is done is described in [the section called “Storage Classes”](#storage-classes).) Experiment B uses the storage class `exp-b:alldata@osm` for all its data. Especially important data is tagged with the cache class `important`. (This is described in [the section called “Cache Class”](#cache-class).) A suitable setup would be:   



      psu create pgroup exp-a-pools
      psu create pool pool1
      psu addto pgroup exp-a-pools pool1

      psu create pgroup exp-b-pools
      psu create pool pool2
      psu addto pgroup exp-b-pools pool2

      psu create pgroup exp-b-imp-pools
      psu create pool pool3
      psu addto pgroup exp-b-imp-pools pool3

      psu create unit -net 111.111.111.0/255.255.255.0
      psu create ugroup allnet-cond
      psu addto ugroup allnet-cond 111.111.111.0/255.255.255.0

      psu create ugroup exp-a-cond
      psu create unit -store exp-a:run2011@osm
      psu addto ugroup exp-a-cond exp-a:run2011@osm
      psu create unit -store exp-a:run2010@osm
      psu addto ugroup exp-a-cond exp-a:run2010@osm

      psu create link exp-a-link allnet-cond exp-a-cond
      psu set link exp-a-link -readpref=10 -writepref=10 -cachepref=10
      psu add link exp-a-link exp-a-pools

      psu create ugroup exp-b-cond
      psu create unit -store exp-b:alldata@osm
      psu addto ugroup exp-b-cond exp-b:alldata@osm

      psu create ugroup imp-cond
      psu create unit -dcache important
      psu addto ugroup imp-cond important

      psu create link exp-b-link allnet-cond exp-b-cond  
      psu set link exp-b-link -readpref=10 -writepref=10 -cachepref=10  
      psu add link exp-b-link exp-b-pools  

      psu create link exp-b-imp-link allnet-cond exp-b-cond imp-cond
      psu set link exp-b-imp-link -readpref=20 -writepref=20 -cachepref=20
      psu add link exp-b-link exp-b-imp-pools



Data tagged with cache class “`important`” will always be written and read from pools in the pool group `exp-b-imp-pools`, except when all pools in this group cannot be reached. Then the pools in `exp-a-pools` will be used.
Note again that these will never be used otherwise. Not even, if all pools in `exp-b-imp-pools` are very busy and some pools in `exp-a-pools` have nothing to do and lots of free space.

The central IT department might also want to set up a few pools, which are used as fall-back, if none of the pools of the experiments are functioning. These will also be used for internal testing. The following would have to be added to the previous setup:

Example:  

    psu create pgroup it-pools  
    psu create pool pool_it  
    psu addto pgroup it-pools pool_it  

    psu create link fallback-link allnet-cond  
    psu set link fallback-link -readpref=5 -writepref=5 -cachepref=5  
    psu add link fallback-link it-pools  

Note again that these will only be used, if none of the experiments pools can be reached, or if the storage class is not of the form `exp-a:run2009@osm`, `exp-a:run2010@osm`, or `exp-b:alldata@osm`. If the administrator fails to create the unit `exp-a:run2005@osm` and add it to the unit group `exp-a-cond`, the fall-back pools will be used eventually.

THE PARTITION MANAGER
=====================

The partition manager defines one or more load balancing policies. Whereas the PSU produces a prioritized set of candidate pools using a collection of rules defined by the administrator, the load balancing policy determines the specific pool to use. It is also the load balancing policy that determines when to fall back to lesser prirority links, or when to trigger creation of additional copies of a file.

Since the load balancing policy and parameters are defined per partition, understanding the partition manager is essential to tuning load balancing. This does not imply that one has to partition the dCache instance. It is perfectly valid to use a single partition for the complete instance.

This section documents the use of the partition manager, how to create partitions, set parameters and how to associate links with partitions. In the following sections the available partition types and their configuration parameters are described.

OVERVIEW
--------

There are various parameters that affect the load balancing policy. Some of them are generic and apply to any load balancing policy, but many are specific to a particular policy. To avoid limiting the complete dCache instance to a single configuration, the choice of load balancing policy and the various parameters apply to partitions of the instance. The load balancing algorithm and the available parameters is determined by the partition type.

Each PSU link can be associated with a different partion and the policy and parameters of that partition will be used to choose a pool from the set of candidate pools. The only partition that exists without being explicitly created is the partition called `default`. This partition is used by all links that do not explicitly identify a partition. Other partitions can be created or modified as needed.

The `default` partition has a hard-coded partition type called `classic`. This type implements the one load balancing policy that was available in dCache before version 2.0. The `classic` partition type is described later. Other partitions have one of a number of available types. The system is pluggable, meaning that third party plugins can be loaded at runtime and add additional partition types, thus providing the ultimate control over load balancing in dCache. Documentation on how to develop plugins is beyond the scope of this chapter.

To ease the management of partition parameters, a common set of shared parameters can be defined outside all partitions. Any parameter not explicitly set on a partition inherits the value from the common set. If not defined in the common set, a default value determined by the partition type is used. Currently, the common set of parameters happens to be the same as the parameters of the `default` partition, however this is only due to compatibility constraints and may change in future versions.

MANAGING PARTITIONS
-------------------

For each partition you can choose the load balancing policy. You do this by chosing the type of the partition.

Currently four different partition types are supported:

**classic**:    
This is the pool selection algorithm used in the versions of dCache prior to version 2.0. See [the section called “Classic Partitions”](#classic-partitions) for a detailed description.

**random**:  
This pool selection algorithm selects a pool randomly from the set of available pools.

**lru**:  
This pool selection algorithm selects the pool that has not been used the longest.

**wass**:  
This pool selection algorithm selects pools randomly weighted by available space, while incorporating age and amount of garbage collectible files and information about load.

This is the partition type of the default partition. See [How to Pick a Pool](https://www.dcache.org/articles/wass.html) for more details.

Commands related to dCache partitioning:

-   `pm types` 

Lists available partition types. New partition types can be added through plugins.


-   `pm create` 

[-type=partitionType] partitionName
Creates a new partition. If no partition type is specified, then a `wass` partition is created.


-   `pm set`  [partitionName] -parameterName =value|off
Sets a parameter `parameterName` to a new value.

    If `partitionName` is omitted, the common shared set of parameters is updated. The value is used by any partition for which the parameter is not explicitly set.

If a parameter is set to off then this parameter is no longer defined and is inherited from the common shared set of parameters, or a partition type specific default value is used if the parameter is not defined in the common set.

-   `pm ls`  [-l] [partitionName]
Lists a single or all partitions, including the type of each partition. If a partition name or the `-l` option are used, then the partition parameters are shown too. Inherited and default values are identified as such.

-   `pm destroy` partitionName
Removes a partition from dCache. Any links configured to use this partition will fall back to the `default` partition.


USING PARTITIONS
----------------

A partition, so far, is just a set of parameters which may or may not differ from the default set. To let a partition relate to a part of the dCache, links are used. Each link may be assigned to exactly one partition. If not set, or the assigned partition doesn't exist, the link defaults to the `default` partition.

psu set link [linkName] -section= partitionName [other-options...]

Whenever this link is chosen for pool selection, the associated parameters of the assigned partition will become active for further processing.

> **WARNING**
>
> Depending on the way links are setup it may very well happen that more than just one link is triggered for a particular dCache request. This is not illegal but leads to an ambiguity in selecting an appropriate dCache partition. If only one of the selected links has a partition assigned, this partition is chosen. Otherwise, if different links point to different partitions, the result is indeterminate. This issue is not yet solved and we recommend to clean up the poolmanager configuration to eliminate links with the same preferences for the same type of requests.

In the [Web Interface](intouch.md#the-web-interface-for-monitoring-dcache) you can find a web page listing partitions and more information. You will find a page summarizing the partition status of the system. This is essentially the output of the command `pm ls -l`.

Example:  

For your dCache on dcache.example.org the address is
http://dcache.example.org:2288/poolInfo/parameterHandler/set/matrix/*


### Examples   

For the subsequent examples we assume a basic poolmanager setup :

    #
    # define the units
    #
    psu create unit -protocol   */*
    psu create unit -protocol   xrootd/*
    psu create unit -net        0.0.0.0/0.0.0.0
    psu create unit -net        131.169.0.0/255.255.0.0
    psu create unit -store      *@*
    #
    #  define unit groups
    #
    psu create ugroup  any-protocol
    psu create ugroup  any-store
    psu create ugroup  world-net
    psu create ugroup  xrootd
    #
    psu addto ugroup any-protocol */*
    psu addto ugroup any-store    *@*
    psu addto ugroup world-net    0.0.0.0/0.0.0.0
    psu addto ugroup desy-net     131.169.0.0/255.255.0.0
    psu addto ugroup xrootd       xrootd/*
    #
    #  define the pools
    #
    psu create pool pool1
    psu create pool pool2
    psu create pool pool3
    psu create pool pool4

    #
    #  define the pool groups
    #
    psu create pgroup default-pools
    psu create pgroup special-pools
    #
    psu addto pgroup default-pools pool1
    psu addto pgroup default-pools pool2
    #
    psu addto pgroup special-pools pool3
    psu addto pgroup special-pools pool4
    #

#### Disallowing pool to pool transfers for special pool groups based on the access protocol

For a special set of pools, where we only allow the xrootd protocol, we don't want the datasets to be replicated on high load while for the rest of the pools we allow replication on hot spot detection.

    #
    pm create xrootd-section
    #
    pm set default        -p2p=0.4
    pm set xrootd-section -p2p=0.0
    #
    psu create link default-link any-protocol any-store world-net
    psu add    link default-link default-pools
    psu set    link default-link -readpref=10 -cachepref=10 -writepref=0
    #
    psu create link xrootd-link xrootd any-store world-net
    psu add    link xrootd-link special-pools
    psu set    link xrootd-link -readpref=10 -cachepref=10 -writepref=0
    psu set    link xrootd-link -section=xrootd-section
    #        

#### Choosing pools randomly for incoming traffic only

For a set of pools we select pools following the default setting of cpu and space related cost factors. For incoming traffic from outside, though, we select the same pools, but in a randomly distributed fashion. Please note that this is not really a physical partitioning of the dCache system, but rather a virtual one, applied to the same set of pools.

    #
    pm create incoming-section
    #
    pm set default          -cpucostfactor=0.2 -spacecostfactor=1.0
    pm set incoming-section -cpucostfactor=0.0 -spacecostfactor=0.0
    #
    psu create link default-link any-protocol any-store desy-net
    psu add    link default-link default-pools
    psu set    link default-link -readpref=10 -cachepref=10 -writepref=10
    #
    psu create link default-link any-protocol any-store world-net
    psu add    link default-link default-pools
    psu set    link default-link -readpref=10 -cachepref=10 -writepref=0
    #
    psu create link incoming-link any-protocol any-store world-net
    psu add    link incoming-link default-pools
    psu set    link incoming-link -readpref=10 -cachepref=10 -writepref=10
    psu set    link incoming-link -section=incoming-section
    #

CLASSIC PARTITIONS
------------------

The `classic` partition type implements the load balancing policy known from dCache releases before version 2.0. This partition type is still the default. This section describes this load balancing policy and the available configuration parameters.

Example:   

To create a classic partition use the command: `pm create` -type=classic  <partitionName>

### Load Balancing Policy

From the allowable pools as determined by the [pool selection unit](rf-glossary.md#pool-selection-unit), the pool manager determines the pool used for storing or reading a file by calculating a [cost](rf-glossary.md#cost) value for each pool. The pool with the lowest cost is used.

If a client requests to read a file which is stored on more than one allowable pool, [the performance costs](rf-glossary.md#the-performance_costs) are calculated for these pools. In short, this cost value describes how much the pool is currently occupied with transfers.

If a pool has to be selected for storing a file, which is either written by a client or [restored](rf-glossary.md#restored) from a [tape backend](rf-glossary.md#tape_backend), this performance cost is combined with a [space cost](rf-glossary.md#space_cost) value to a [total cost](rf-glossary.md#total-cost) value for the decision. The space cost describes how much it “hurts” to free space on the pool for the file.

The [cost module](rf-glossary.md#cost-module) is responsible for calculating the cost values for all pools. The pools regularly send all necessary information about space usage and request queue lengths to the cost module. It can be regarded as a cache for all this information. This way it is not necessary to send “get cost” requests to the pools for each client request. The cost module interpolates the expected costs until a new precise information package is coming from the pools. This mechanism prevents clumping of requests.

Calculating the cost for a data transfer is done in two steps. First, the cost module merges all information about space and transfer queues of the pools to calculate the performance and space costs separately. Second, in the case of a write or stage request, these two numbers are merged to build the total cost for each pool. The first step is isolated within a separate loadable class. The second step is done by the partition.

### The Performance Cost

The load of a pool is determined by comparing the current number of active and waiting transfers to the maximum number of concurrent transfers allowed. This is done separately for each of the transfer types (store, restore, pool-to-pool client, pool-to-pool server, and client request) with the following equation:

perfCost(per Type) = ( activeTransfers + waitingTransfers ) / maxAllowed .

The maximum number of concurrent transfers (`maxAllowed`) can be configured with the commands [store](rf-glossary.md#to-store),[restore](rf-glossary.md#to-restore), pool-to-pool client, pool-to-pool server, and client request) with the following equation:.

perfCost(per Type) = ( activeTransfers + waitingTransfers ) / maxAllowed .

The maximum number of concurrent transfers (maxAllowed) can be configured with the commands [st set max active](reference.md#st-set-max-active) (store), [rh set max active](reference.md#rh-set-max-active) (restore), [mover set max] activereference.md#mover-set-max) (client request), [mover set max active -queue=p2p](reference.md#mover-set-max-active-queue-p2p) (pool-to-pool server), and [pp set max active](reference.md#pp-set-max-active) (pool-to-pool client).

Then the average is taken for each mover type where maxAllowed is not zero. For a pool where store, restore and client transfers are allowed, e.g.,

perfCost(total) = ( perfCost(store) + perfCost(restore) + perfCost(client) ) / 3 ,

and for a read only pool:

perfCost(total) = ( perfCost(restore) + perfCost(client) ) / 2 .

For a well balanced system, the performance cost should not exceed 1.0.

### The Space Cost

In this section only the new scheme for calculating the space cost will be described. Be aware, that the old scheme will be used if the [breakeven parameter](rf-glossary.md#breakeven-parameter) of a pool is larger or equal 1.0.

The cost value used for determining a pool for storing a file depends either on the free space on the pool or on the age of the [least recently used (LRU) file](rf-glossary.md#least-recently-used-lru-file), which whould have to be deleted.

The space cost is calculated as follows:

<table>
<colgroup>
<col width="23%" />
<col width="5%" />
<col width="29%" />
<col width="3%" />
<col width="29%" />
<col width="9%" />
</colgroup>
<tbody>
<tr class="odd">
<td>If</td>
<td>freeSpace &gt; gapPara</td>
<td></td>
<td></td>
<td>then</td>
<td>spaceCost = 3 * newFileSize / freeSpace</td>
</tr>
<tr class="even">
<td>If</td>
<td>freeSpace &lt;= gapPara</td>
<td>and</td>
<td>lruAge &lt; 60</td>
<td>then</td>
<td>spaceCost = 1 + costForMinute</td>
</tr>
<tr class="odd">
<td>If</td>
<td>freeSpace &lt;= gapPara</td>
<td>and</td>
<td>lruAge &gt;= 60</td>
<td>then</td>
<td>spaceCost = 1 + costForMinute * 60 / lruAge</td>
</tr>
</tbody>
</table>

where the variable names have the following meanings:

freeSpace  
The free space left on the pool

newFileSize  
The size of the file to be written to one of the pools, and at least 50MB.

lruAge  
The age of the [least recently used file](rf-glossary.md#least-recently-used-lru-file) on the pool.

gapPara  
The gap parameter. Default is 4 GiB. The size of free space below which it will be assumed that the pool is full and consequently the least recently used file has to be removed. If, on the other hand, the free space is greater than `gapPara`, it will be expensive to store a file on the pool which exceeds the free space.

It can be set per pool with the [set gap](reference.md#set-gap) command. This has to be done in the pool cell and not in the pool manager cell. Nevertheless it only influences the cost calculation scheme within the pool manager and not the bahaviour of the pool itself.

costForMinute  
A parameter which fixes the space cost of a one-minute-old LRU file to (1 + costForMinute). It can be set with the [set breakeven](reference.md#set-breakeven), where

costForMinute = breakeven \* 7 \* 24 \* 60.

I.e. the the space cost of a one-week-old LRU file will be (1 + breakeven). Note again, that all this only applies if breakeven &lt; 1.0

The prescription above can be stated a little differently as follows:

|     |                         |      |                                                             |
|-----|-------------------------|------|-------------------------------------------------------------|
| If  | freeSpace &gt; gapPara  | then | spaceCost = 3 \* newFileSize / freeSpace                    |
| If  | freeSpace &lt;= gapPara | then | spaceCost = 1 + breakeven \* 7 \* 24 \* 60 \* 60 / lruAge , |

where `newFileSize` is at least 50MB and `lruAge` at least one minute.

#### Rationale

As the last version of the formula suggests, a pool can be in two states: Either freeSpace &gt; gapPara or freeSpace &lt;= gapPara - either there is free space left to store files without deleting cached files or there isn't.

Therefore, `gapPara` should be around the size of the smallest files which frequently might be written to the pool. If files smaller than `gapPara` appear very seldom or never, the pool might get stuck in the first of the two cases with a high cost.

If the LRU file is smaller than the new file, other files might have to be deleted. If these are much younger than the LRU file, this space cost calculation scheme might not lead to a selection of the optimal pool. However, in pratice this happens very seldomly and this scheme turns out to be very efficient.

### The Total Cost

The total cost is a linear combination of the [performance](rf-glossary.md#performance-cost) and [space cost](rf-glossary.md#space-cost). I.e. totalCost = ccf \* perfCost + scf \* spaceCost , where `ccf` and `scf` are configurable with the command [set pool decision](reference.md#set-pool-decision). E.g.,

    (PoolManager) admin > set pool decision -spacecostfactor=3 -cpucostfactor=1

will give the [space cost](rf-glossary.md#space-cost) three times the weight of the [performance cost](rf-glossary.md#performance-cost).

### Parameters of Classic Partitions  

Classic partitions have a large number of tunable parameters. These parameters are set using the `pm set` command.

Example:  

To set the space cost factor on the `default` partition to `0.3`, use the following command:

                      pm set default -spacecostfactor=0.3
                  

| Command                                       | Meaning                                                                                                                                                                                                                                                                                                                 | Type    |
|-----------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------|
| `pm set` partitionName -spacecostfactor=scf   | Sets the `space cost factor` for the partition.                                                                                                                                                                                                                                                                         
                                                                                                                                                                                                                                                                                                                                                                          
                                                 The default value is `1.0`.                                                                                                                                                                                                                                                                                              | float   |
| `pm set` partitionName -cpucostfactor=ccf     | Sets the cpu cost factor for the partition.                                                                                                                                                                                                                                                                             
                                                                                                                                                                                                                                                                                                                                                                          
                                                 The default value is `1.0`.                                                                                                                                                                                                                                                                                              | float   |
| `pm set` partitionName -idle=idle-value       | The concept of the idle value will be turned on if idle-value &gt; `0.0`.                                                                                                                                                                                                                                               
                                                                                                                                                                                                                                                                                                                                                                          
                                                 A pool is idle if its performance cost is smaller than the idle-value. Otherwise it is not idle.                                                                                                                                                                                                                         
                                                                                                                                                                                                                                                                                                                                                                          
                                                 If one or more pools that satisfy the read request are idle then only one of them is chosen for a particular file irrespective of total cost. I.e. if the same file is requested more than once it will always be taken from the same pool. This allowes the copies on the other pools to age and be garbage collected.  
                                                                                                                                                                                                                                                                                                                                                                          
                                                 The default value is `0.0`, which disables this feature.                                                                                                                                                                                                                                                                 | float   |
| `pm set` partitionName -p2p=p2p-value         | Sets the static replication threshold for the partition.                                                                                                                                                                                                                                                                
                                                                                                                                                                                                                                                                                                                                                                          
                                                 If the performance cost on the best pool exceeds p2p-value and the value for [slope] = `0.0` then this pool is called hot and a pool to pool replication may be triggered.                                                                                                                                               
                                                                                                                                                                                                                                                                                                                                                                          
                                                 The default value is `0.0`, which disables this feature.                                                                                                                                                                                                                                                                 | float   |
| `pm set` partitionName -alert=value           | Sets the alert value for the partition.                                                                                                                                                                                                                                                                                 
                                                                                                                                                                                                                                                                                                                                                                          
                                                 If the best pool's performance cost exceeds the p2p value and the alert value then no pool to pool copy is triggered and a message will be logged stating that no pool to pool copy will be made.                                                                                                                        
                                                                                                                                                                                                                                                                                                                                                                          
                                                 The default value is `0.0`, which disables this feature.                                                                                                                                                                                                                                                                 | float   |
| `pm set` partitionName -panic=value           | Sets the panic cost cut level for the partition.                                                                                                                                                                                                                                                                        
                                                                                                                                                                                                                                                                                                                                                                          
                                                 If the performance cost of the best pool exceeds the panic cost cut level the request will fail.                                                                                                                                                                                                                         
                                                                                                                                                                                                                                                                                                                                                                          
                                                 The default value is `0.0`, which disables this feature.                                                                                                                                                                                                                                                                 | float   |
| `pm set` partitionName -fallback=value        | Sets the fallback cost cut level for the partition.                                                                                                                                                                                                                                                                     
                                                                                                                                                                                                                                                                                                                                                                          
                                                 If the best pool's performance cost exceeds the fallback cost cut level then a pool of the next level will be chosen. This means for example that instead of choosing a pool with `readpref` = 20 a pool with `readpref` &lt; 20 will be chosen.                                                                         
                                                                                                                                                                                                                                                                                                                                                                          
                                                 The default value is `0.0`, which disables this feature.                                                                                                                                                                                                                                                                 | float   |
| `pm set` partitionName -slope=slope           | Sets the dynamic replication threshold value for the partition.                                                                                                                                                                                                                                                         
                                                                                                                                                                                                                                                                                                                                                                          
                                                 If slope&gt; `0.01` then the product of best pool's performance cost and slope is used as threshold for pool to pool replication.                                                                                                                                                                                        
                                                                                                                                                                                                                                                                                                                                                                          
                                                 If the performance cost on the best pool exceeds this threshold then this pool is called hot.                                                                                                                                                                                                                            
                                                                                                                                                                                                                                                                                                                                                                          
                                                 The default value is `0.0`, which disables this feature.                                                                                                                                                                                                                                                                 | float   |
| `pm set` partitionName -p2p-allowed=value     | This value can be specified if an HSM is attached to the dCache.                                                                                                                                                                                                                                                        
                                                                                                                                                                                                                                                                                                                                                                          
                                                 If a partition has no HSM connected, then this option is overridden. This means that no matter which value is set for `p2p-allowed` the pool to pool replication will always be allowed.                                                                                                                                 
                                                                                                                                                                                                                                                                                                                                                                          
                                                 By setting value = `off` the values for `p2p-allowed`, `p2p-oncost` and `p2p-fortransfer` will take over the value of the default partition.                                                                                                                                                                             
                                                                                                                                                                                                                                                                                                                                                                          
                                                 If value = `yes` then pool to pool replication is allowed.                                                                                                                                                                                                                                                               
                                                                                                                                                                                                                                                                                                                                                                          
                                                 As a side effect of setting value = `no` the values for `p2p-oncost` and `p2p-fortransfer` will also be set to `no`.                                                                                                                                                                                                     
                                                                                                                                                                                                                                                                                                                                                                          
                                                 The default value is `yes`.                                                                                                                                                                                                                                                                                              | boolean |
| `pm set` partitionName -p2p-oncost=value      | Determines whether pool to pool replication is allowed if the best pool for a read request is hot.                                                                                                                                                                                                                      
                                                                                                                                                                                                                                                                                                                                                                          
                                                 The default value is `no`.                                                                                                                                                                                                                                                                                               | boolean |
| `pm set` partitionName -p2p-fortransfer=value | If the best pool is hot and the requested file will be copied either from the hot pool or from tape to another pool, then the requested file will be read from the pool where it just had been copied to if value = `yes`. If value = `no` then the requested file will be read from the hot pool.                      
                                                                                                                                                                                                                                                                                                                                                                          
                                                 The default value is `no`.                                                                                                                                                                                                                                                                                               | boolean |
| `pm set` partitionName -stage-allowed=value   | Set the stage allowed value to `yes` if a tape system is connected and to `no` otherwise.                                                                                                                                                                                                                               
                                                                                                                                                                                                                                                                                                                                                                          
                                                 As a side effect, setting the value for `stage-allowed` to `no` changes the value for `stage-oncost` to `no`.                                                                                                                                                                                                            
                                                                                                                                                                                                                                                                                                                                                                          
                                                 The default value is `no`.                                                                                                                                                                                                                                                                                               | boolean |
| `pm set` partitionName -stage-oncost=value    | If the best pool is hot, p2p-oncost is disabled and an HSM is connected to a pool then this parameter determines whether to stage the requested file to a different pool.                                                                                                                                               
                                                                                                                                                                                                                                                                                                                                                                          
                                                 The default value is `no`.                                                                                                                                                                                                                                                                                               | boolean |
| `pm set` partitionName -max-copies=copies     | Sets the maximal number of replicas of one file. If the maximum is reached no more replicas will be created.                                                                                                                                                                                                            
                                                                                                                                                                                                                                                                                                                                                                          
                                                 The default value is `500`.                                                                                                                                                                                                                                                                                              | integer |

Link Groups
===========

The PoolManager supports a type of objects called link groups. These link groups are used by the [SRM SpaceManager](config-SRM.md#srm-spacemanager) to make reservations against space. Each link group corresponds to a number of dCache pools in the following way: A link group is a collection of [links](#links) and each link points to a set of pools. Each link group knows about the size of its available space, which is the sum of all sizes of available space in all the pools included in this link group.

To create a new link group login to the [Admin Interface](intouch.md#the-admin-interface) and `cd` to the PoolManager.

    (local) admin > cd PoolManager
    (PoolManager) admin > psu create linkGroup <linkgroup>
    (PoolManager) admin > psu addto linkGroup <linkgroup> <link>
    (PoolManager) admin > save


With `save` the changes will be saved to the file **/var/lib/dcache/config/poolmanager.conf**.

> **NOTE**
>
> You can also edit the file **/var/lib/dcache/config/poolmanager.conf** to create a new link group. Please make sure that it already exists. Otherwise you will have to create it first via the Admin Interface by
>
>     (PoolManager) admin > save
>
> Edit the file **/var/lib/dcache/config/poolmanager.conf**
>
>     psu create linkGroup <linkgroup>
>     psu addto linkGroup <linkgroup> <link>
>
> After editing this file you will have to restart the domain which contains the PoolManager cell to apply the changes.

> **NOTE**
>
> Administrators will have to take care, that no pool is present in more than one link group.

**Access latency and retention policy.**

A space reservation has a *retention policy* and an *access latency*, where retention policy describes the quality of the storage service that will be provided for files in the space reservation and access latency describes the availability of the files. See [the section called “Properties of Space Reservation”](config-SRM.md#properties-of-space-reservation) for further details.

A link group has five boolean properties called `replicaAllowed, outputAllowed, custodialAllowed, onlineAllowed` and `nearlineAllowed`, which determine the access latencies and retention policies allowed in the link group. The values of these properties (true or false) can be configured via the Admin Interface or directly in the file **/var/lib/dcache/config/poolmanager.conf**.

For a space reservation to be allowed in a link group, the the retention policy and access latency of the space reservation must be allowed in the link group.

    (PoolManager) admin > psu set linkGroup custodialAllowed <linkgroup> <true|false>
    (PoolManager) admin > psu set linkGroup outputAllowed <linkgroup> <true|false>
    (PoolManager) admin > psu set linkGroup replicaAllowed <linkgroup> <true|false>
    (PoolManager) admin > psu set linkGroup onlineAllowed <linkgroup> <true|false>
    (PoolManager) admin > psu set linkGroup nearlineAllowed <linkgroup> <true|false>

> **IMPORTANT**
>
> It is up to the administrator to ensure that the link groups' properties are specified correctly.
>
> For example dCache will not complain if a link group that does not support a tape backend will be declared as one that supports `custodial` files.
>
> It is essential that space in a link group is homogeneous with respect to access latencies, retention policies and storage groups accepted. Otherwise space reservations cannot be guaranteed. For instance, if only a subset of pools accessible through a link group support custodial files, there is no guarantee that a custodial space reservation created within the link group will fit on those pools.

  [Admin Interface]: #intouch-admin
  [section\_title]: #cf-pm-psu-pref
  [1]: #secStorageClass
  [2]: #secExReadWrite
  [3]: #secCacheClass
  [4]: #cf-pm-classic
  [How to Pick a Pool]: http://www.dcache.org/articles/wass.html
  [Web Interface]: #intouch-web
  [???]: #cmd-st_set_max_active
  [5]: #cmd-rh_set_max_active
  [6]: #cmd-mover_set_max_active
  [7]: #cmd-p2p_set_max_active
  [8]: #cmd-pp_set_max_active
  [9]: #cmd-set_gap
  [10]: #cmd-set_breakeven
  [11]: #cmd-set_pool_decision
  [slope]: #slope
  [SRM CELL-SPACEMNGR]: #cf-srm-space
  [links]: #cf-pm-links
  [12]: #cf-srm-intro-spaceReservation
