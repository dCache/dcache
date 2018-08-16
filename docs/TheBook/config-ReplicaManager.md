CHAPTER 6. THE REPLICA SERVICE (REPLICA MANAGER)
================================================

Table of Contents
-----------------

* [The Basic Setup](#the-basic-setup)  
        
	[Define a poolgroup for resilient pools](#define-a-poolgroup-for-resilient-pools)  

* [Operation](#operation)  

	[Pool States](#pool-states)  
	[Startup](#startup)  
	[Avoid replicas on the same host](#avoid-replicas-on-the-same-host)  
	[Hybrid dCache](#hybrid-dcache)  
	[Commands for the admin interface](#commands-for-the-admin-interface)  
	
* [Properties of the replica service](#properties-of-the-replica-service)  


The replica service (which is also referred to as Replica Manager) controls the number of replicas of a file on the pools. If no [tertiary storage system](rf-glossary.md-tertiary-storage-system) is connected to a dCache instance (i.e., it is configured as a [large file store](rf-glossary.md#large-file-store-lfs)), there might be only one copy of each file on disk. (At least the [precious replica](rf-glossary.md#precious-replica).) If a higher security and/or availability is required, the resilience feature of dCache can be used: If running in the default configuration, the replica service will make sure that the number of [replicas](rf-glossary.md#replicas) of a file will be at least 2 and not more than 3. If only one replica is present it will be copied to another pool by a [pool to pool transfer](rf-glossary.md#pool-to-pool-transfer). If four or more replicas exist, some of them will be deleted.



THE BASIC SETUP
===============

The standard configuration assumes that the database server is installed on the same machine as the `replica` service — usually the admin node of the dCache instance. If this is not the case you need to set the property `replica.db.host`.

To create and configure the database *replica* used by the `replica` service in the database server do:


   [root] # createdb -U dcache replica
   [root] # psql -U dcache -d replica -f /usr/share/dcache/replica/psql_install_replicas.sql


To activate the replica service you need to

1.  Enable the replica service in a layout file.

        [<someDomain>]
         ...

        [<someDomain>/replica]

2.  Configure the service in the **/etc/dcache/dcache.conf** file on the node with the dCacheDomain and on the node on which the `pnfsmanager` is running.

        dcache.enable.replica=true

    > **NOTE**
    >
    > It will not work properly if you defined the `replica` service in one of the layout files and set this property to `no` on the node with the `dCacheDomain` or on the node on which the `pnfsmanager` is running.

3.  Define a pool group for the resilient pools if necessary.

4.  Start the `replica` service.

In the default configuration, all pools of the dCache instance which have been created with the command `dcache pool create` will be managed. These pools are in the pool group named default which does exist by default. The replica service will keep the number of replicas between 2 and 3 (including). At each restart of the replica service the pool configuration in the database will be recreated.

Example:

This is a simple example to get started with. All your pools are assumed to be in the pool group default.

1.  In your layout file in the directory **/etc/dcache/layouts** define the replica service.


        [dCacheDomain]
        ...

        [replicaDomain]
        [replicaDomain/replica]

2.  In the file **/etc/dcache/dcache.conf** set the value for the property `replicaManager` to `true` and the `replica.poolgroup` to `default`.

        dcache.enable.replica=true
        replica.poolgroup=default

3.  The pool group `default` exists by default and does not need to be defined.

4.  To start the `replica` service restart dCache.

        [root] # dcache restart

DEFINE A POOLGROUP FOR RESILIENT POOLS
--------------------------------------

For more complex installations of dCache you might want to define a pool group for the resilient pools.  

Define the resilient pool group in the **/var/lib/dcache/config/poolmanager.conf** file on the host running the `poolmanager` service. Only pools defined in the resilient pool group will be managed by the `replica` service.  

Example:  
Login to the admin interface and cd to the `PoolManager`. Define a poolgroup for resilient pools and add pools to that poolgroup.

   (local) admin > cd PoolManager
   (PoolManager) admin > psu create pgroup ResilientPools
   (PoolManager) admin > psu create pool  pool3
   (PoolManager) admin > psu create pool  pool4
   (PoolManager) admin > psu addto pgroup ResilientPools pool3
   (PoolManager) admin > psu addto pgroup ResilientPools pool4
   (PoolManager) admin > save

By default the pool group named `ResilientPools` is used for replication.

To use another pool group defined in **/var/lib/dcache/config/poolmanager.conf** for replication, please specify the group name in the **etc/dcache.conf** file.

    replica.poolgroup=<NameOfResilientPoolGroup>.

OPERATION
=========

When a file is transfered into dCache its replica is copied into one of the pools. Since this is the only replica and normally the required range is higher (e.g., by default at least 2 and at most 3), this file will be replicated to other pools.

When some pools go down, the replica count for the files in these pools may fall below the valid range and these files will be replicated. Replicas of the file with replica count below the valid range and which need replication are called *deficient* replicas.

Later on some of the failed pools can come up and bring online more valid replicas. If there are too many replicas for some file these extra replicas are called *redundant* replicas and they will be “reduced”. Extra replicas will be deleted from pools.

The replica service counts the number of replicas for each file in the pools which can be used online (see Pool States below) and keeps the number of replicas within the valid range (`replica.limits.replicas.min, replica.limits.replicas.max`).


POOL STATES 
-----------

The possible states of a pool are `online`, `down`, `offline`, `offline-prepare` and `drainoff`. They can be set by the admin through the admin interface.  (See [the section called “Commands for the admin interface”](#commands-for-the-admin-interface).)

![Pool State Diagram](poolstate.jpg)



online  
	 Normal operation.
	 Replicas in this state are readable and can be counted. Files can be written (copied) to this pool.

down  
	A pool can be `down` because
	-   the admin stopped the domain in which the pool was running.
	-   the admin set the state value via the admin interface.
	-   the pool crashed

	To confirm that it is safe to turn pool down there is the command `ls unique` in the admin interface to check number  		of files which can be locked in this pool. (See [the section called “Commands for the admin interface”](#commands-for-the-admin-interface).)

	Replicas in pools which are `down` are not counted, so when a pool crashes the number of `online` replicas for some 		files is reduced. The crash of a pool (pool departure) may trigger replication of multiple files.

	On startup, the pool comes briefly to the `online` state, and then it goes `down` to do pool “Inventory” to cleanup 		files which broke when the pool crashed during transfer. When the pool comes online again, the REPLICA service will 		update the list of replicas in the pool and store it in the database.

	Pool recovery (arrival) may trigger massive deletion of file replicas, not necessarily in this pool.

offline  
	The admin can set the pool state to be `offline`. This state was introduced to avoid unnecessary massive replication if 	the operator wants to bring the pool down briefly without triggering massive replication.

	Replicas in this pool are counted, therefore it does not matter for replication purpose if an `offline` pool goes down 		or up.

	When a pool comes `online` from an `offline` state replicas in the pool will be inventoried to make sure we know the 		real list of replicas in the pool.

offline-prepare  
	This is a transient state betweeen `online` and `offline`.

 	The admin will set the pool state to be `offline-prepare` if he wants to change the pool state and does not want to 		trigger massive replication.

	Unique files will be evacuated MDASH at least one replica for each unique file will be copied out. It is unlikely that a 	 file will be locked out when a single pool goes down as normally a few replicas are online. But when several pools go 		down or set drainoff or offline file lockout might happen.

	Now the admin can set the pool state `offline` and then `down` and no file replication will be triggered.

drainoff  
	This is a transient state betweeen `online` and `down`.
 
	The admin will set the pool state to be `drainoff` if he needs to set a pool or a set of pools permanently out of 		operation and wants to make sure that there are no replicas “locked out”.

	Unique files will be evacuated MDASH at least one replica for each unique file will be copied out. It is unlikely that a 	 file will be locked out when a single pool goes down as normally a few replicas are online. But when several pools go 		down or set drainoff or offline file lockout might happen.

	Now the admin can set the pool state down. Files from other pools might be replicated now, depending on the values of 	 	`replica.limits.replicas.min` and `replica.limits.replicas.max`.

STARTUP
-------

When the `replica` service starts it cleans up the database. Then it waits for some time to give a chance to most of the pools in the system to connect. Otherwise unnecessary massive replication would start. Currently this is implemented by some delay to start adjustments to give the pools a chance to connect.



### Cold Start

Normally (during Cold Start) all information in the database is cleaned up and recreated again by polling pools which are `online` shortly after some minimum delay after the `replica` service starts. The `replica` service starts to track the pools' state (pool up/down messages and polling list of online pools) and updates the list of replicas in the pools which came online. This process lasts for about 10-15 minutes to make sure all pools came up online and/or got connected. Pools which once get connected to the `replica` service are in online or down state.

It can be annoying to wait for some large period of time until all known “good” pools get connected. There is a “Hot Restart” option to accelerate the restart of the system after the crash of the head node.

### Hot Restart

On Hot Restart the `replica` service retrieves information about the pools' states before the crash from the database and saves the pools' states to some internal structure. When a pool gets connected the `replica` service checks the old pool state and registers the old pool's state in the database again if the state was `offline`, `offline-prepare` or `drainoff` state. The `replica` service also checks if the pool was `online` before the crash. When all pools which were `online` get connected once, the `replica` service supposes it recovered its old configuration and the `replica` service starts adjustments. If some pools went down during the connection process they were already accounted and adjustment would take care of it.

Example:  

Suppose we have ten pools in the system, where eight pools were `online` and two were `offline` before a crash. The `replica` service does not care about the two `offline` pools to get connected to start adjustments. For the other eight pools which were `online`, suppose one pool gets connected and then it goes down while the other pools try to connect. The `replica` service considers this pool in known state, and when the other seven pools get connected it can start adjustments and does not wait any more. If the system was in equilibrium state before the crash, the `replica` service may find some deficient replicas because of the crashed pool and start replication right away.

AVOID REPLICAS ON THE SAME HOST
-------------------------------

For security reasons you might want to spread your replicas such that they are not on the same host, or in the same building or even in the same town. To configure this you need to set the `tag.hostname` label for your pools and check the properties `replica.enable.check-pool-host` and `replica.enable.same-host-replica`.

Example:
We assume that some pools of your dCache are in Hamburg and some are in Berlin. In the layout files where the respective pools are defined you can set

    [poolDomain]
    [poolDomain/pool1]
    name=pool1
    path=/srv/dcache/p1
    pool.size=500G
    pool.wait-for-files=${path}/data
    tag.hostname=Hamburg

and

    [poolDomain]
    [poolDomain/pool2]
    name=pool2
    path=/srv/dcache/p2
    pool.size=500G
    pool.wait-for-files=${path}/data
    tag.hostname=Berlin

By default the property `replica.enable.check-pool-host` is `true` and `replica.enable.same-host-replica` is `false`. This means that the `tag.hostname` will be checked and the replication to a pool with the same `tag.hostname` is not allowed.

HYBRID dCache
-------------

A * hybrid dCache*  operates on a combination of pools (maybe connected to tape) which are not in a resilient pool group and the set of resilient pools. The `replica` service takes care only of the subset of pools configured in the pool group for resilient pools and ignores all other pools.

> **NOTE**
>
> If a file in a resilient pool is marked precious and the pool were connected to a tape system, then it would be flushed to tape. Therefore, the pools in the resilient pool group are not allowed to be connected to tape.

COMMANDS FOR THE ADMIN INTERFACE
--------------------------------

If you are an advanced user, have proper privileges and you know how to issue a command to the admin interface you may connect to the `ReplicaManager` cell and issue the following commands. You may find more commands in online help which are for debug only — do not use them as they can stop `replica` service operating properly.

`set pool` <pool><state>
set pool state

`show pool` <pool>
show pool state

`ls unique` <pool>
Reports number of unique replicas in this pool.

`exclude` <pnfsId> 
exclude <pnfsId> from adjustments

`release` <pnfsId>  
removes transaction/`BAD` status for <pnfsId>

`debug true | false`  
enable/disable DEBUG messages in the log file

PROPERTIES OF THE REPLICA SERVICE
=================================

**replica.cell.name**     
Default: `dcache.enable.replica`  
Cell name of the REPLICA service  

**dcache.enable.replica**   
Default: `false`  
Set this value to `true` if you want to use the REPLICA service.  

**replica.poolgroup**     
Default: `ResilientPools`  
If you want to use another pool group for the resilient pools set this value to the name of the resilient pool group.  

**replica.db.host**     
Default: `localhost  
Set this value to the name of host of the REPLICA service database.  

**replica.db.name**  
Default: `replica`  
Name of the replica database table.  

**replica.db.user** 
Default: `dcache`  
Change if the `replicas` database was created with a user other than `dcache`.  

**replica.db.password.file**   
Default: no password  

**replica.db.driver**    
Default: `org.postgresql.Driver`    
replica service was tested with PSQL only.    


**replica.limits.pool-watchdog-period **    
Default: `600` (10 min)     
Pools Watch Dog poll period. Poll the pools with this period to find if some pool went south without sending a notice (messages). Can not be too short because a pool can have a high load and not send pings for some time. Can not be less than pool ping period.  

**replica.limits.excluded-files-expiration-timeout** 
Default: `43200` (12 hours)  

**replica.limits.delay-db-start-timeout**
Default: `1200` (20 min)  
On first start it might take some time for the pools to get connected. If replication started right away, it would lead to massive replications when not all pools were connected yet. Therefore the database init thread sleeps some time to give a chance to the pools to get connected.

**replica.limits.adjust-start-timeout**  
Default: `1200` (20 min)  
Normally Adjuster waits for database init thread to finish. If by some abnormal reason it cannot find a database thread then it will sleep for this delay.

**replica.limits.wait-replicate-timeout**   
Default: `43200` (12 hours)  
Timeout for pool-to-pool replica copy transfer.  

**replica.limits.wait-reduce-timeout**   
Default: `43200` (12 hours)  
Timeout to delete replica from the pool.  

**replica.limits.workers**  
Default: `6`  
Number of worker threads to do the replication. The same number of worker threads is used for reduction. Must be more for larger systems but avoid situation when requests get queued in the pool.

**replica.limits.replicas.min**    
Default: `2`  
Minimum number of replicas in pools which are `online` or `offline`.  

**replica.limits.replicas.max**   
Default: `3`
Maximum number of replicas in pools which are `online` or `offline`.  

**replica.enable.check-pool-host**    
Default: `true`  
Checks `tag.hostname` which can be specified in the layout file for each pool.  
Set this property to `false` if you do not want to perform this check.  

**replica.enable.same-host-replica**    
Default: `false`  
If set to `true` you allow files to be copied to a pool, which has the same `tag.hostname` as the source pool.  

> **NOTE**
>
> The property
> replica.enable.check-pool-host
> needs to be set to
> true
> if
> replica.enable.same-host-replica
> is set to false.

  [section\_title]: #cf-repman-op-cmds 
  [Pool State Diagram]: images/resilient_poolstate_v1-0.svg
