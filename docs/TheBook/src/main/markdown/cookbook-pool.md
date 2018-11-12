Chapter 23. Pool OperationsPool Operations
==========================================

Table of Contents

+ [Checksums](#checksums)  

    [How to configure checksum calculation](#how-to-configure-checksum-calculation)  

+ [Migration Module](#migration-module)    

    [Overview and Terminology](#overview-and-terminology)  
    [Command Summary](#command-summary)  
    [Examples](#examples)  

+ [Renaming a Pool](#renaming-a-pool)  
+ [Pinning Files to a Pool](#pinning-files-to-a-pool)  

Checksums
=========

In dCache the storage of a checksum is part of a successful transfer.

-   For an incoming transfer a checksum can be sent by the client (*Client Checksum*, it can be calculated during the transfer (*Transfer Checksum*) or it can be calculated on the server after the file has been written to disk (*Server File Checksum*).

-   For a pool to pool transfer a Transfer Checksum or a Server File Checksum can be calculated.

-   For data that is flushed to or restored from tape a checksum can be calculated before flushed to tape or after restored from tape, respectively.

Client Checksum  

The client calculates the checksum before or while the data is sent to dCache. The checksum value, depending on when it has been calculated, may be sent together with the open request to the door and stored into CHIMERA before the data transfer begins or it may be sent with the close operation after the data has been transferred.

The `dCap` protocol provides both methods, but the `dCap` clients use the latter by default.

The `FTP` protocol does not provide a mechanism to send a checksum. Nevertheless, some `FTP` clients can (mis-)use the “`site`” command to send the checksum prior to the actual data transfer.


Transfer Checksum 

While data is coming in, the server data mover may calculate the checksum on the fly.


Server File Checksum  

After all the file data has been received by the dCache server and the file has been fully written to disk, the server may calculate the checksum, based on the disk file.

The default configuration is that a checksum is calculated on write, i.e. a Server File Checksum.

How to configure checksum calculation 
-------------------------------------

Configure the calculation of checksums in the [admin interface](https://www.dcache.org/manuals/Book-2.16/start/intouch-admin-fhs.shtml). The configuration has to be done for each pool separately.

    (local) admin > cd <poolname>
    (<poolname>) admin > csm set policy -<option>=<on/off>
    (<poolname>) admin > save
    
The configuration will be saved in the file **<path/to/pool>/<nameOfPooldirectory>/setup**.

Use the command `csm info` to see the checksum policy of the pool.

   (<poolname>) admin > csm info
     Policies :
            on read : false
           on write : true
           on flush : false
         on restore : false
        on transfer : false
        enforce crc : true
      getcrcfromhsm : false
              scrub : false
          

The default configuration is to check checksums on write.

Use the command `help csm set policy` to see the configuration options.

The syntax of the command `csm set policy` is `csm set policy` ` [-<option>=on [|off]] ` where <option> can be replaced by


OPTIONS

**ontransfer** 
If supported by the protocol, the checksum is calculated during file transfer.

**onwrite**  
The checksum is calculated after the file has been written to disk.

**onrestore**  
The checksum is calculated after data has been restored from tape.

**onflush**  
The checksum is calculated before data is flushed to tape.

**getcrcfromhsm**  
If the HSM script supports it, the `<pnfsid>.crcval` file is read and stored in CHIMERA.

**scrub** 
Pool data will periodically be veryfied against checksums. Use the command `help csm set
	      policy` to see the configuration options.

**enforcecrc**  
If no checksum has been calculated after or during the transfer, this option ensures that a checksum is calculated and stored in CHIMERA.

The option `onread` has not yet been implemented.

If an option is enabled a checksum is calculated as described. If there is already another checksum, the checksums are compared and if they match stored in CHIMERA.

> **Important**
>
> Do not change the default configuration for the option `enforcecrc`. This option should always be enabled as this ensures that there will always be a checksum stored with a file.

Migration Module
================

The purpose of the migration module is essentially to copy or move the content of a pool to one or more other pools.

Typical use cases for the migration module include:

-   Vacating pools, that is, moving all files to other pools before decomissioning the pool.

-   Caching data on other pools, thus distributing the load and increasing availability.

-   As an alternative to the hopping manager.


Overview and Terminology
------------------------

The migration module runs inside pools and hosts a number of migration jobs. Each job operates on a set of files on the pool on which it is executed and can copy or move those files to other pools. The migration module provides filters for defining the set of files on which a job operates.

The act of copying or moving a single file is called a migration task. A task selects a target pool and asks it to perform a pool to pool transfer from the source pool. The actual transfer is performed by the same component performing other pool to pool transfers. The migration module does not perform the transfer; it only orchestrates it.

The state of the target copy (the target state) as well as the source copy (the source state) can be explicitly defined. For instance, for vacating a pool the target state is set to be the same as the original source state, and the source state is changed to removed; for caching files, the target state is set to cached, and the source state is unmodified.

Sticky flags owned by the pin manager are never touched by a migration job, however the migration module can ask the pin manager to move the pin to the target pool. Care has been taken that unless the pin is moved by the pin manager, the source file is not deleted by a migration job, even if asked to do so. To illustrate this, assume a source file marked precious and with two sticky flags, one owned by foobar and the other by the pin manager. If a migration job is configured to delete the source file, but not to move the pin, the result will be that the file is marked cached, and the sticky flag owned by foobar is removed. The pin remains. Once it expires, the file is eligible for garbage collection.

All operations are idempotent. This means that a migration job can be safely rerun, and as long as everything else is unchanged, files will not be transferred again. Because jobs are idempotent they do not need to maintain persistent state, which in turns means the migration module becomes simpler and more robust. Should a pool crash during a migration job, the job can be rerun and the remaining files will be transfered.

> **Note**
>
> Please notice that a job is only idempotent as long as the set of target pools do not change. If pools go offline or are excluded as a result of a an exclude or include expression, then the idempotent nature of a job may be lost.

It is safe to run migration jobs while pools are in use. Once started, migration jobs run to completion and do only operate on those files that matched the selection filters at the time the migration job started. New files that arrive on the pool are not touched. Neither are files that change state after a migration job has been initialized, even though the selection filters would match the new state of the file. The exception to the rule is when files are deleted from the pool or change state so that they no longer match the selection filter. Such files will be excluded from the migration job, unless the file was already processed. Rerunning a migration job will force it to pick up any new files. Because the job is idempotent, any files copied before are not copied again.

Permanent migration jobs behave differently. Rather than running to completion, permanent jobs keep running until explicitly cancelled. They monitor the pool for any new files or state changes, and dynamically add or remove files from the transfer queue. Permanent jobs are made persistent when the **save** command is executed and will be recreated on pool restart. The main use case for permanent jobs is as an alternative to using a central hopping manager.

Idempotence is achieved by locating existing copies of a file on any of the target pools. If an existing copy is found, rather than creating a new copy, the state of the existing copy is updated to reflect the target state specified for the migration job. Care is taken to never make a file more volatile than it already is: Sticky flags are added, or existing sticky flags are extended, but never removed or shortened; cached files may be marked precious, but not vice versa. One caveat is when the target pool containing the existing copy is offline. In that case the existence of the copy cannot be verified. Rather than creating a new copy, the task fails and the file is put back into the transfer queue. This behaviour can be modified by marking a migration job as eager. Eager jobs create new copies if an existing copy cannot be immediately verified. As a rule of thumb, permanent jobs should never be marked eager. This is to avoid that a large number of unnecessary copies are created when several pools are restarted simultaneously.

A migration task aborts whenever it runs into a problem. The file will be reinserted at the end of the transfer queue. Consequently, once a migration job terminates, all files have been successfully transferred. If for some reason tasks for particular files keep failing, then the migration job will never terminate by itself as it retries indefinitely.

Command Summary
---------------

Login to the [admin interface](https://www.dcache.org/manuals/Book-2.16/start/intouch-admin-fhs.shtml) and `cd` to a pool to use the `migration` commands. Use the command `help migration` to view the possiblities.

    (local) admin > cd <poolname>
    (<poolname>) admin > help migration
    migration cache [OPTIONS] TARGET...
    migration cancel [-force] JOB
    migration clear
    migration concurrency ID CONCURRENCY
    migration copy [OPTIONS] TARGET...
    migration info JOB
    migration ls
    migration move [OPTIONS] TARGET...
    migration resume JOB
    migration suspend JOB
          

The commands `migration copy`, `migration cache` and `migration move` create new migration jobs. These commands are used to copy files to other pools. Unless filter options are specified, all files on the source pool are copied. The syntax for these commands is the same; example `migration copy`: 

`migration <copy> [<option>] <target>` 

There are four different types of options. The filter options, transfer options, target options and lifetime options. Please run the command `help migration copy` for a detailed description of the various options.

The commands `migration copy`, `migration move` and `migration
	cache` take the same options and only differ in default values.


migration move  

The command `migration move` does the same as the command `migration copy` with the options:

-   -smode
    =
    delete
    (default for
    migration copy
    is
    same
    ).
-   -pins
    =
    move
    (default for
    migration copy
    is
    keep
    ).

additionally it uses the option `-verify`.

migration cache  
The command `migration cache` does the same as the command `migration copy` with the option:

-   -tmode
    =
    cached

Jobs are assinged a job ID and are executed in the background. The status of a job may be queried through the `migration info` command. A list of all jobs can be obtained through `migration ls`. Jobs stay in the list even after they have terminated. Terminated jobs can be cleared from the list through the `migration clear` command.

Jobs can be suspended, resumed and cancelled through the `migration suspend`, `migration
        resume` and `migration cancel` commands. Existing tasks are allowed to finish before a job is suspended or cancelled.


Example:

A migration job can be suspended and resumed with the commands `migration suspend ` and `migration resume` respectively.

    (local) admin > cd <poolname>
    (<poolname>) admin > migration copy -pnfsid=000060D40698B4BF4BE284666ED29CC826C7 pool2
    [1] INITIALIZING migration copy 000060D40698B4BF4BE284666ED29CC826C7 pool2
    [1] SLEEPING     migration copy 000060D40698B4BF4BE284666ED29CC826C7 pool2
    (<poolname>) admin > migration ls
    [1] RUNNING      migration copy 000060D40698B4BF4BE284666ED29CC826C7 pool2
    (<poolname>) admin > migration suspend 1
    [1] SUSPENDED    migration copy 000060D40698B4BF4BE284666ED29CC826C7 pool2
    (<poolname>) admin > migration resume 1
    [1] RUNNING      migration copy 000060D40698B4BF4BE284666ED29CC826C7 pool2
    (<poolname>) admin > migration info 1
    Command    : migration copy -pnfsid=000060D40698B4BF4BE284666ED29CC826C7 pool2
    State      : RUNNING
    Queued     : 0
    Attempts   : 1
    Targets    : pool2
    Completed  : 0 files; 0 bytes; 0%
    Total      : 5242880 bytes
    Concurrency: 1
    Running tasks:
    [1] 00007C75C2E635CD472C8A75F5A90E4960D3: TASK.GettingLocations
    (<poolname>) admin > migration info 1
    Command    : migration copy -pnfsid=000060D40698B4BF4BE284666ED29CC826C7 pool2
    State      : FINISHED
    Queued     : 0
    Attempts   : 1
    Targets    : pool2
    Completed  : 1 files; 5242880 bytes
    Total      : 5242880 bytes
    Concurrency: 1
    Running tasks:
    (<poolname>) admin > migration ls
    [1] FINISHED     migration copy 000060D40698B4BF4BE284666ED29CC826C7 pool2
    	

A migration job can be cancelled with the command `migration cancel `.

    (local) admin > cd <poolname>
    (<poolname>) admin > migration copy -pnfsid=0000D194FBD450A44D3EA606D0434D6D88CD pool2
    [1] INITIALIZING migration copy 0000D194FBD450A44D3EA606D0434D6D88CD pool2
    (<poolname>) admin > migration cancel 1
    [1] CANCELLED    migration copy -pnfsid=0000D194FBD450A44D3EA606D0434D6D88CD pool2
	

And terminated jobs can be cleared with the command `migration clear`.

    (<poolname>) admin > migration ls
    [3] FINISHED     migration copy -pnfsid=0000D194FBD450A44D3EA606D0434D6D88CD pool2
    [2] FINISHED     migration copy -pnfsid=00007C75C2E635CD472C8A75F5A90E4960D3 pool2
    [1] FINISHED     migration copy -pnfsid=0000A48650142CBF4E55A7A26429DFEA27B6 pool2
    [5] FINISHED     migration move -pnfsid=000028C0C288190C4CE7822B3DB2CA6395E2 pool2
    [4] FINISHED     migration move -pnfsid=00007C75C2E635CD472C8A75F5A90E4960D3 pool2
    (<poolname>) admin > migration clear
    (<poolname>) admin > migration ls
	
    	

Except for the number of concurrent tasks, transfer parameters of existing jobs cannot be changed. This is by design to ensure idempotency of jobs. The concurrency can be altered through the `migration concurrency` command.

    (<poolname>) admin > migration concurrency 4 2
    (<poolname>) admin > migration info
    Command    : migration copy pool2
    State      : FINISHED
    Queued     : 0
    Attempts   : 6
    Targets    : pool2
    Completed  : 6 files; 20976068 bytes
    Total      : 20976068 bytes
    Concurrency: 2
    Running tasks:
      
          

Examples
--------

### Vacating a pool

To vacate the pool <sourcePool>, we first mark the pool `read-only` to avoid that more files are added to the pool, and then move all files to the pool <targetPool>. It is not strictly necessary to mark the pool `read-only`, however if not done there is no guarantee that the pool is empty when the migration job terminates. The job can be rerun to move remaining files.

    (<sourcePool>) admin > pool disable -rdonly
    (<sourcePool>) admin > migration move <targetPool>
    [1] RUNNING      migration move <targetPool>
    (<sourcePool>) admin > migration info 1
    Command    : migration move <targetPool>
    State      : RUNNING
    Queued     : 0
    Attempts   : 1
    Targets    : <targetPool>
    Completed  : 0 files; 0 bytes; 0%
    Total      : 830424 bytes
    Concurrency: 1
    Running tasks:
    [0] 0001000000000000000BFAE0: TASK.Copying -> [<targetPool>@local]
    (<sourcePool>) admin > migration info 1
    Command    : migration move <targetPool>
    State      : FINISHED
    Queued     : 0
    Attempts   : 1
    Targets    : <targetPool>
    Completed  : 1 files; 830424 bytes
    Total      : 830424 bytes
    Concurrency: 1
    Running tasks:
    (<sourcePool>) admin > rep ls
    (<sourcePool>) admin >

### Caching recently accessed files

Say we want to cache all files belonging to the storage group `atlas:default` and accessed within the last month on a set of low-cost cache pools defined by the pool group `cache_pools`. We can achieve this through the following command.

     <sourcePool>) admin > migration cache -target=pgroup -accessed=0..2592000 -storage=atlas:default cache_pools
    [1] INITIALIZING migration cache -target=pgroup -accessed=0..2592000 -storage=atlas:default cache_pools
    (<sourcePool>) admin > migration info 1
    Command    : migration cache -target=pgroup -accessed=0..2592000 -storage=atlas:default cache_pools
    State      : RUNNING
    Queued     : 2577
    Attempts   : 2
    Targets    : pool group cache_pools, 5 pools
    Completed  : 1 files; 830424 bytes; 0%
    Total      : 2143621320 bytes
    Concurrency: 1
    Running tasks:
    [72] 00010000000000000000BE10: TASK.Copying -> [pool_2@local]

The files on the source pool will not be altered. Any file copied to one of the target pools will be marked cached.

Renaming a Pool
===============

A pool may be renamed with the following procedure, regardless of the type of files stored on it.

Disable file transfers from and to the pool with

    (<poolname>) admin > pool disable -strict

Then make sure, no transfers are being processed anymore. All the following commands should give no output:

   (<poolname>) queue ls queue
   (<poolname>) mover ls
   (<poolname>) p2p ls
   (<poolname>) pp ls
   (<poolname>) st jobs ls
   (<poolname>) rh jobs ls

Now the files on the pools have to be unregistered on the namespace server with

   (<poolname>) pnfs unregister

> **Note**
>
> Do not get confused that the commands **pnfs unregister** and **pnfs register** contain `pnfs` in their names. They also apply to dCache > instances with Chimera and are named like that for legacy reasons. 

Even if the pool contains precious files, this is no problem, since we will register them again in a moment. The files might not be available for a short moment, though. Log out of the pool, and stop the domain running the pool:

    [root] # dcache stop <poolDomain>
     Stopping <poolDomain> (pid=6070) 0 1 2 3 done
    [root] #

Adapt the name of the pool in the layout files of your dCache installation to include your new pool-name. For a general overview of layout-files see [ the section called “Defining domains and services”. ](install.md#defining-domains-and-services).

Example:
For example, to rename a pool from `swimmingPool` to `carPool`, change your layout file from

    [poolDomain]
    [poolDomain/pool]
    name=swimmingPool
    path=/pool/

to

    [poolDomain]
    [poolDomain/pool]
    name=carPool
    path=/pool/

> **Warning**
>
> Be careful about renaming pools in the layout after users have already been writing to them. This can cause inconsistencies in other components of dCache, if they are relying on pool names to provide their functionality. An example of such a component is the CHIMERA cache info.

Start the domain running the pool:

    [root] # dcache start <poolDomain>
    Starting poolDomain done
    [root] #

Register the files on the pool with

   (<poolname>) admin > pnfs register

Pinning Files to a Pool
=======================

You may pin a file locally within the private pool repository:

    (<poolname>) admin > rep set sticky <pnfsid> on|off

the `sticky` mode will stay with the file as long as the file is in the pool. If the file is removed from the pool and recreated afterwards this information gets lost.

You may use the same mechanism globally: in the command line interface (local mode) there is the command

   (local) admin > set sticky <pnfsid>

This command does:

1.  Flags the file as `sticky` in the name space database (CHIMERA). So from now the filename is globally set `sticky`.

2.  Will go to all pools where it finds the file and will flag it `sticky` in the pools.

3.  All new copies of the file will become `sticky`.

  [admin interface]: #intouch-admin
  [???]: #in-install-layout
