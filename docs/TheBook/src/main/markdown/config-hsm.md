Chapter 8: The dCache Tertiary Storage System Interface
============================================

One of the features dCache provides is the ability to migrate files from its disk repository to one or more connected Tertiary Storage Systems (TSS) and to move them back to disk when necessary. Although the interface between dCache and the TSS is kept simple, dCache assumes to interact with an intelligent TSS. dCache does not drive tape robots or tape drives by itself. More detailed requirements to the storage system are described in one of the subsequent paragraphs.

-----
[TOC bullet hierarchy]
-----

## Scope of this chapter

This document describes how to enable a standard dCache installation to interact with a Tertiary Storage System. In this description we assume that

-   every dCache disk pool is connected to only one TSS instance.
-   all dCache disk pools are connected to the same TSS instance.
-   the dCache instance has not yet been populated with data, or only with a negligible amount of files.

In general, not all pools need to be configured to interact with the same Tertiary Storage System or with a storage system at all. Furthermore pools can be configured to have more than one Tertiary Storage System attached, but all those cases are not in the scope of the document.

## Requirements for a tertiary storage system

dCache can only drive intelligent Tertiary Storage Systems. This essentially means that tape robot and tape drive operations must be done by the TSS itself and that there is some simple way to abstract the file `PUT, GET and REMOVE` operation.

### Migrating Tertiary Storage Systems with a file system interface.

Most migrating storage systems provide a regular POSIX file system interface. Based on rules, data is migrated from primary to tertiary storage (mostly tape systems). Examples for migrating storage systems are:

-   [HPSS](http://www.hpss-collaboration.org/)
    (High Performance Storage System)
-   [DMF](http://www.sgi.com/products/storage/tiered/dmf.html?)
    (Data Migration Facility)

### Tertiary Storage Systems with a minimalistic PUT, GET and REMOVE interface

Some tape systems provide a simple PUT, GET, REMOVE interface. Typically, a copy-like application writes a disk file into the TSS and returns an identifier which uniquely identifies the written file within the Tertiary Storage System. The identifier is sufficient to get the file back to disk or to remove the file from the TSS. Examples are:

-   [OSM](http://www.qstar.com/qstar-products/qstar-object-storage-manager)
    (Object Storage Manager)
-   [Enstore](http://www-ccf.fnal.gov/enstore/)
    (FERMIlab)

## How dCache interacts with a Tertiary Storage System

Whenever dCache decides to copy a file from disk to tertiary storage a user-provided [executable](#example-of-an-executable-to-simulate-a-tape-backend) which can be either a script or a binary is automatically started on the pool where the file is located. That `executable` is expected to write the file into the Backend Storage System and to return a URI, uniquely identifying the file within that storage system. The format of the URI as well as the arguments to the `executable`, are described later in this document. The unique part of the URI can either be provided by the storage element, in return of the `STORE FILE` operation, or can be taken from dCache. A non-error return code from the `executable` lets dCache assume that the file has been successfully stored and, depending on the properties of the file, dCache can decide to remove the disk copy if space is running short on that pool. On a non-zero return from the `executable`, the file doesn't change its state and the operation is retried or an error flag is set on the file, depending on the error [return code](#summary-of-return-codes) from the `executable`.

If dCache needs to restore a file to disk the same `executable` is launched with a different set of arguments, including the URI, provided when the file was written to tape. It is in the responsibility of the `executable` to fetch the file back from tape based on the provided URI and to return `0` if the `FETCH FILE` operation was successful or non-zero otherwise. In case of a failure the pool retries the operation or dCache decides to fetch the file from tape using a different pool.

Details about [writing an HSM plugin](cookbook-writing-hsm-plugins.md) can be found in the cookbook section of this book.

## Details on the TSS-support executable

### Summary of command line options

	This part explains the syntax of calling the EXECUTABLE that supports STOREFILE, `FETCH
	FILE` and REMOVEFILE operations.

	put<pnfsID><filename>-si=<storage-information> [<other-options>...]

	get <pnfsID> <filename> -si=<storage-information> -uri=<storage-uri> [<other-options>...]

	remove -uri=<storage-uri> [<other-options>...]

   	- put / get / remove: these keywords indicate the operation to be performed.
        put: copy file from disk to TSS.
        get: copy file back from TSS to disk.
        remove: remove the file from TSS.
  	 - <pnfsID>: The internal identifier (i-node) of the file within dCache. The <pnfsID> is unique within a single dCache 	instance and globally unique with a very high probability.
   	- <filename>: is the full path of the local file to be copied to the TSS (for put) and respectively into which the file from the TSS should be copied (for get).
   	- <storage-information>: the storage information of the file, as explained below.
  	 - <storage-uri>: the URI, which was returned by the executable, after the file was written to tertiary storage. In order to get the file back from the TSS the information of the URI is preferred over the information in the <storage-information>.
  	 - <other-options>: -<key> = <value> pairs taken from the TSS configuration commands of the pool 'setup' file. One of the options, always provided is the option -command=<full path of this executable>.



#### Storage Information

The `<storage-information>` is a string in the format
`-si=size=bytes;new=true/false;stored=true/false;sClass=StorageClass;cClass0CacheClass;hsm=StorageType;key=value;[key=value;[...]]`

Here is an example:

    -si=size=1048576000;new=true;stored=false;sClass=desy:cms-sc3;cClass=-;hsm=osm;Host=desy;

Mandatory storage information’s keys

    - <size>: Size of the file in bytes
    - <new>: False if file already in the dCache; True otherwise
    - <stored>: True if file already stored in the TSS; False otherwise
    - <sClass>: HSM depended, is used by the poolmanager for pool attraction.
    - <cClass>: Parent directory tag (cacheClass). Used by the poolmanager for pool attraction. May be '-'.
    - <hsm>: Storage manager name (enstore/osm). Can be overwritten by the parent directory tag (hsmType).


OSM specific storage information’s keys

	- <group>: The storage group of the file to be stored as specified in the ".(tag)(sGroup)" tag of the parent directory of the file to be stored.
	- <store>: The store name of the file to be stored as specified in the ".(tag)(OSMTemplate)" tag of the parent directory of the file to be stored.
	- <bfid>: Bitfile ID (get and remove only) (e.g. 000451243.2542452542.25424524)

Enstore specific storage information’s keys

 	- <group>: The storage group (e.g. cdf, cms ...)
 	- <family>: The file family (e.g. sgi2test, h6nxl8, ...)
	 - <bfid>: Bitfile ID (get only) (e.g. B0MS105746894100000)
	 - <volume>: Tape Volume (get only) (e.g. IA6912)
	 - <location>: Location on tape (get only) (e.g. : 0000_000000000_0000117)

There might be more key values pairs which are used by the dCache internally and which should not affect the behaviour of the   executable.

#### Storage URI

The storage-uri is formatted as follows:

    hsmType://hsmInstance/?store=storename&group=groupname&bfid=bfid

    <hsmType>: The type of the Tertiary Storage System
    <hsmInstance>: The name of the instance
    <storename> and <groupname> : The store and group name of the file as provided by the arguments to this executable.
    <bfid>: The unique identifier needed to restore or remove the file if necessary.

Example:

A storage-uri:

    osm://osm/?store=sql&group=chimera&bfid=3434.0.994.1188400818542

### Summary of return codes

| Return Code         | Meaning                 | Behaviour for PUT FILE | Behaviour for GET FILE                             |
|---------------------|-------------------------|------------------------|----------------------------------------------------|
| 30 &lt;= rc &lt; 40 | User defined            | Deactivates request    | Reports problem to POOLMNGR                   |
| 41                  | No space left on device | Pool retries           | Disables pool and reports problem to POOLMNGR |
| 42                  | Disk read I/O error     | Pool retries           | Disables pool and reports problem to POOLMNGR |
| 43                  | Disk write I/O error    | Pool retries           | Disables pool and reports problem to POOLMNGR |
| other               | -                       | Pool retries           | Reports problem to POOLMNGR                   |

### The EXECUTABLE and the STORE FILE operation

Whenever a disk file needs to be copied to a Tertiary Storage System dCache automatically launches an `executable` on the pool containing the file to be copied. Exactly one instance of the `executable` is started for each file. Multiple instances of the `executable` may run concurrently for different files. The maximum number of concurrent instances of the `executable` per pool as well as the full path of the `executable` can be configured in the 'setup' file of the pool as described in [the section called “The pool ’setup’ file”.](#the-pool-setup-file)

The following arguments are given to the executable of a STORE FILE operation on startup.

**put** pnfsID filename -si= storage-information more options

Details on the meaning of certain arguments are described in [the section called “Summary of command line options”.](#summary-of-command-line-options)

With the arguments provided the `executable` is supposed to copy the file into the Tertiary Storage System. The `executable` must not terminate before the transfer of the file was either successful or failed.

Success must be indicated by a `0` return of the `executable`. All non-zero values are interpreted as a failure which means, dCache assumes that the file has not been copied to tape.

In case of a `0` return code the `executable` has to return a valid storage URI to dCache in formate:

    hsmType://hsmInstance/?store=<storename>&group=<groupname>&bfid=<bfid>

Details on the meaning of certain parameters are described [above](#storage-uri).

The `bfid` can either be provided by the TSS as result of the STORE FILE operation or the `pnfsID` may be used. The latter assumes that the file has to be stored with exactly that `pnfsID` within the TSS. Whatever URI is chosen, it must allow to uniquely identify the file within the Tertiary Storage System.

> **Note**
>
> Only the URI must be printed to stdout by the `executable`. Additional information printed either before or after the URI will result in an error. stderr can be used for additional debug information or error messages.

### The EXECUTABLE and the FETCH FILE operation

Whenever a disk file needs to be restored from a Tertiary Storage System dCache automatically launches an `executable` on the pool containing the file to be copied. Exactly one instance of the `executable` is started for each file. Multiple instances of the `executable` may run concurrently for different files. The maximum number of concurrent instances of the `executable` per pool as well as the full path of the `executable` can be configured in the 'setup' file of the pool as described in [the section called “The pool ’setup’ file”](#the-pool-setup-file).

The following arguments are given to the executable of a FETCH FILE operation on startup:

**get**  pnfsID filename -si= storage-information-uri= storage-uri more options

Details on the meaning of certain arguments are described in [the section called “Summary of command line options”](#summary-of-command-line-options). For return codes see [the section called “Summary of return codes”](#summary-of-return-codes).

### The EXECUTABLE and the REMOVE FILE operation
Whenever a file is removed from the dCache namespace (file system) a process inside dCache makes sure that all copies of the file are removed from all internal and external media. The pool which is connected to the TSS which stores the file is activating the executable with the following command line options:

remove -uri= storage-uri more options

Details on the meaning of certain arguments are described in [the section called “Summary of command line options.”](#summary-of-command-line-options) For return codes see [the section called “Summary of return codes”.](#summary-of-return-codes)

The `executable` is supposed to remove the file from the TSS and report a zero return code. If a non-zero error code is returned, the dCache will call the script again at a later point in time.

## Configuring pools to interact with a Tertiary Storage System

The `executable` interacting with the Tertiary Storage System (TSS), as described in the chapter above, has to be provided to dCache on all pools connected to the TSS. The `executable`, either a script or a binary, has to be made `executable` for the user, dCache is running as, on that host.

The following files have to be modified to allow dCache to interact with the TSS.

-   The `/var/lib/dcache/config/poolmanager.conf` file (one per system)
-   The pool layout file (one per pool host)
-   The pool 'setup' file (one per pool)
-   The namespaceDomain layout file (one per system)

After the layout files and the various 'setup' files have been corrected, the following domains have to be restarted :

-   pool services
-   dCacheDomain
-   namespaceDomain

### The dCache layout files

#### The `/var/lib/dcache/config/poolmanager.conf` file

To be able to read a file from the tape in case the cached file has been deleted from all pools, enable the restore-option. The best way to do this is to log in to the Admin Interface and run the following commands:

    [example.dcache.org] (local) admin > \c PoolManager
    [example.dcache.org] (PoolManager) admin > pm set -stage-allowed=yes
    [example.dcache.org] (PoolManager) admin > save

 A restart of the dCacheDomain is not necessary in this case.

 Alternatively, if the file `/var/lib/dcache/config/poolmanager.conf` already exists then you can add the entry

    pm set -stage allowed=yes

and restart the DOMAIN-dCache.

> **Warning**
>
> Do not create the file `/var/lib/dcache/config/poolmanager.conf` with this single entry! This will result in an error.

#### The pool layout

The dCache layout file must be modified for each pool node connected
to a TSS. If your pool nodes have been configured correctly to work
without TSS, you will find the entry lfs=precious in the layout file
(that is located in `/etc/dcache/layouts` and in the file
`/etc/dcache/dcache.conf` respectively) for each pool service. This
entry is a disk-only-option and has to be removed for each pool which
should be connected to a TSS. This will default the lfs parameter to
hsm which is exactly what we need.

#### The pool setup file

The pool 'setup' file is the file `$poolHomeDir/$poolName/setup`. It
mainly defines 3 details related to TSS connectivity.

-   Pointer to the `executable` which is launched on storing and fetching files.
-   The maximum number of concurrent `STORE FILE` requests allowed per pool.
-   The maximum number of concurrent `FETCH FILE` requests allowed per pool.

Define the `executable` and Set the maximum number of concurrent `PUT` and `GET` operations:

   hsm create [-key[=value]] ... type [instance] [provider]

    hsm create osm osm -hsmBase=var/pools/tape/ -hsmInstance=osm
     -command=share/lib/hsmcp.rb -c:puts=1 -c:gets=1 -c:removes=1

    #
    #  PUT operations
    # set the maximum number of active PUT operations >= 1
    #
    st set max active <numberOfConcurrentPUTS>

   #
   # GET operations
   # set the maximum number of active GET operations >= 1
   #
   rh set max active `numberOfConcurrentGETs`

       - <hsmType>: the type ot the TSS system. Must be set to “osm” for basic setups.
       - <hsmInstanceName>: the instance name of the TSS system. Must be set to “osm” for basic setups.
       - </path/to/executable>: the full path to the executable which should be launched for each TSS operation.

Setting the maximum number of concurrent PUT and GET operations.

Both numbers must be non zero to allow the pool to perform transfers.

Example:

We provide a
[script](#example-of-an-executable-to-simulate-a-tape-backend) to
simulate a connection to a TSS. To use this script place it in the
directory `/usr/share/dcache/lib`, and create a directory to simulate
the base of the TSS.

```console-root
mkdir -p /hsmTape/data
```

Login to the Admin Interface to change the entry of the pool 'setup' file for a pool named pool\_1.

    (local) admin > \c pool_1
    (pool_1) admin > hsm set osm osm
    (pool_1) admin > hsm set osm -command=/usr/share/dcache/lib/hsmscript.sh
    (pool_1) admin > hsm set osm -hsmBase=/hsmTape
    (pool_1) admin > st set timeout  5
    (pool_1) admin > rh set timeout 5
    (pool_1) admin > save


As with mover queues, the flush queue can also be set to behave as either LIFO
(last-in-first-out) or FIFO (first-in-first-out).  This can be done statically using
the property:

```
    (one-of?fifo|lifo)pool.flush-controller.queue-order=fifo
```

in the setup file:

```
    flush set queue order lifo
```

or by using the admin command itself:

```
   \s <pool-name> flush set queue order lifo
```

While neither queue order guarantees fairness, switching to LIFO under heavy
queuing where the jobs are long running may provide better throughput to
late-coming users.  (Default is FIFO.)


#### The namespace layout

In order to allow dCache to remove files from attached TSSes, the “cleaner.enable.hsm = true” must be added immediately underneath the \[namespaceDomain/cleaner\] service declaration:

```ini
[namespaceDomain]
[namespaceDomain/cleaner]
cleaner.enable.hsm = true
```

### What happens next

After restarting the necessary dCache domains, pools, already containing files, will start transferring them into the TSS as those files only have a disk copy so far. The number of transfers is determined by the configuration in the pool 'setup' file as described above in [the section called “The pool ’setup’ file”.](#the-pool-setup-file)

## How to Store-/Restore files via the Admin Interface

In order to see the state of files within a pool, login into the pool in the admin interface and run the command `rep ls`.

    [example.dcache.org] (<poolname>) admin > rep ls

The output will have the following format:

    PNFSID <MODE-BITS(LOCK-TIME)[OPEN-COUNT]> SIZE si={STORAGE-CLASS}

-   PNFSID: The pnfsID of the file
-   MODE-BITS:

           CPCScsRDXEL
           |||||||||||
           ||||||||||+--  (L) File is locked (currently in use)
           |||||||||+---  (E) File is in error state
           ||||||||+----  (X) File is pinned (aka "sticky")
           |||||||+-----  (D) File is in process of being destroyed
           ||||||+------  (R) File is in process of being removed
           |||||+-------  (s) File sends data to back end store
           ||||+--------  (c) File sends data to client (DCAP,FTP...)
           |||+---------  (S) File receives data from back end store
           ||+----------  (C) File receives data from client (DCAP,FTP)
           |+-----------  (P) File is precious, i.e., it is only on disk
           +------------  (C) File is on tape and only cached on disk.

-   LOCK-TIME: The number of milli-seconds this file will still be locked. Please note that this is an internal lock and not the pin-time (SRM).
-   OPEN-COUNT: Number of clients currently reading this file.
-   SIZE: File size
-   STORAGE-CLASS: The storage class of this file.

Example:

    [example.dcache.org] (pool_1) admin > rep ls
    00008F276A952099472FAD619548F47EF972 <-P---------L(0)[0]> 291910 si={dteam:STATIC}
    00002A9282C2D7A147C68A327208173B81A6 <-P---------L(0)[0]> 2011264 si={dteam:STATIC}
    0000EE298D5BF6BB4867968B88AE16BA86B0 <-C----------L(0)[0]> 1976 si={dteam:STATIC}

In order to `flush` a file to the tape run the command `flush pnfsid`.

       [example.dcache.org] (<poolname>) admin > flush pnfsid <pnfsid>

Example:
   [example.dcache.org] (pool_1) admin > flush pnfsid 00002A9282C2D7A147C68A327208173B81A6
Flush Initiated

A file that has been flushed to tape gets the flag 'C'.

Example:

   [example.dcache.org] (pool_1) admin > rep ls
    00008F276A952099472FAD619548F47EF972 <-P---------L(0)[0]> 291910 si={dteam:STATIC}
    00002A9282C2D7A147C68A327208173B81A6 <C----------L(0)[0]> 2011264 si={dteam:STATIC}
    0000EE298D5BF6BB4867968B88AE16BA86B0 <C----------L(0)[0]> 1976 si={dteam:STATIC}

To remove such a file from the repository run the command `rep rm`.

   [example.dcache.org] (<poolname>) admin > rep rm <pnfsid>

Example:

    [example.dcache.org] (pool_1) admin > rep rm  00002A9282C2D7A147C68A327208173B81A6
Removed 00002A9282C2D7A147C68A327208173B81A6

In this case the file will be restored when requested.

To `restore` a file from the tape you can simply request it by initializing a reading transfer or you can fetch it by running the command `rh restore`.

    [example.dcache.org] (<poolname>) admin > rh restore [-block] <pnfsid>

Example:

    [example.dcache.org] (pool_1) admin > rh restore 00002A9282C2D7A147C68A327208173B81A6
    Fetch request queued

## How to monitor what's going on

This section briefly describes the commands and mechanisms to monitor the TSS `PUT, GET and REMOVE` operations. dCache provides a configurable logging facility and a Command Line Admin Interface to query and manipulate transfer and waiting queues.

### Log Files

By default dCache is configured to only log information if something unexpected happens. However, to get familiar with Tertiary Storage System interactions you might be interested in more details. This section provides advice on how to obtain this kind of information.

#### The executable log file

Since you provide the `executable`, interfacing dCache and the TSS, it is in your responsibility to ensure sufficient logging information to be able to trace possible problems with either dCache or the TSS. Each request should be printed with the full set of parameters it receives, together with a timestamp. Furthermore information returned to dCache should be reported.

#### dCache log files in general

In dCache, each domain (e.g. dCacheDomain, <pool>Domain etc) prints
logging information into its own log file named after the domain. The
location of those log files it typically the `/var/log` or
`/var/log/dCache` directory depending on the individual
configuration. In the default logging setup only errors are
reported. This behavior can be changed by either modifying
`/etc/dcache/logback.xml` or using the dCache CLI to increase the log
level of particular components as described
[below](#obtain-information-via-the-dcache-command-line-admin-interface).

##### Increase the dCache log level by changes in `/etc/dcache/logback.xml`

If you intend to increase the log level of all components on a
particular host you would need to change the `/etc/dcache/logback.xml`
file as described below. dCache components need to be restarted to
activate the changes.

    <threshold>
         <appender>stdout</appender>
         <logger>root</logger>
         <level>warn</level>
       </threshold>

needs to be changed to

    <threshold>
         <appender>stdout</appender>
         <logger>root</logger>
         <level>info</level>
       </threshold>

> **Important**
>
> The change might result in a significant increase in log messages. So don't forget to change back before starting production operation. The next section describes how to change the log level in a running system.

##### Increase the dCache log level via the Command Line Admin Interface

Example:

Login into the dCache Command Line Admin Interface and increase the log level of a particular service, for instance for the `poolmanager` service:

    [example.dcache.org] (local) admin > \c PoolManager
    [example.dcache.org] (PoolManager) admin > log set stdout ROOT INFO
    [example.dcache.org] (PoolManager) admin > log ls
    stdout:
     ROOT=INFO
     dmg.cells.nucleus=WARN*
     logger.org.dcache.cells.messages=ERROR*
     .....

### Obtain information via the dCache Command Line Admin Interface

The dCache Command Line Admin Interface gives access to information describing the process of storing and fetching files to and from the TSS, as there are:

-   The
    *Pool Manager Restore Queue*. A list of all requests which have been issued to all pools for a `FETCH FILE` operation from the TSS (rc ls)
-   The *Pool Collector Queue*. A list of files, per pool and storage group, which will be scheduled for a `STORE FILE` operation as soon as the configured trigger criteria match.
-   The *Pool STORE FILE*  Queue. A list of files per pool, scheduled for the `STORE FILE` operation. A configurable amount of requests within this queue are active, which is equivalent to the number of concurrent store processes, the rest is inactive, waiting to become active.
-   The Pool *FETCH FILE* Queue. A list of files per pool, scheduled for the `FETCH FILE` operation. A configurable amount of requests within this queue are active, which is equivalent to the number of concurrent fetch processes, the rest is inactive, waiting to become active.

For evaluation purposes, the *pinboard* of each component can be used to track down dCache behavior. The *pinboard* only keeps the most recent 200 lines of log information but reports not only errors but informational messages as well.

Check the pinboard of a service, here the POOLMNGR service.

Example:

    [example.dcache.org] (local) admin > \c PoolManager
    [example.dcache.org] (PoolManager) admin > show pinboard 100
    08.30.45  [Thread-7] [pool_1 PoolManagerPoolUp] sendPoolStatusRelay: ...
    08.30.59  [writeHandler] [NFSv41-dcachetogo PoolMgrSelectWritePool ...
    ....

Example:

The **PoolManager** Restore Queue.  Remove the file `test.root` with the pnfs-ID 00002A9282C2D7A147C68A327208173B81A6.

    [example.dcache.org] (pool_1) admin > rep rm  00002A9282C2D7A147C68A327208173B81A6

Request the file `test.root`

```console-user
dccp dcap://example.dcache.org:/data/test.root test.root
```

Check the PoolManager Restore Queue:

    [example.dcache.org] (local) admin > \c PoolManager
    [example.dcache.org] (PoolManager) admin > rc ls
    0000AB1260F474554142BA976D0ADAF78C6C@0.0.0.0/0.0.0.0-*/* m=1 r=0 [pool_1] [Staging 08.15 17:52:16] {0,}

Example:

**The Pool Collector Queue.**

  [example.dcache.org] (local) admin > \c pool_1
  [example.dcache.org] (pool_1) admin > queue ls -l queue
                       Name: chimera:alpha
                  Class@Hsm: chimera:alpha@osm
     Expiration rest/defined: -39 / 0   seconds
     Pending   rest/defined: 1 / 0
     Size      rest/defined: 877480 / 0
     Active Store Procs.   :  0
      00001BC6D76570A74534969FD72220C31D5D


    [example.dcache.org] (local) admin > \c pool_1
    Class                 Active   Error  Last/min  Requests    Failed
    dteam:STATIC@osm           0       0         0         1         0

Example:

**The pool STORE FILE Queue.**

    [example.dcache.org] (local) admin > \c pool_1
    [example.dcache.org] (pool_1) admin > st ls
    0000EC3A4BFCA8E14755AE4E3B5639B155F9  1   Fri Aug 12 15:35:58 CEST 2011

Example:

**The pool FETCH FILE Queue.**

    [example.dcache.org] (local) admin > \c pool_1
    [example.dcache.org] (pool_1) admin >  rh ls
    0000B56B7AFE71C14BDA9426BBF1384CA4B0  0   Fri Aug 12 15:38:33 CEST 2011

To check the repository on the pools run the command `rep ls` that is described in the beginning of [the section called “How to Store-/Restore files via the Admin Interface”.](#how-to-store-restore-files-via-the-admin-interface)


# Example of an EXECUTABLE to simulate a tape backend

```shell
#!/bin/sh
#
#set -x
#
logFile=/tmp/hsm.log
#
################################################################
#
#  Some helper functions
#
##.........................................
#
# print usage
#
usage() {
   echo "Usage : put|get <pnfsId> <filePath> [-si=<storageInfo>] [-key[=value] ...]" 1>&2
}
##.........................................
#
#
printout() {
#---------
   echo "$pnfsid : $1" >>${logFile}
   return 0
}
##.........................................
#
#  print error into log file and to stdout.
#
printerror() {
#---------
if [ -z "$pnfsid" ] ; then
#      pp="000000000000000000000000000000000000"
      pp="------------------------------------"
   else
      pp=$pnfsid
   fi

   echo "$pp : (E) : $*" >>${logFile}
   echo "$pp : $*" 1>&2

}
##.........................................
#
#  find a key in the storage info
#
findKeyInStorageInfo() {
#-------------------

   result=`echo $si  | awk  -v hallo=$1 -F\; '{ for(i=1;i<=NF;i++){ split($i,a,"=") ; if( a[1] == hallo )print a[2]} }'`
   if [ -z "$result" ] ; then return 1 ; fi
   echo $result
   exit 0

}
##.........................................
#
#  find a key in the storage info
#
printStorageInfo() {
#-------------------
   printout "storageinfo.StoreName : $storeName"
   printout "storageinfo.store : $store"
   printout "storageinfo.group : $group"
   printout "storageinfo.hsm   : $hsmName"
   printout "storageinfo.accessLatency   : $accessLatency"
   printout "storageinfo.retentionPolicy : $retentionPolicy"
   return 0
}
##.........................................
#
#  assign storage info the keywords
#
assignStorageInfo() {
#-------------------

    store=`findKeyInStorageInfo "store"`
    group=`findKeyInStorageInfo "group"`
    storeName=`findKeyInStorageInfo "StoreName"`
    hsmName=`findKeyInStorageInfo "hsm"`
    accessLatency=`findKeyInStorageInfo "accessLatency"`
    retentionPolicy=`findKeyInStorageInfo "retentionPolicy"`
    return 0
}
##.........................................
#
# split the arguments into the options -<key>=<value> and the
# positional arguments.
#
splitArguments() {
#----------------
#
  args=""
  while [ $# -gt 0 ] ; do
    if expr "$1" : "-.*" >/dev/null ; then
       a=`expr "$1" : "-\(.*\)" 2>/dev/null`
       key=`echo "$a" | awk -F= '{print $1}' 2>/dev/null`
       value=`echo "$a" | awk -F= '{for(i=2;i<NF;i++)x=x $i "=" ; x=x $NF ; print x }' 2>/dev/null`
       if [ -z "$value" ] ; then a="${key}=" ; fi
       eval "${key}=\"${value}\""
       a="export ${key}"
       eval "$a"
    else
       args="${args} $1"
    fi
    shift 1
  done
  if [ ! -z "$args" ] ; then
     set `echo "$args" | awk '{ for(i=1;i<=NF;i++)print $i }'`
  fi
  return 0
}
#
#
##.........................................
#
splitUri() {
#----------------
#
  uri_hsmName=`expr "$1" : "\(.*\)\:.*"`
  uri_hsmInstance=`expr "$1" : ".*\:\/\/\(.*\)\/.*"`
  uri_store=`expr "$1" : ".*\/\?store=\(.*\)&group.*"`
  uri_group=`expr "$1" : ".*group=\(.*\)&bfid.*"`
  uri_bfid=`expr "$1" : ".*bfid=\(.*\)"`
#
  if [  \( -z "${uri_store}" \) -o \( -z "${uri_group}" \) -o \(  -z "${uri_bfid}" \) \
     -o \( -z "${uri_hsmName}" \) -o \( -z "${uri_hsmInstance}" \) ] ; then
     printerror "Illegal URI formal : $1"
     return 1
  fi
  return 0

}
#########################################################
#
echo "--------- $* `date`" >>${logFile}
#
#########################################################
#
createEnvironment() {

   if [ -z "${hsmBase}" ] ; then
      printerror "hsmBase not set, can't continue"
      return 1
   fi
   BASE=${hsmBase}/data
   if [ ! -d ${BASE} ] ; then
      printerror "${BASE} is not a directory or doesn't exist"
      return 1
   fi
}
##
#----------------------------------------------------------
doTheGetFile() {

   splitUri $1
   [ $? -ne 0 ] && return 1

   createEnvironment
   [ $? -ne 0 ] && return 1

   pnfsdir=${BASE}/$uri_hsmName/${uri_store}/${uri_group}
   pnfsfile=${pnfsdir}/$pnfsid

   cp $pnfsfile $filename 2>/dev/null
   if [ $? -ne 0 ] ; then
      printerror "Couldn't copy file $pnfsfile to $filename"
      return 1
   fi

   return 0
}
##
#----------------------------------------------------------
doTheStoreFile() {

   splitUri $1
   [ $? -ne 0 ] && return 1

   createEnvironment
   [ $? -ne 0 ] && return 1

   pnfsdir=${BASE}/$hsmName/${store}/${group}
   mkdir -p ${pnfsdir} 2>/dev/null
   if [ $? -ne 0 ] ; then
      printerror "Couldn't create $pnfsdir"
      return 1
   fi
   pnfsfile=${pnfsdir}/$pnfsid

   cp $filename $pnfsfile 2>/dev/null
   if [ $? -ne 0 ] ; then
      printerror "Couldn't copy file $filename to $pnfsfile"
      return 1
   fi

   return 0

}
##
#----------------------------------------------------------
doTheRemoveFile() {

   splitUri $1
   [ $? -ne 0 ] && return 1

   createEnvironment
   [ $? -ne 0 ] && return 1

   pnfsdir=${BASE}/$uri_hsmName/${uri_store}/${uri_group}
   pnfsfile=${pnfsdir}/$uri_bfid

   rm $pnfsfile 2>/dev/null
   if [ $? -ne 0 ] ; then
      printerror "Couldn't remove file $pnfsfile"
      return 1
   fi

   return 0
}
#########################################################
#
#  split arguments
#
  args=""
  while [ $# -gt 0 ] ; do
    if expr "$1" : "-.*" >/dev/null ; then
       a=`expr "$1" : "-\(.*\)" 2>/dev/null`
       key=`echo "$a" | awk -F= '{print $1}' 2>/dev/null`
         value=`echo "$a" | awk -F= '{for(i=2;i<NF;i++)x=x $i "=" ; x=x $NF ; print x }' 2>/dev/null`
       if [ -z "$value" ] ; then a="${key}=" ; fi
       eval "${key}=\"${value}\""
       a="export ${key}"
       eval "$a"
    else
       args="${args} $1"
    fi
    shift 1
  done
  if [ ! -z "$args" ] ; then
     set `echo "$args" | awk '{ for(i=1;i<=NF;i++)print $i }'`
  fi
#
#
if [ $# -lt 1 ] ; then
    printerror "Not enough arguments : ... put/get/remove ..."
    exit 1
fi
#
command=$1
pnfsid=$2
#
# !!!!!!  Hides a bug in the dCache HSM remove
#
if [ "$command" = "remove" ] ; then pnfsid="000000000000000000000000000000000000" ; fi
#
#
printout "Request for $command started `date`"
#
################################################################
#
if [ "$command" = "put" ] ; then
#
################################################################
#
  filename=$3
#
  if [ -z "$si" ] ; then
     printerror "StorageInfo (si) not found in put command"
     exit 5
  fi
#
  assignStorageInfo
#
  printStorageInfo
#
  if [ \( -z "${store}" \) -o \( -z "${group}" \) -o \( -z "${hsmName}" \) ] ; then
     printerror "Didn't get enough information to flush : hsmName = $hsmName store=$store group=$group pnfsid=$pnfsid "
     exit 3
  fi
#
  uri="$hsmName://$hsmName/?store=${store}&group=${group}&bfid=${pnfsid}"

  printout "Created identifier : $uri"

  doTheStoreFile $uri
  rc=$?
  if [ $rc -eq 0 ] ; then echo $uri ; fi

  printout "Request 'put' finished at `date` with return code $rc"
  exit $rc
#
#
################################################################
#
elif [ "$command" = "get"  ] ; then
#
################################################################
#
  filename=$3
  if [ -z "$uri" ] ; then
     printerror "Uri not found in arguments"
     exit 3
  fi
#
  printout "Got identifier : $uri"
#
  doTheGetFile $uri
  rc=$?
  printout "Request 'get' finished at `date` with return code $rc"
  exit $rc
#https://onedio.com/haber/abd-yi-karistiran-kendall-jenner-li-pepsi-reklami-765067
################################################################
#
elif [ "$command" = "remove" ] ; then
#
################################################################
#
   if [ -z "$uri" ] ; then
      printerror "Illegal Argument error : URI not specified"
      exit 4
   fi
#
   printout "Remove uri = $uri"
   doTheRemoveFile $uri
   rc=$?
#
   printout "Request 'remove' finished at `date` with return code $rc"
   exit $rc
#
else
#
   printerror "Expected command : put/get/remove , found : $command"
   exit 1
#
fi
```

  [EXECUTABLE]: #tss-executable
  [return code]: #cf-tss-support-return-codes
  [section\_title]: #cf-tss-pools-layout-setup
  [1]: #cf-tss-support-clo
  [above]: #cf-tss-support-storage-uri
  [below]: #cf-tss-monitor-log-cli
  [2]: #cf-tss-pools-admin
