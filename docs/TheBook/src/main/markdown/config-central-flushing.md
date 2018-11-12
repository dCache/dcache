Central Flushing to tertiary storage systems
============================================

This chapter is of interest for dCache instances connected to a tertiary storage system or making use of the mass storage interface for any other reason. 

> **Warning**
>
> The central flush control is still in the evaluation phase. The configuration description within this chapter is mainly for the dCache team to get it running on their test systems. The final prodution version will have most of this stuff already be configured.

dCache instances, connected to tertiary storage systems, collect incoming data, sort it by storage class and flush it as soon as certain thresholds are reached. All this is done autonomously by each individual write pool. Consequently those flush operations are coordinated on the level of a pool but not globally wrt a set of write pools or even to the whole dCache instance. Experiences during the last years show, that for various purposes a global flush management would be desirable.

> The total thoughput of various disk storage systems tend to drop significantly if extensive read and write operations have to be performed in parallel on datasets exceeding the filesystem caches. To overcome this technical obstacle, it would be good if disk storage systems would either allow writing into a pool or flushing data out of a pool into the HSM system, but never both at the same time.

> Some HSM systems, mainly those not coming with their own scheduler, apply certain restrictions on the number of requests being accepted simultaniously. For those, a central flush control system would allow for limiting the number of requests or the number of storage classes being flushed at the same time.

Basic configuration (Getting it to run)
=======================================

This section describes how to setup a central flush control manager.

-   Whitin the CELL-POOLMNGR, a pool-group (flushPoolGroup) has to be created and populated with pools planned to be controlled by the central flush mechanism. An arbitrary number of flush control managers may run within the same dCache instance as long as each can work on its own pool-group and no pool is member of more than one flushPoolGroup.

-   To start the flush control system, an corresponding dCache batch file has to be setup, installed and started. As input parameter, the CELL-HSMFLUSHCTL cell needs the name of the flushPoolGroup) and the name of the driver, controlling the flush behaviour. Within the same batch file more than one flush control manager may be started as long as they get different cell-names and different pool-groups assigned.

-   The flush control web pages have to be defined in the `httpd.batch`.

Creating the flush pool group
-----------------------------

Creating flushPoolGroup and adding pools is done within the `config/PoolManager.config` setup file or using the CELL-POOLMNGR command line interface. Pools may be member of other pool-groups, as long as those pool-groups are not managed by other flush control managers.

    psu create pool pool-1
    psu create pool ...
    #
    psu create pgroup flushPoolGroup
    #
    psu addto pgroup flushPoolGroup  pool-1
    psu addto pgroup flushPoolGroup  ...
    #

Creating and activating the hsmcontrol batch file
-------------------------------------------------

    #
    set printout default errors
    set printout CellGlue none
    #
    onerror shutdown
    #
    check -strong setupFile
    #
    copy file:${setupFile} context:setupContext
    #
    import context -c setupContext
    #
    check -strong serviceLocatorHost serviceLocatorPort
    #
    create dmg.cells.services.RoutingManager  RoutingMgr
    #
    create dmg.cells.services.LocationManager lm \
         "${serviceLocatorHost} ${serviceLocatorPort}"
    #
    create diskCacheV111.hsmControl.flush.HsmFlushControlManager FlushManagerName  \
            "flushPoolGroup  \
             -export   -replyObject \
             -scheduler=SchedulerName  \
             Scheduler specific options \
            "
    #

Which the following meaning of the variables :

-   flushPoolGroup needs to be the name of the pool group defined in the `PoolManager.conf` files.

-   SchedulerName is the name of a class implementing the `diskCacheV111.hsmControl.flush.HsmFlushSchedulable` interface.

-   Scheduler specific options may be options specific to the selected scheduler.

Initially there are three schedulers available :

-   `diskCacheV111.hsmControl.flush.driver.HandlerExample` may be used as an example implementation of the HsmFlushScheduler interface. The functionality is useless in an production environment but can be useful to check the functionality of the central flush framework. If one allows this driver to take over control it will initiate the flushing of data as soon as it becomes aware of it. One the other hand it supports a mode where is doesn't do anything except preventing the individual pools from doing the flush autonomously. In that mode, the driver assumes the flushes to be steered manually by the flush web pages decribed in the next paragraph. The latter mode is enabled by starting the flush driver with the Scheduler specific options set to `-do-nothing`

-   `diskCacheV111.hsmControl.flush.driver.AlternateFlush` is intended to provide suffient functionality to cope with issues described in the introduction of the paragraph. Still quite some code and knowledge has to go into this driver.

-   `diskCacheV111.hsmControl.flush.driver.AlternatingFlushSchedulerV1` is certainly the most useful driver. It can be configured to flush all pools on a single machine simultaniously. It is trigger by space consumption, number of files within a pool or the time the oldest file resides on a pool without having been flushed. Please checkout the next section for details on configuration and usage.

The AlternatingFlushSchedulerV1 driver
======================================

The AlternatingFlushSchedulerV1 is an alternating driver, which essentially means that it either allows data to flow into a pool, or data going from a pool onto an HSM system but never both at the same time. Data transfers from pools to other pools or from pools to clients are not controlled by this driver. In order to minimize the latter one should configure HSM write pools to not allow transfers to clients but doing pool to pool transfers first.

Configuration
-------------

    #
    create diskCacheV111.hsmControl.flush.HsmFlushControlManager FlushManagerName  \
            "flushPoolGroup  \
             -export   -replyObject \
             -scheduler=diskCacheV111.hsmControl.flush.driver.AlternatingFlushSchedulerV1  \
             -driver-config-file=${config}/flushDriverConfigFile \
            "
    #

Where flushPoolGroup is a PoolGroup defined in the `PoolManager.conf` file, containing all pools which are intended to be managed by this FlushManager. flushDriverConfigFile is a file within the dCache `config` directory holding property values for this driver. The driver reloads the file whenever it changes its modification time. One should allow for a minute of two before new setting are getting activated. The configuration file has to contain key value pairs, separated by the = sign. Keys, not corresponding to a driver property are silently ignored. Properties, not set in the configuration file, are set to some reasonable default value.

Properties
----------

Driver properties may be specified by a configuration file as described above or by talking to the driver directly using the command line interface. Driver property commands look like :

    driver properties -PropertyName=value

Because the communication with the driver is asynchronous, this command will never return an error. To check if the new property value has been accepted by the driver, run the sequence

                driver properties
                info

It will list all available properties together with the currently active values.

<table>
<caption>Driver Properties</caption>
<colgroup>
<col width="27%" />
<col width="18%" />
<col width="54%" />
</colgroup>
<thead>
<tr class="header">
<th>Property Name</th>
<th>Default Value</th>
<th>Meaning</th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td>max.files</td>
<td>500</td>
<td>Collect this number of files per pool, before flushing</td>
</tr>
<tr class="even">
<td>max.minutes</td>
<td>120</td>
<td>Collect data for this amount of minutes before flushing</td>
</tr>
<tr class="odd">
<td>max.megabytes</td>
<td>500 * 1024</td>
<td>Collecto this number of megabytes per pool before flushing</td>
</tr>
<tr class="even">
<td>max.rdonly.fraction</td>
<td>0.5</td>
<td>Do not allow more than this percentage of pools to be set read only</td>
</tr>
<tr class="odd">
<td>flush.atonce</td>
<td>0</td>
<td>Never flush more than that in one junk</td>
</tr>
<tr class="even">
<td>timer</td>
<td>60</td>
<td>Interval timer (minimum resolution)</td>
</tr>
<tr class="odd">
<td>print.events</td>
<td>false</td>
<td>Print events delivered by the FlushManager</td>
</tr>
<tr class="even">
<td>print.rules</td>
<td>false</td>
<td>Print remarks from the rule engine</td>
</tr>
<tr class="odd">
<td>print.poolset.progress</td>
<td>false</td>
<td>Print progress messages</td>
</tr>
</tbody>
</table>

The selection process
---------------------

> A pool is becoming a flush candidate if either the number of files collected exceeds `max.files` or the number of megabytes collected exceeds `max.megabytes` or the oldest file, not flushed yet, is becoming older than `max.minutes`.

> Pool Candidates are sorted according to a metric, which is essentially the sum of three items. The number of files devided by `max.files`, the number of megabytes devided by `max.megabytes` and the age of the oldest file devided by `max.minutes`.
>
> The pool with the highest metric is chosen first. The driver determines the hardware unit, this pools resides on. The intention is to flush all pools of this unit simultanionsly. Depending on the configuration, the unit can be either a disk partition or a host. After the hardware unit is determined, the driver adds the number of pools on that unit to the number of pools already in 'read only' mode. If this sum exceeds the total number of pools in the flush pool group, multiplied by the `max.rdonly.fraction` property, the pool is NOT selected. The process proceeds until a pool, resp. a hardware unit complies with these contrains.
>
> The hardware unit, a pool belongs to, is set by the 'tag.hostname' field in the `config/hostname` file.

> If a pool is flushed, all storage groups of that pool are flushed, and within each storage group all precious files are flushed simultaniously. Setting the property `flush.atonce` to some positive nonzero number will advise each storage group not to flush more than this number of files per flush operation. There is no way to stop a flush operation which has been triggered by the FlushManager. The pool will proceed until all files, belonging to this flush operation, have been successfully flushed or failed to flush. Though, the next section describes how to suspend the flush pool selection mechanism.

Suspending and resuming flush operations
----------------------------------------

The driver can be advised to suspend all new flush operations and switch to halt mode.

    driver command suspend

To resume flushing :

    driver command resume

In suspend mode, all flushing is halted which sooner or later results in overflowing write pools.

Driver interactions with the flush web portal or the GUI
--------------------------------------------------------

Flush Manager operations can be visualized by configuring the flush web pages, described in one of the subsequent sections or by using the flush module of the 'org.pcells' GUI. In addition to monitoring, both mechanisms allow to set the pool I/O mode (rdOnly, readWrite) and to flush individual storage groups or pools. The problem may be that those manual interactions interfere with driver operations. The AlternatingFlushSchedulerV1 tries to cope with manual interactions as follows :

-   The pool I/O mode may be manually set to `read
    	     only ` while the pool is not flushing data and therefor naturally would be in read write mode. If this pool is then subsequently chosen for flushing, and the flushing process has finished, the pool is NOT set back to readWrite mode, as it usually would be, but it stays in readOnly mode, because the driver found this mode when starting the flush process and assumes that it had been in that mode for good reason. So, setting the pool I/O mode to readOnly while the pool is not flushing freezes this mode until manually changed again. Setting the I/O mode to readOnly while the pool is flushing, has no effect.

-   If a pool is in readOnly mode because the driver has been initiating a flush process, and the pool is manually set back to readWrite mode, is stays in readWrite mode during this flush process. After the flush sequence has finished, the pool is set back to normal as if no manual intervention had taken place. It does *not* stay with readWrite mode forever as it stays in readOnly mode forever in the example above.

When using the web interface or the GUI for flushing pools or individual storage groups, one is responsible for setting the pool I/O mode oneself.

Setting up and using the flush control web pages.
=================================================

In order to keep track on the flush activities the flush control web pages need to be activated. Add a new `set
       alias` directive somewhere between the `define
       context httpdSetup endDefine` and the `endDefine` command in the `PATH-ODS-USD/services/httpd.batch` file.

    define context httpdSetup endDefine
    ...
    set alias flushManager class diskCacheV111.hsmControl.flush.HttpHsmFlushMgrEngineV1 mgr=FlushManagerName
    ...
    endDefine

Additional flush managers may just be added to this command, separated by commas. After restarting the 'httpd' service, the flush control pages are available at `http://headnode:2288/flushManager/mgr/*`.

The flush control web page is split into 5 parts. The top part is a switchboard, pointing to the different flush control managers installed. (listed in the mgr= option of the `set alias flushManager` in the `config/httpd.config`). The top menu is followed by a `reload` link. Its important to use this link instead of the 'browsers' reload button. The actual page consists of tree tables. The top one presents common configuration information. Initially this is the name of the flush cell, the name of the driver and whether the flush controller has actually taken over control or not. Two action buttons allow to switch between centrally and locally controlled flushing. The second table lists all pools managed by this controller. Information is provided on the pool mode (readonly vers. readwrite), the number of flushing storage classes, the total size of the pool and the amount of precious space per pool. Action buttons allow to toggle individual pools between `ReadOnly` and `ReadWrite` mode. Finally the third table presents all storage classes currently holding data to be flushed. Per storage class and pool, characteristic properties are listed, like total size, precious size, active and pending files. Here as well, an action button allows to flush individual storage classes on individual pools.

> **Warning**
>
> The possibilty to interactively interact with the flush manager needs to be supported by the driver choosen. Please check the information on the individual driver how far this is supported.

Examples
========

Configuring Central Flushing for a single Pool Group with the AlternatingFlushSchedulerV1 driver
------------------------------------------------------------------------------------------------

> Add all pools, which are planned to be centrally flushed to a PoolGroup, lets say `flushPoolGroup` :
>
>     psu create pool migration-pool-1
>     psu create pool migration-pool-2
>     #
>     psu create pgroup flushPoolGroup
>     #
>     psu addto pgroup flushPoolGroup migration-pool-1
>     psu addto pgroup flushPoolGroup migration-pool-2
>     #

> Create a batchfile `PATH-ODS-USD/services/hsmcontrol.batch` with the following content :
>
>     #
>     set printout default 3
>     set printout CellGlue none
>     #
>     onerror shutdown
>     #
>     check -strong setupFile
>     #
>     copy file:${setupFile} context:setupContext
>     #
>     import context -c setupContext
>     #
>     check -strong serviceLocatorHost serviceLocatorPort
>     #
>     create dmg.cells.services.RoutingManager  RoutingMgr
>     #
>     create dmg.cells.services.LocationManager lm \
>          "${serviceLocatorHost} ${serviceLocatorPort}"
>     #
>     create diskCacheV111.hsmControl.flush.HsmFlushControlManager FlushManager  \
>             "flushPoolGroup \
>              -export   -replyObject \
>              -scheduler=diskCacheV111.hsmControl.flush.driver.AlternatingFlushSchedulerV1  \
>               -driver-config-file=${config}/flushPoolGroup.conf \
>             "
>     #
>
> Change to `/opt/d-cache/jobs` and run `./initPackage.sh`. Ignore possible warnings and error messages. The Script will create the necessary links, mainly the `jobs/hsmcontrol` startup file. To start the central service run
>
>     cd /opt/d-cache/jobs
>     ./hsmcontrol start
>
> This setup will produce quite some output in `/var/log/hsmcontrol.log`. Reduce the output level if this is not required.
>
>     set printout default errors

> Create a file in `/opt/d-cache/config` named `flushPoolGroup.conf` with the content listed below. You may change the content any time. The driver will reload it after awhile.
>
>     #
>     #  trigger parameter
>     #
>     max.files=4
>     max.minutes=10
>     max.megabytes=200
>     #
>     #  time interval between rule evaluation
>     #
>     timer=60
>     #
>     # which fraction of the pool set should be rdOnly (maximum)
>     #
>     max.rdonly.fraction=0.999
>     #
>     #  output steering
>     #
>     print.events=true
>     print.rules=true
>     print.pool.progress=true
>     print.poolset.progress=true
>     mode=auto
