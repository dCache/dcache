The New Resilience Service
--------------------------

With version 2.16, dCache introduced a new service for creating and managing
multiple permanent copies of a file. This **resilience** service is intended to
replace the **replica (manager)** service (see the [dCache Book, Chapter
II.6](config-ReplicaManager.md)). While the latter continues to remain available
in 2.16, it is considered deprecated. Users are encouraged to adopt the
resilience service as soon as possible, as the replica service will eventually
be eliminated.

For migration from the replica manager to resilience, see
[below](https://github.com/dCache/dcache/wiki/Resilience#migrating-from-the-old-replica-manager-to-the-new-service).

### The (original) Replica Manager: history and limitations

From early on in its development, dCache has sought to provide for file
durability and availability in the absence of a tertiary storage system. The
replica manager, or replica service, was created for this purpose. It used
pool-to-pool (`p2p`) copying to guarantee that the number of locations, or
"replicas", for new files met at least a default minimum (2). It would maintain
this minimum by creating additional copies should one or more pools containing
that file become unreadable, and by eliminating redundant replicas when the old
locations once again became accessible. The manager relied on a special pool
group in order to distinguish pools under its control from the rest of the
dCache installation; thanks to this partitioning, dCache could be run in what
was referred to as "hybrid" mode, with replication coexisting alongside back-end
storage and retrieval.

While improvements and fixes have been applied to the Replica Manager, there has
been no significant modification since 2007. In particular, this service
utilizes a rather heavyweight set of database tables; with the subsequent
adoption of Chimera, much of the data stored by the Replica Manager now ends up
duplicating what is stored in the namespace. Moreover, the Replica Manager's
tables are populated by querying the pool repositories, which is not as reliable
a source of information.

The manager handled only `precious` files (only these are considered "countable"
replicas); to prevent such files from becoming `cached`, it further required the
setting `lfs=precious` on all pools in the "resilient" pool group under its
control. This pool setting, however, has now been superseded by the
`RetentionPolicy` and `AccessLatency` concepts. It is thus a legacy option and
replication should not be based on it; furthermore, defining replication in
terms of pool configuration is intrinsically incorrect to begin with.

Finally, there is a certain brittleness both in the way the manager views
replication requirements, and in its ability to satisfy them. First, replication
is determined by a global range (minimum \<= N \<= maximum) for *all* files in
the pool group. Second, the manager does not handle more than one such group. To
simulate the existence of multiple such groups, it is necessary to run as many
managers as the pool groups one wishes to make resilient.

### Resilience: A complete redesign

Since there continues to be a need to provide for file replication inside of
dCache, and to situate it within a broader quality-of-service framework, it was
deemed desirable to re-evaluate how replication should be defined and managed.
These considerations led to the decision to re-implement this service from
scratch. In order to distinguish it from the older service, we call it simply
"Resilience".

#### Principal Features

1.  There is no database to maintain.

2.  A single Resilience instance handles multiple sets of resilience
    constraints.

3.  No special pool configuration (other than including a pool in a resilient
    pool group) is needed.

    1.  pools do not require `lfs=precious` to work.

    2.  replicas are required to be `ONLINE`, not `precious`, and are not
        prohibited from being written to pools which are also backed by a
        tertiary storage system.

These three features fundamentally distinguish the new resilience service from
the old Replica Manager.

The new service continues to exercise the same vigilance over file location
accessibility as before:

#### Retained Features

1.  New files are handled by intercepting cache location messages relayed by
    PnfsManager.

2.  Pool status changes trigger either the creation of missing copies or the
    removal of redundant ones.

3.  The pools in the resilient groups are periodically scanned in order to
    assure replica consistency and to retry copying of any files which may not
    have been replicated previously.

4.  In-flight or queued file operations are recoverable if the service itself
    goes offline and is then restarted.

5.  Pools can be temporarily excluded from resilience counts and handling (see
    further below on this feature).

6.  now takes place by comparing the new state of the PoolMonitor, pushed to the
    resilience service by the PoolManager every 30 seconds, with the previous
    one.

The following features are either extensions or refinements of old ones:

#### Extended Features

1.  Resilience constraints are now defined in terms of **storage units**, not
    pool groups:

    1.  The `required` attribute is used to establish the number of copies for
        all files in that unit.

    2.  The `onlyOneCopyPer` attribute is used to partition copies among pools
        on the basis of pool tags.

2.  Resilience constraints, along with pool membership in resilient groups, can
    be redefined without requiring a service restart.

3.  Broken or corrupt replicas are handled by removal and recopying when this is
    possible.

4.  An alarm is raised when there is a fatal replication error.

5.  Copies resulting in non-fatal failures can be retried (this is
    configurable).

6.  A rich set of admin commands for monitoring and diagnostics, as well as for
    manually adjusting replicas or initiating pool scans, is provided.

The first point is treated in more detail below; (1.ii) is a generalization of
the Replica Manager's global property for disallowing multiple copies of a file
on the same host. With regard to (2), changes in the composition of pool groups
or to the constraints expressed for a storage unit will trigger a re-scan of the
pools involved in the change (unless those pools are marked `EXCLUDED`). If
changes to (1.ii) are made, this may also trigger a redistribution of files in
the storage unit. Retrying of failed copies is best-effort, and is repeated
during periodic pool scanning should the original retry also fail.

#### Some remarks on implementation

As before, Resilience is a self-contained dCache cell or service which may be
run in the same domain as other services, or by itself in a separate domain. It
can also be disabled and re-enabled via an admin command without stopping and
restarting the entire domain.

The new service is more fully integrated with other dCache services. As noted
above, it now has access to the entire PoolManager state through periodic
updates from that service. It also accesses the namespace (Chimera) database
directly for information on file locations. For `p2p` copying, it makes use of
the full-featured support furnished by the migration module which runs on the
pools. Running Resilience does not, however, affect the performance of other
services such as PnfsManager and PoolManager, whose efficiency is crucial to the
normal functioning of a dCache installation.

In re-implementing this service, scalability has been a foremost concern,
particularly since it now keeps all state in memory. The service has been
repeatedly stress tested on an installation of approximately 100 pools, with the
number of files on each reaching more than 1 million, and ingest rates exceeding
600HZ. With the increase in number of operations, performance has not been
observed to degrade due to internal factors, though it is susceptible, as would
be expected, to such things as the size of the namespace, load on the pools and
network latency.

We have also paid some attention to questions of fairness and anti-starvation.
For a large system, a pool scan can queue up millions of operations. We have
attempted to balance work between the handling of these scanned files and newly
created ones, such that some progress is always being made on both. We have also
adopted the policy of preferring the availability of at least two copies,
promoting to the head of the waiting list operations on files with currently
only one replica, and re-queueing the operation for any additional copies
required.

Since state is maintained in memory, we have implemented periodic check-pointing
of the main operation table to a file which can be read back in at startup. To
avoid costly locking, this check-pointing is not guaranteed to be "lossless",
but this is compensated for by the periodic verification done on the pools.

##### Operation concurrency

1.  File operations of course run concurrently; however, operations are
    serialized with respect to logical file (`pnfsid`) – that is, only one copy
    or remove for a given file id is running at a given time.

2.  Pool scans also run concurrently. When separate pool scans request
    operations on the same file, the operation count for that file is simply
    incremented. File information and pool information is refreshed for each
    pass of the operation, so the operation will only run as many times as is
    necessary to fulfill the resilience requirements for that file.

3.  For both file and pool operations, there are obviously diminishing returns
    on performance if the concurrency is set too high. An explanation of the
    relevant parameters is given in the `resilience.properties` file, should
    further tuning be necessary or desirable (defaults are already set based on
    the benchmark testing we have conducted).

### Configuring the new resilience service

#### Activating resilience

The service can be run out of the box. All that is required is to include it in
some domain.

Resilience communicates directly with Chimera, so `chimera.db.host` should be
set explicitly if Resilience is not running on the same host as the database.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
[someDomain/resilience]
chimera.db.host=<host-where-chimera-runs>
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#### Memory requirements

While it is possible to run Resilience in the same domain as other services,
memory requirements for resilience handling are fairly substantial, particularly
for large installations.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 BEST PRACTICE: We recommend at least 8GB of JVM heap be allocated; 16GB is preferable.
                Be sure to allow enough memory for the entire domain.
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Whenever feasible, it is recommended to give Resilience its own domain.

#### Some definitions

A **resilient file** is one whose `AccessLatency` is `ONLINE` and which belongs
to a **resilient storage unit**.

A **resilient storage unit** is one where the `required` (copies) attribute is
set to a value greater than 1 (the default).

A **resilient storage unit** must be linked to a **resilient pool group** for it
to be acted upon. When linked to a pool group which is not resilient, the
storage unit requirements will be invisible to Resilience.

Resilient replicas (belonging to **resilient storage unit**s) will, for this
reason, only be found on **resilient pools** (belonging to a **resilient pool
group**), though not all files on resilient pools are necessarily resilient (see
below concerning [shared
pools](https://github.com/dCache/dcache/wiki/Resilience#pool-sharing)).

Note that a file's `RetentionPolicy` is not limited to `REPLICA` in order for it
to qualify as resilient; one may have `CUSTODIAL` files which are also given
permanent on-disk copies (the 'Own Cloud' dCache instance running at DESY uses
this configuration to maintain both hard replicas and a tape copy for each
file).

#### Setting up resilience

To have a fully functioning resilient installation, you must take the following
steps:

1.  Define one or more resilient pool groups.

2.  Define one or more resilient storage units, and link them to a resilient
    group or groups.

3.  Create the namespace directories where these files will reside, with the
    necessary tags.

##### Defining a resilient pool group

To make a pool group resilient, simply add the '`-resilient`' flag; e.g., in
`poolmanager.conf`:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
psu create pgroup resilient-group -resilient
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Once a pool group is defined as resilient, it will become "visible" to the
resilience service.

A pool may belong to **only one** resilient group, though it can belong to any
number of non-resilient groups as well. When the resilience service selects a
location for an additional copy, it does so from within the resilient pool group
of the file's source location.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
WARNING:  Be careful when redefining a pool group by removing its resilient flag.  
          (This is not possible, as a safety precaution, using an admin command; 
          it requires manual modification and reloading of the poolmanager 
          configuration file.)  Doing so makes all files on all pools in that 
          group no longer considered to be resilient;  they will, however, remain 
          indefinitely "pinned" or sticky, and thus not susceptible to garbage 
          collection.
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
BEST PRACTICE:  Drain pools in a resilient pool group before removing the 
                group's resilience flag.
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

##### Defining a resilient storage unit

There are two attributes which pertain to resilience.

1.  `required` defines the number of copies files of this unit should receive
    (default is 1).

2.  `onlyOneCopyPer` takes a comma-delimited list of pool tag names; it
    indicates that copies must be partitioned among pools such that each replica
    has a distinct value for each of the tag names in the list (default is
    undefined).

To create, for instance, a resilient storage unit requiring two copies, each on
a different host, one would do something like this:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
psu create unit -store test:resilient1@osm
psu set storage unit test:resilient1@osm -required=2 -onlyOneCopyPer=hostname
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This example assumes that the pool layout has also configured the pools with the
appropriate `hostname` tag and value.

##### Configuration Example

The following demonstrates the setup for a single resilient pool, pool group and
storage unit. The usual procedure for linking pools, pool groups and units
continues to apply.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
psu create unit -store test:resilient1@osm
psu set storage unit test:resilient1@osm -required=2 -onlyOneCopyPer=hostname

psu create ugroup resilient1-units
psu addto ugroup resilient1-units test:resilient1@osm

psu create pool resilient1-pool1

psu create pgroup resilient1-pools -resilient
psu addto pgroup resilient1-pools resilient1-pool1

psu create link resilient1-link resilient1-units ...
psu add link resilient1-link resilient1-pools
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

##### Setting the directory tags

To continue with the above example, the tags which would be minimally required
in the directories pointing to this storage unit are:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.(tag)(AccessLatency): ONLINE
.(tag)(sGroup): resilient1
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#### Pool Sharing

It is possible to share pools between a resilient group and a group backed by an
HSM. The files can be mixed, and the HSM files will not be replicated. This
diagram illustrates a simple example:

![Pool Sharing](https://github.com/alrossi/dcache-miscellany/blob/master/docs/Slide4.png)

Because Resilience checks whether the `AccessLatency` attribute is `ONLINE`, the
`NEARLINE` files belonging to `hsmunit` are ignored. This pertains to pool scans
as well. Thus 1A, 2A and 3A can contain both replicas and cached copies.

#### Cached files on resilient pools

On pools belonging to a resilient group, any copies which are simply `cached`
(without a permanent sticky bit set by the system) are assumed to be from files
whose `AccessLatency` is `NEARLINE` (residing on the pool as a consequence of
pool sharing). On the other hand, the resilience service checks that every
`ONLINE` file written to a resilient pool has a sticky bit owned by the system,
adding one if it doesn't already exist.

When a file is originally written to a pool, `ONLINE` actually translates into
"`--------X--`" in the pool repository, i.e., ... + (system-owned) `sticky`.
How, then, could a cached copy of an `ONLINE` file appear on a pool without this
sticky bit set in the first place?

-   by a manual p2p copy via an admin command (**why would you want to do
    that?**)

-   by replication across links

Pool-to-pool copying, especially of temporary replicas (such as during hot
replication), does not *necessarily* reproduce the source's sticky flags.
Resilience guards against leaving such files without a sticky bit, because
otherwise the counting procedure it utilizes to maintain the correct number of
replicas for a file would be rendered less efficient.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
BEST PRACTICE:  Hot replication should be turned off on resilient pools.   
                The required number of replicas for files of a resilient  
                storage unit should reflect the estimated average usage 
                as well as the available space.
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

### Resilience home

On the host where the resilience service is running, you will see several files
in the `resilience.home` directory (`/var/lib/dcache/resilience` by default):

-   `pnfs-operation-map`

-   `pnfs-backlogged-messages`

-   `pnfs-operation-statistics`

-   `pnfs-operation-statistics-task-{datetime}`

-   `excluded-pools`

Explanations of these files follow.

#### pnfs-operation-map

This is the snapshot of the main operation table. By default, this checkpoint is
written out (in simple text format) every minute. As mentioned above, this
serves as a heuristic for immediately reprocessing incomplete operations in case
of a service crash and restart. It is an approximation of current state, so not
all file operations in progress at that time may have actually been saved, nor
completed ones removed. As previously stated, the periodic pool scan should
eventually detect any missing replicas not captured at restart.

When a new snapshot is taken, the old is first renamed to
`pnfs-operation-map-old`. Similarly, at restart, before being reread, the file
is renamed to `pnfs-operation-map-reload` to distinguish it from the current
snapshot whose processing may overlap with the reloading.

#### pnfs-backlogged-messages

It is possible to enable and disable message handling inside Resilience. There
is also a short delay at startup which may also accumulate a backlog. Any
messages which happen to arrive during this period are written out to this file,
which is then read back in and deleted during initialization. The lifetime of
the file, in the latter case, is therefore rather brief, so you will usually not
see one in the directory unless you purposely disable message handling.

#### pnfs-operation-statistics and pnfs-operation-statistics-task-{datetime}

Through an admin command, one can enable and disable the recording of
statistical data. When activated, two kinds of files are produced. The first is
an overview of performance with respect to file location messages and
operations. The second contains detailed, task-by-task records and timings for
each terminated file operation, written out at 1-minute intervals; this logging
rolls over every hour until deactivated.

The overview file can be accessed directly in the admin door via `diag history`:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
CHECKPOINT                   |          NEWLOC        HZ      CHNG |          FILEOP        HZ      CHNG       FAILED |         CHCKPTD
Tue Jun 07 09:22:21 CDT 2016 |        65592310       512     1.94% |        65595972       516     2.88%           23 |              68
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

As can be seen, the time of the snapshot is followed by a breakdown based on
arriving (new) locations and triggered file operations; for each, the total
(since the start of the resilience service) is given, along with the rate
sampled over the last checkpoint interval, and the percentage change from the
previous interval. The number of failures and the entry count for the last
snapshot are also reported.

The hourly task files must be read locally. The format used is as follows:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
PNFSID | STARTED ENDED | PARENT SOURCE TARGET | VERIFY COPY REMOVE | STATUS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

*Parent* refers to a pool which is being scanned; new files have no parent. For
each entry, two timings are reported: *verify* refers to the part of the
operation which determines whether the file needs handling; then either a copy
or remove timing is given.

#### excluded-pools

When the command `pool exclude <pools>` is used, the pool operation record is
marked as `EXCLUDED`. This can be observed when the `pool ls` command is issued.
Pools so marked are recorded to this file after each pool monitor state update
(every 30 seconds). This file is read in, again if it exists, at startup, so
that previously excluded pools will continue to be treated as such.

For pool exclusion, see the [section
below](https://github.com/dCache/dcache/wiki/Resilience#exclude-a-pool-from-resilience-handling)
under typical scenarios.

### Admin Commands

There are a number of ways to interact with the resilience service through the
admin door. As usual, the best guide for each command is to consult its help
documentation (`\h <command>`).

These commands will not be discussed in detail here. We instead present a brief
overview and a few examples of output.

#### Available actions

1.  `enable`, `disable [strict]`: Enable and disable Resilience message
    handling; enable and disable all of Resilience without having to stop the
    domain.

2.  `inaccessible`: Print to a file the `pnfsids` for a pool which currently
    have no readable locations.

3.  `file check`: Run a job to adjust replicas for a given file (i.e., make
    required copies or remove unnecessary ones).

4.  `file ctrl`, `pool ctrl`: Reset properties controlling internal behavior
    (such as sweep intervals, grace periods, etc.).

5.  `file ls`: List the current file operations, filtered by attributes or
    state; or just output the count for the given filter.

6.  `file cancel`: Cancel file operations using a filter similar to the list
    filter.

7.  `pool info`: List pool information derived from the pool monitor.

8.  `pool group info`: List the storage units linked to a pool group and confirm
    resilience constraints can be met by the member pools.

9.  `pool ls`: List pool operations, filtered by attributes or state.

10. `pool cancel`: Cancel pool operations using a filter similar to the list
    filter.

11. `pool scan`: Initiate forced scans of one or more pools.

12. `pool include`, `pool exclude`: Set the status of a pool operation to
    exclude or include it in replica counting and resilience handling.

13. `diag`: Display diagnostic information, including number of messages
    received, number of operations completed or failed, and their rates; display
    detailed transfer information by pool and type (copy/remove), with the pool
    as source and target.

14. `diag history [enable]`: Enable or disable the collection of statistics;
    display the contents of the diagnostic history file.

15. `history [errors]`: List the most recent file operations which have
    completed and are now no longer active; do the same for the most recent
    terminally failed operations.

#### Example output

One of the most useful commands is `diag`, or diagnostics. This can be called
with or without a regular expression (indicating pool names):

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
[fndcatemp2] (Resilience@resilienceDomain) admin > diag .*
Running since: Fri Jun 03 14:47:40 CDT 2016
Uptime 3 days, 19 hours, 56 minutes, 36 seconds

Last pnfs sweep at Tue Jun 07 10:44:17 CDT 2016
Last pnfs sweep took 0 seconds
Last checkpoint at Tue Jun 07 10:43:24 CDT 2016
Last checkpoint took 0 seconds
Last checkpoint saved 86 records

MESSAGE                               received  msgs/sec
    CLEAR_CACHE_LOCATION                    19         0
    CORRUPT_FILE                             5         0
    ADD_CACHE_LOCATION                67898905       515
    POOL_STATUS_DOWN                         0         0
    POOL_STATUS_UP                         169         0

OPERATION                            completed   ops/sec       failed
    FILE                              67901908       517           23
    POOL_SCAN_DOWN                           0         0            0
    POOL_SCAN_ACTIVE                       228         0            0

TRANSFERS BY POOL                   from              to          failed         size             removed          failed         size
dmsdca15-1                        959166          962097               0    939.55 KB                 0               0           0 
dmsdca15-1.1                      972972          973463               0    950.65 KB                 0               0           0 
dmsdca15-2                        967710          970284               1    947.54 KB                 0               0           0 
dmsdca15-2.1                      968995          972802               0    950.00 KB                 0               0           0 
dmsdca15-3                        956909          963618               1    941.03 KB                 0               0           0 
dmsdca15-3.1                      970977          973876               0    951.05 KB                 0               0           0 
dmsdca15-4                        959851          959739               0    937.25 KB                 0               0           0 
dmsdca15-4.1                      926010          926611               0    904.89 KB                 0               0           0 
...
TOTALS                          67901908        67901908              22     64.76 MB                 0               0           0 
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Along with basic uptime information and operation tracking, the pool expression
displays transfer counts by pool and type (copy, remove).

Also very useful is the file operation list command. This can display the actual
records for the file operations (here given without any filtering):

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
[fndcatemp2] (Resilience@resilienceDomain) admin > file ls
Tue Jun 07 11:16:12 CDT 2016 (0000C0C4B60020C04B94A8570B0594ACD416 REPLICA)(COPY RUNNING)(parent none, count 1, retried 0)
Tue Jun 07 11:16:12 CDT 2016 (00000447C17631D34ACE98EEDD19A97522F4 REPLICA)(COPY RUNNING)(parent none, count 1, retried 0)
Tue Jun 07 11:16:12 CDT 2016 (00001EE0A9AA9449405890401D5CE5931EB7 REPLICA)(COPY RUNNING)(parent none, count 1, retried 0)
Tue Jun 07 11:16:12 CDT 2016 (00006F319230C7464FEE8BC9BBECFD11636E REPLICA)( RUNNING)(parent none, count 1, retried 0)
…
Tue Jun 07 11:16:12 CDT 2016 (0000C2F0B0C5240F407E964DA748518C106C REPLICA)( RUNNING)(parent none, count 1, retried 0)
Tue Jun 07 11:16:12 CDT 2016 (000092AF5289517D4C029CDFFBF6D92C769B REPLICA)(COPY RUNNING)(parent none, count 1, retried 0)
TOTAL OPERATIONS:       89
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can also use '`$`' as an argument to list the count:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
[fndcatemp2] (Resilience@resilienceDomain) admin > file ls $ -state=WAITING
0 matching pnfsids
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Appending a '`@`' to the dollar sign will display the counts broken down by
source pool:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
[fndcatemp2] (Resilience@resilienceDomain) admin > file ls $@ -state=RUNNING
152 matching pnfsids.

Operation counts per pool:
     dmsdca15-1                             5
     dmsdca21-6.1                           1
     dmsdca21-8.1                           5
     dmsdca17-1.1                           3
     dmsdca18-1.1                           3
     dmsdca22-8.1                           2
     dmsdca22-4.1                          19
     dmsdca22-2.1                          11
     …
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A similar command, `pool ls`, exists for checking the current status of any pool
scan operations:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
[fndcatemp2] (Resilience@resilienceDomain) admin > pool ls dmsdca17
 dmsdca17-2 (completed: 0 / 0 : ?%) – (updated: Mon Jun 06 14:47:33 CDT 2016)(scanned: Mon Jun 06 14:47:33 CDT 2016)(prev ENABLED)(curr ENABLED)(IDLE) 
 dmsdca17-1 (completed: 0 / 0 : ?%) – (updated: Mon Jun 06 15:01:55 CDT 2016)(scanned: Mon Jun 06 15:01:55 CDT 2016)(prev ENABLED)(curr ENABLED)(IDLE) 
 dmsdca17-1.1   (completed: 0 / 0 : ?%) – (updated: Mon Jun 06 15:20:32 CDT 2016)(scanned: Mon Jun 06 15:20:32 CDT 2016)(prev ENABLED)(curr ENABLED)(IDLE) 
 dmsdca17-2.1   (completed: 0 / 0 : ?%) – (updated: Mon Jun 06 17:53:23 CDT 2016)(scanned: Mon Jun 06 17:53:23 CDT 2016)(prev ENABLED)(curr ENABLED)(IDLE) 
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For each operation, the timestamps of the last update (change in status) and of
the last completed scan are indicated, as well as pool status (`ENABLED` here),
and operation state (`IDLE`, `WAITING`, `RUNNING`). For running scans, the
number of file operations completed out of a total (if known) is reported.

Finally, the two `ctrl` commands can be used to verify or reset basic
configuration values, or to interrupt operation processing or force a sweep to
run. Here is the info output for each:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
[fndcatemp2] (Resilience@resilienceDomain) admin > file ctrl
maximum concurrent operations 200.
maximum retries on failure 2
sweep interval 1 MINUTES
checkpoint interval 1 MINUTES
checkpoint file path /var/lib/dcache/resilience/pnfs-operation-map
Last pnfs sweep at Tue Jun 07 11:38:28 CDT 2016
Last pnfs sweep took 0 seconds
Last checkpoint at Tue Jun 07 11:38:26 CDT 2016
Last checkpoint took 0 seconds
Last checkpoint saved 87 records

[fndcatemp2] (Resilience@resilienceDomain) admin > pool ctrl
down grace period 1 HOURS
restart grace period 6 HOURS
maximum concurrent operations 5
scan window set to 24 HOURS
period set to 3 MINUTE
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

### Tuning

Only a few properties can be reset using the `ctrl` commands shown above. Please
consult the documentation in the `resilience.properties` defaults for a fuller
explanation of the tuning issues which pertain to resilience. If adjustments to
the preset values are made, remember to ensure that enough database connections
remain available to service both Chimera operations and Resilience operations,
and that these be properly matched to the number of threads responsible for the
various operations, in order to avoid contention (see again, the explanation in
the default properties file).

### Some typical scenarios part 1: what happens when ...?

#### Resilience is initialized (service start)

Should the resilience service go offline, nothing special occurs when it is
restarted. That is, it will simply go through the full re-initialization
procedures.

Initialization steps are reported at the logging `INFO` level, and can be
observed from the pinboard:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
[fndcatemp2] (Resilience@resilienceDomain) admin > show pinboard
11:50:50 AM [pool-9-thread-1] [] Waiting for pool monitor refresh notification.
11:50:50 AM [pool-9-thread-1] [] Received pool monitor; loading pool information.
11:50:50 AM [pool-9-thread-1] [] Loading pool operations.
11:50:50 AM [pool-9-thread-1] [] Pool maps reloaded; initializing ...
11:50:50 AM [pool-9-thread-1] [] Pool maps initialized; delivering backlog.
11:50:50 AM [Consumer] [] Backlogged messages consumer exiting.
11:50:50 AM [pool-9-thread-1] [] Messages are now activated; starting pnfs consumer.
11:50:50 AM [pool-9-thread-1] [] Pnfs consumer is running; activating admin commands.
11:50:50 AM [Reloader] [] Done reloading backlogged messages.
11:50:50 AM [pool-9-thread-1] [] Starting the periodic pool monitor refresh check.
11:50:50 AM [pool-9-thread-1] [] Updating initialized pools.
11:50:50 AM [pool-9-thread-1] [] Admin access enabled; reloading checkpoint file.
11:50:50 AM [pool-9-thread-1] [] Checkpoint file finished reloading.
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

PoolMonitor state is initially pulled from PoolManager, and thereafter refreshed
every 30 seconds by a push from PoolManager to its "subscribers" (of which
Resilience is one). Once the initial monitor state is received, pool information
is parsed and loaded into a local data structure (accessed via the `pool info`
command). The pool operations table is then built (accessed via the `pool ls`
command). The `excluded-pools` file is also reloaded at this point.

##### What exactly does `UNINITIALIZED` mean for a pool?

In addition to the usual pool status types (`ENABLED`, `READ_ONLY`, `DOWN`),
`UNINITIALIZED` serves to indicate incomplete information on that pool from the
PoolMonitor. The transition from `UNINITIALIZED` to another state occurs when
the resilience service comes on line, whether simultaneously with pool
initialization or not.

>   **The probability of pools going down at initialization is low, but it is
>   possible that Resilience could go down and then restart to find a number of
>   pools down. In this case, the** `DOWN` **pools will be handled as usual (see
>   below). On the other hand, it is preferable not to treat the transition to**
>   `READ_ONLY`**/**`ENABLED` **as a "restart" when coming out of**
>   `UNINITIALIZED`**, since the majority of pools will most of the time
>   initialize to a viable readable status, and handling this transition would
>   unnecessarily provoke an immediate system-wide scan.**

During this phase of initialization, message handling is still inactive; hence
any incoming messages are temporarily cached. When the pool info and operation
tables have been populated, the backlogged messages are handled. At the same
time, message handling is also activated. Up until this point, issuing any admin
commands requiring access to Resilience state will result in a message that the
system is initializing:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
[fndcatemp2] (Resilience@resilienceDomain) admin > diag
Resilience is not yet initialized; use 'show pinboard' to see progress, or 'enable' to re-initialize if previously disabled.
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

After this point, admin commands become viable and the pool information is
periodically refreshed (every 30 seconds, in response to the PoolMonitor
message). Changes in pool state or pool group and storage unit composition are
recorded and appropriate action, if necessary, is taken.

The final step in initialization is to reload any operations from the checkpoint
file.

>   **This can be a lengthy procedure if the file is large, but should pose no
>   particular issues, since the system treats the reloaded operations as just
>   another set of cache location updates and handles them accordingly.**

#### A pool goes down or is reported as dead

Resilience considers pools viable until they become unreadable. If a pool is
read-only, its files will still be counted as accessible replicas, and will be
used as potential sources for copying (naturally, the pool is excluded as target
for new copies). Once a pool becomes entirely disabled, Resilience will mark it
as `DOWN` and queue it for an eventual scan. The scan will be activated as soon
as the expiration of the grace period for that pool has been detected by the
pool "watchdog" (which wakes up every three minutes by default).

>   **When we speak of "scanning" a pool which is down or inaccessible, this is
>   shorthand for saying that Resilience runs a query against the namespace to
>   find all the** `pnfsids` **which have** `AccessLatency` **=** `ONLINE` **and
>   a** `location` **(copy) on that pool. No actual interaction with the pool
>   takes place.**

Once a scan is completed on a dead pool, no more scans will be done on it until
its state changes.

Under most circumstances, no intervention should be required. This is part of
the normal functioning of the service.

#### A pool is re-enabled or brought back on line

This is just the counterpart to the previous scenario. When a pool is brought
back on line, Resilience queues it for eventual scanning, once again activated
as soon as the expiration of the grace period is detected. Note that the grace
periods for pool down and pool restart are independent properties.

If a scan on the pool is currently running when the pool comes back up, it will
be immediately canceled and a new scan rescheduled.

Note that Resilience always does the same thing on a scan, whether triggered by
a change from `UP` to `DOWN` or vice versa: it checks the countable replicas of
a file, compares them to the required number, and takes the appropriate action
(either copy or remove).

#### Several pools go off line simultaneously

Each will be treated as above. By default, five scans are allowed to run
simultaneously. If there are more scans than five, they will continue in the
`WAITING` state until a slot becomes available.

Scans can take some time to complete, depending on how big the pool is. If there
is a `WAITING` scan which has not run yet when the periodic window for the scan
expires, it will simply remain queued until it finally runs.

##### Why don't I see a value for the 'total' on the admin `pool ls` output for a `RUNNING` pool operation?

When a pool is scanned, a query is run against Chimera to collect all the
`pnfsids` associated with `ONLINE` files on the given pool. The scanning
processes the results from the query via a cursor, so each file is checked and
queued for treatment if necessary, in order. This looping through the results
can very easily take longer than it does for some of the operations it has
created to be run and to complete. But the total number of files to process is
only known at the end of this loop. So it is possible to see a value for the
number of files processed so far before seeing the total. Obviously, only when
the total exists can the % value be computed.

### Some typical scenarios part 2: how do I ...?

During the normal operation of a resilient installation of dCache, changes to
the number of pools, along with the creation and removal of storage classes or
pool groups, will undoubtedly be necessary. The following describes the steps to
take and what response to expect from the resilience service in each case.

#### Add a pool to a resilient group

Let us assume that we have some new disk space available on node `dmsdca24`, and
that we want to use it to host some pools. Of course, the first steps are to
prepare the appropriate partitions for the pools and to create the pool area
(directory) with the necessary setup file for dCache (see "Creating and
configuring pools" in the [Configuring
dCache](https://www.dcache.org/manuals/Book-2.16/Book-fhs.shtml#in-install-configure)
section of the dCache Book). Once we have done that, and have also added the
pool stanza to the layout file
[(ibid)](https://www.dcache.org/manuals/Book-2.16/Book-fhs.shtml#in-install-configure),
we can proceed to add the pool to the `psu` (PoolSelectionUnit):

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
[fndcatemp2] (PoolManager@dCacheDomain) admin > \c PoolManager
psu create pool rw-dmsdca24-1 -disabled
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We can then start the pool on the host. It should then appear enabled:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
[fndcatemp2] (PoolManager@dCacheDomain) admin > psu ls -a  pool rw-dmsdca24-1 
rw-dmsdca24-1  (enabled=false;active=15;rdOnly=false;links=0;pgroups=0;hsm=[];mode=enabled)
  linkList   :
  pGroupList :
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Let us now say that we want to make this a resilient pool by adding it to a
resilient pool group.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
[fndcatemp2] (PoolManager@dCacheDomain) admin > psu addto pgroup res-group rw-dmsdca24-1

[fndcatemp2] (Resilience@resilienceDomain) admin > \sp psu ls -a pool rw-dmsdca24-1
rw-dmsdca24-1  (enabled=false;active=19;rdOnly=false;links=0;pgroups=1;hsm=[];mode=enabled)
  linkList   :
  pGroupList : 
    res-group(links=1; pools=35; resilient=true)

[fndcatemp2] (PoolManager@dCacheDomain) admin > psu ls -a pgroup res-group
res-group
 resilient = true
 linkList :
   res-link  (pref=10/10/-1/10;;ugroups=3;pools=1)
 poolList :
   dmsdca15-1  (enabled=true;active=14;rdOnly=false;links=0;pgroups=1;hsm=[enstore];mode=enabled)
   dmsdca15-1.1  (enabled=true;active=10;rdOnly=false;links=0;pgroups=1;hsm=[enstore];mode=enabled)
   ...
   dmsdca22-8.1  (enabled=true;active=7;rdOnly=false;links=0;pgroups=1;hsm=[enstore];mode=enabled)
   rw-dmsdca24-1  (enabled=false;active=5;rdOnly=false;links=0;pgroups=1;hsm=[];mode=enabled)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

After the next monitor refresh, Resilience should show this pool as well:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
[fndcatemp2] (Resilience@resilienceDomain) admin > pool info rw-dmsdca24-1
key           84
name          rw-dmsdca24-1
tags          {hostname=dmsdca24.fnal.gov, rack=24-1}
mode          enabled
status        ENABLED
last update   Thu Jun 16 09:24:21 CDT 2016

[fndcatemp2] (Resilience@resilienceDomain) admin > pool ls rw-dmsdca24-1
rw-dmsdca24-1   (completed: 0 / 0 : ?%) – (updated: Thu Jun 16 09:25:42 CDT 2016)(scanned: Thu Jun 16 09:25:42 CDT 2016)(prev ENABLED)(curr ENABLED)(IDLE) 
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When the pool is added, a scan is scheduled for it, provided it is in an
initialized state. In this case, since the pool is empty, the scan completes
quickly.

#### Remove a pool from a resilient group

When a pool is removed from a resilient group, the pool needs to be scanned,
because the `ONLINE` files it contains constitute replicas, and as such, the
respective counts for each will be diminished by 1.

Here we walk through several steps to show Resilience put through its paces.

First, let us remove a pool from a resilient pool group.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
[fndcatemp2] (PoolManager@dCacheDomain) admin > psu removefrom pgroup res-group rw-dmsdca24-2
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We observe the pool has been queued for a scan:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
[fndcatemp2] (Resilience@resilienceDomain) admin > pool ls rw-
rw-dmsdca24-2   (completed: 0 / 0 : ?%) – (updated: Thu Jun 16 17:17:09 CDT 2016)(scanned: Thu Jun 16 17:14:33 CDT 2016)(prev ENABLED)(curr ENABLED)(WAITING) 
rw-dmsdca24-3   (completed: 0 / 0 : ?%) – (updated: Thu Jun 16 17:14:33 CDT 2016)(scanned: Thu Jun 16 17:14:33 CDT 2016)(prev UNINITIALIZED)(curr ENABLED)(IDLE) 
rw-dmsdca24-4   (completed: 0 / 0 : ?%) – (updated: Thu Jun 16 17:14:33 CDT 2016)(scanned: Thu Jun 16 17:14:33 CDT 2016)(prev UNINITIALIZED)(curr ENABLED)(IDLE)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Because we are impatient and don't want to wait 3 minutes, let's wake up the
sleeping watchdog:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
[fndcatemp2] (Resilience@resilienceDomain) admin > pool ctrl run
Forced watchdog scan.

[fndcatemp2] (Resilience@resilienceDomain) admin > pool ls rw-dmsdca24-2
rw-dmsdca24-2   (completed: 0 / 0 : ?%) – (updated: Thu Jun 16 17:17:19 CDT 2016)(scanned: Thu Jun 16 17:14:33 CDT 2016)(prev ENABLED)(curr ENABLED)(RUNNING) 
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Shortly thereafter, we can see it is doing some work:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
[fndcatemp2] (Resilience@resilienceDomain) admin > pool ls rw-dmsdca24-2
rw-dmsdca24-2   (completed: 491 / ? : ?%) – (updated: Thu Jun 16 17:17:19 CDT 2016)(scanned: Thu Jun 16 17:14:33 CDT 2016)(prev ENABLED)(curr ENABLED)(RUNNING) 
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Because this is a demo pool, there aren't many files, so the scan completes
nearly simultaneously with the actual copying; hence there is not enough of a
time lag to see a "% complete" reported.

Upon termination, this pool is no longer resilient; as such, there should no
longer be a record for it in the pool operation table; and indeed, only the
other two "rw-" pools still appear:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
[fndcatemp2] (Resilience@resilienceDomain) admin > pool ls rw-
rw-dmsdca24-3   (completed: 0 / 0 : ?%) – (updated: Thu Jun 16 17:14:33 CDT 2016)(scanned: Thu Jun 16 17:14:33 CDT 2016)(prev UNINITIALIZED)(curr ENABLED)(IDLE) 
rw-dmsdca24-4   (completed: 0 / 0 : ?%) – (updated: Thu Jun 16 17:14:33 CDT 2016)(scanned: Thu Jun 16 17:14:33 CDT 2016)(prev UNINITIALIZED)(curr ENABLED)(IDLE) 
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Now, just to spot check that everything is right with the world, let's examine
the recent operations, pick a `pnfsid`, and find its locations (replicas).

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 [fndcatemp2] (Resilience@resilienceDomain) admin > history
 … [many other files]
 Thu Jun 16 17:17:23 CDT 2016 (00008F22B12729CE458596DE47E00411D68C REPLICA)(COPY DONE)(parent 20, retried 0) 
 … [many other files]

 [fndcatemp2] (Resilience@resilienceDomain) admin > \sn cacheinfoof 00008F22B12729CE458596DE47E00411D68C
 dmsdca15-1 dmsdca15-1.1 dmsdca16-2.1 rw-dmsdca24-2

 [fndcatemp2] (Resilience@resilienceDomain) admin > \sn storageinfoof 00008F22B12729CE458596DE47E00411D68C
 size=1;new=false;stored=false;sClass=resilient-4.dcache-devel-test;cClass=-;hsm=enstore;accessLatency=ONLINE;retentionPolicy=REPLICA;path=<Unknown>;group=resilient-4;family=dcache-devel-test;bfid=<Unknown>;volume=<unknown>;location=<unknown>;
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The storage class/unit for this file (`resilient-4.dcache-devel-test@enstore`)
happens to require three copies; there are indeed three, plus the no-longer
valid location which we just removed from resilience handling.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
NOTE:     removing a pool from a resilient pool group will not automatically 
          delete the files it contains from the pool, nor will it remove
          the system-owned sticky bits for these files (such behavior
          would be unsafe).

WARNING:  if a pool is moved from one resilient pool group to another, the 
          replica counts for the added files will be seen in the context
          of the new group, leading to the creation of the required number of              
          copies on the pools of the new group (without affecting the previous copies).
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The removal of a pool from the `psu` using `psu remove pool <name>` will also
remove the pool from the pool info listing in Resilience; however, if the pool
keeps connecting (i.e., it has not been stopped), its entry will continue to
appear in the PoolMonitor, and thus also in the internal resilience table.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
WARNING:  do NOT remove the pool from the psu using 'psu remove pool <name>' 
          until the scan of the pool has completed. Otherwise, its reference 
          will no longer be visible to Resilience, and the scan will partially 
          or completely fail.
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

One could also disable the pool, let Resilience handle it as a `DOWN` pool, and
then when the scan completes, remove it from the pool group. The removal in this
case should not trigger an additional scan, since the pool is `DOWN` and has
already been processed. Thus this sequence would also work:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
[fndcatemp2] (Resilience@resilienceDomain) admin > \s rw-dmsdca24-2 pool disable -strict
...
(after the DOWN scan has completed)
...
[fndcatemp2] (PoolManager@dCacheDomain) admin > psu removefrom pgroup res-group rw-dmsdca24-2
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

>   **As long as at least one replica of a file continues to reside in a
>   resilient pool group, action will be taken by Resilience (if possible) to
>   maintain the proper number of replicas for that file inside the pool group
>   (a single but currently unreadable copy will raise an alarm, however). Once
>   all pools containing replicas of a given file have been removed from the
>   pool group, however, Resilience becomes agnostic concerning the number of
>   copies of that file (the file becomes "invisible" to Resilience).**

#### Add or remove a resilient group

There is nothing special about adding or removing a resilient group. Doing so
will register the group inside Resilience as well:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
[fndcatemp2] (Resilience@resilienceDomain) admin > \sp psu create pgroup r-test-group -resilient

[fndcatemp2] (Resilience@resilienceDomain) admin > pool group info r-test-group
Name         : r-test-group
Key          : 12
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

even though at this point it is empty. Once pools are added to this group, the
behavior will be as indicated above. To remove the group, remove all the pools
first, let their scans complete, and then remove the group. (See below for how
to override the grace periods for pool status changes in order to force the scan
to run.)

>   **As mentioned above (see the warning under "Defining a resilient group"),
>   it is not possible to use the admin shell to demote a resilient pool group
>   to non-resilient status.**

#### Exclude a pool from resilience handling

During normal operation, the resilience service should be expected to handle
gracefully situations where a pool with many files, for one reason or another,
goes offline. Such an incident, even if the "grace period" value were set to 0,
in initiating a large scan, should not bring the system down. Obversely, should
the pool come back on line, any such scan should be (and is) immediately
canceled, and the pool rescheduled for a scan to remove unnecessary copies. So
under most circumstances, no manual intervention or adjustment in this regard
should be required.

Nevertheless, temporary exclusion of one or more pools from being handled by the
resilience service may be desirable in a few situations.

The old Replica Manager provided for marking pools `OFFLINE` (a state distinct
from `DOWN` or `disabled`). Such pools were exempted from pool state change
handling, but their files continued to "count" as valid replicas.

This feature has been held over in the new resilience service.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
\s Resilience pool exclude <regular expression>
\s Resilience pool include <regular expression>
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

##### When **NOT** to use `pool exclude`

The `pool exclude` command interferes with the normal operation of Resilience;
use in the wrong circumstances may easily lead to inconsistent state.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
WARNING:  Only use 'pool exclude' for temporary situations where the intention
          is eventually to restore the excluded location(s) to resilience management; 
          or when the locations on those pools are actually being migrated or 
          deleted from the namespace.
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If, for instance, one set a pool to `EXCLUDED`, then removed the pool from a
resilient group, the pool would disappear from the pool operation list (`pool
ls`), but its replicas would still be counted by Resilience. One would then have
to repair the situation manually, either by adding the pool back into the
resilient group and then removing it correctly, or by manually deleting or
migrating the files.

`pool exclude` is useful for doing manual migrations on resilient pool groups,
but caution should be taken when applying it.

#### Rebalance or migrate a resilient pool (group)

Rebalancing should be required less often on resilient pools; but if you should
decide to rebalance a resilient pool group, or need to migrate files from one
pool group to another, be sure to disable resilience on all those pools. One
could do this by stopping resilience altogether,

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
\s Resilience disable strict
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

but this of course would stop the processing of other resilient groups not
involved in the operation. The alternative is to use the `exclude` command one
or more times with expressions matching the pools you are interested in:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
\s Resilience pool exclude <exp1>
\s Resilience pool exclude <exp2>
...
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Note that the exclusion of a pool inside Resilience will survive a restart of
the service because excluded pools are written out to a file (`excluded-pools`;
see above) which is read back in on initialization.

When rebalancing or migration is completed, pools can be set back to active
resilience control:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
\s Resilience pool include .*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

(Note that only `EXCLUDED` pools are affected by the `include` command.)
Re-inclusion sets the pools back to `IDLE`, and does not schedule them
automatically for a scan, so if you wish to scan these pools before the periodic
window elapses, a manual scan is required.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
BEST PRACTICE:  Disable resilience on the potential source and target pools 
                by setting them to EXCLUDED before doing a rebalance 
                or migration.  
                
                An alternative to this would be to remove all the pools 
                from the pool group(s) in question, and then add them back 
                afterwards.
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#### Manually schedule or cancel a pool scan

A scan can be manually scheduled for any resilient pool, including those in the
`DOWN` or `EXCLUDED` states.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
[fndcatemp2] (Resilience@resilienceDomain) admin > pool scan dmsdca18-2
Scans have been issued for:
dmsdca18-2
dmsdca18-2.1
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Note that if a pool is in the `WAITING` state as a result of a pool status
change from `UP` to `DOWN` or vice versa, calling `pool scan` on it will
override the grace period wait so that it will begin to run the next time the
watchdog wakes up.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
NOTE:  One can override the grace period for a waiting scan by calling 'pool scan'.
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

One could of course also change the global setting for the grace periods using
`pool ctrl`:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
[fndcatemp2] (Resilience@resilienceDomain) admin > pool ctrl
down grace period 6 HOURS
restart grace period 6 HOURS
maximum concurrent operations 5
scan window set to 24 HOURS
period set to 3 MINUTES

[fndcatemp2] (Resilience@resilienceDomain) admin > pool ctrl reset -down=1 -unit=MINUTES
down grace period 1 MINUTES
restart grace period 6 HOURS
maximum concurrent operations 5
scan window set to 24 HOURS
period set to 3 MINUTES

[fndcatemp2] (Resilience@resilienceDomain) admin > pool ctrl reset -restart=1 -unit=MINUTES
down grace period 1 MINUTES
restart grace period 1 MINUTES
maximum concurrent operations 5
scan window set to 24 HOURS
period set to 3 MINUTES
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Any scan operation, however initiated, can by cancelled manually by issuing the
`pool cancel` command. This has a number of options, but perhaps the most
important is `-includeChildren`; this indicates that, aside from resetting to
idle the pool operation in question (and cancelling the underlying database
query if it is still running), all of the incomplete child (i.e., file)
operations scheduled thus far will also be immediately cancelled and removed
from the file operation table. File operations can also be cancelled
individually (see below).

#### Add or remove a resilient storage unit

Adding a resilient storage unit from scratch would involve creating the unit and
adding it to a unit group.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
\sp psu create unit -store resilient-5.dcache-devel-test@enstore
\sp psu set storage unit -required=4 resilient-5.dcache-devel-test@enstore
\sp psu addto ugroup resilient resilient-5.dcache-devel-test@enstore
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

None of these actions triggers an immediate response from Resilience, though the
unit does show up as registered:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
[fndcatemp2] (Resilience@resilienceDomain) admin > pool group info -showUnits res-group
Name         : res-group
Key          : 9
    resilient-0.dcache-devel-test@enstore
    resilient-4.dcache-devel-test@enstore
    resilient-1.dcache-devel-test@enstore
    resilient-5.dcache-devel-test@enstore
    resilient-3.dcache-devel-test@enstore
    resilient-2.dcache-devel-test@enstore

[fndcatemp2] (Resilience@resilienceDomain) admin > pool group info -showUnits rss-group
Name         : rss-group
Key          : 11
    resilient-0.dcache-devel-test@enstore
    resilient-4.dcache-devel-test@enstore
    resilient-1.dcache-devel-test@enstore
    resilient-5.dcache-devel-test@enstore
    resilient-3.dcache-devel-test@enstore
    resilient-2.dcache-devel-test@enstore
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If this is a new group, the appropriate tag must be set on any target
directories for the files that belong to it.

Removing a unit also will not trigger an immediate response from Resilience,
though once again the unit will be unregistered internally.

#### Modify a resilient storage unit

There are two possible modifications to a resilient storage unit. One would be
to change the required number of replicas, and the other, the tag constraints.
In the first case, copy or remove operations will be triggered according to
whether there are now not enough or too many replicas. In the latter, both
removes and copies may occur in order to redistribute the existing files to
satisfy the new partitioning by pool tag. In both cases, all the pools in all
the pool groups to which the storage unit is linked will be scheduled for scans.

\#\#\#Troubleshooting file operations

Intervention to rectify resilience handling should hopefully be needed
infrequently; yet it is not impossible for copy or remove jobs not to run to
completion (for instance, due to lost network connections from which Resilience
could not recover).

There are several palliatives available short of restarting the Resilience
domain in these cases.

One can try to identify which operations are stuck in the RUNNING state for a
long period:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
file ls -lastUpdateBefore=2016/06/17-12:00:00 -state=RUNNING
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If no progress is being made, one can cancel these operations (and allow them to
be retried later):

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
file cancel -lastUpdateBefore=2016/06/17-12:00:00 -state=RUNNING
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

>   **Note that with file cancellation, if the operation count is more than 1
>   (meaning it is scheduled to make or remove more than once), the operation
>   will attempt the next pass. To force immediate cancellation of the entire
>   operation for such cases, add** `-forceRemoval` **to the command.**

Should none of the file operations be making progress, or if there are `WAITING`
operations but nothing on the `RUNNING` queue, this is symptomatic of a bug, and
should be reported to the dCache team. One could try to restart the consumer in
this case:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
file ctrl shutdown
file ctrl start
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

but the bug may present itself again.

The above `ctrl` command is nevertheless useful for attempting recovery without
stopping all of Resilience; one can similarly start and stop the pool operation
consumer using `pool ctrl`. The resilience service as a whole can also be
stopped and restarted (should you be running other services in the same domain)
using `disable -strict` and `enable`.

Another potentially useful diagnostic procedure is to pause the handling of new
or deleted file locations:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
disable
...
enable
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Without the `-strict` flag, `disable` stops the message handling, passing off
the incoming messages to a file cache; when re-enabled, the messages are read
back in. This may help to determine if, for instance, pool scanning is making
any progress by itself.

### Migrating from the old Replica Manager to the new service

The best way to illustrate what needs to be done is to start with a simple
(single replica manager) setup. The diagram below shows the differences between
the original hybrid (tape + replicas) system and the way it should look under
the Resilience service.

![Migration Example](https://github.com/alrossi/dcache-miscellany/blob/master/docs/rm_migration.png)

There are three steps to the migration process:

-   Change `AccessLatency` and `RetentionPolicy` on current files and
    directories belonging to the resilient (disk-only) pools.

-   Change the metadata for existing files on the resilient pools.

-   Configure the `poolmanager.conf` with the necessary resilience attributes.

If there are multiple replica managers involved, then there is an additional
step to follow if these each had different minimum/maximum settings for the
number of required copies:

-   Either adapt existing storage units or create new ones to capture the files
    which would be in each of the resilient pool groups and set the resilience
    attributes accordingly on them. This will also mean that appropriate storage
    class tags need to be added to the directories linked to these groups if
    they do not already exist.

>   This procedure can usually be accomplished on a live dCache system (see
>   below for the one case which will require a pool restart), but the **new
>   resilience service as well as the old replica manager MUST NOT be running**.

#### Changing `AccessLatency` and `RetentionPolicy`

dCache provides a python script
(`/var/lib/dcache/migration/migrate_from_repman_to_resilience.py`) which will
make all the necessary changes to the namespace directly. These include marking
all the appropriate files as `REPLICA ONLINE` and changing the corresponding
tags on the appropriate directories so that future files have the correct
storage attributes.

The script is run (by default) on the host where the `chimera` database is
located; it takes as input a file containing a new-line separated list of the
pools in the old resilient pool group. Given that under the old replica manager
it was not possible to write both replicas and files needing to go to tape on
the same pool, the script simply changes the attributes for all files it finds
at each location. The script also records the directories in which those
replicas are found, and searches for the ancestors with the controlling
`AccessLatency` and `RetentionPolicy` tags, changing them there (so that they
propagate downward to the sub-directories which are supposed to inherit them).
Finally, the script outputs the list of parent directories for these files;
*this is for convenience, in case storage unit tags need to be added (fourth
step above for multiple replica manager systems)*.

#### Changes on the resilient pools

There are two separate issues to be addressed on the pools. The first involves
the `sticky` bits for permanent replicas and is the most important. The second
concerns `precious` files, and only becomes important if an original pool is to
be re-purposed for sharing between resilient and non-resilient files.

##### (A) Ensure that all files have the sticky bit set.

Under the old replica manager a pool could not hold original copies of files
that were supposed to go to tape; this was because original replicated files
were written as `precious`, and the pool itself was configured not to try to
write these files to tape and subsequently change their state to `cached` when
they were flushed. This means that replicas on these pools were either
`precious` (the original) or `cached+sticky` (the copies). If files which were
`cached` but lacked the system `sticky` bit existed on these pools, it was
because they got there by some other means (hot replication, for instance). The
old replica manager ignored these `cached` copies in its count of existing valid
permanent copies.

With the new resilience service, things are a bit different. The state of the
file in the pool repository is not checked; a policy is enforced up front that
any file that is `cached` without the `sticky` bit is only allowed onto the pool
if it is a temporary copy of a non-resilient (i.e., `CUSTODIAL NEARLINE`) file.
While `cached` copies of resilient replicas should not exist on such pools, they
may on the old pools.

If such copies are allowed to exist after migration, this could lead to data
loss, because the new resilience service will count them as legitimate replicas,
and may remove a copy which is `sticky`, leaving what it considered to be a
permanent replica in a state susceptible to being swept and removed.

It is thus necessary to set the sticky bit on all the files on the pool before
engaging the new resilience service. The replicas which are in excess will
eventually be detected and removed by the periodic pool checks.

Hence, for each pool in the resilient pool group, use the admin command:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
rep set sticky -all
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

##### (B) Dealing with the precious files.

If you continue to use the original pool for resilient storage only, there is
nothing further you need to do. The new resilience service is indifferent to
whether the file is classified as `precious` or `cached` on the pool. What
counts for it is that the file's `AccessLatency` is `ONLINE`. We already took
care of this in step one above.

>   In this case, you should **NOT** remove `lfs=precious` (as instructed for
>   the second case below). Otherwise, your logs will probably fill up with
>   error messages about not being able to flush the `precious` files to tape,
>   since there is presumably no HSM handler script running on this pool.

If, on the other hand, you intend to use this pool to store both resilient and
non-resilient files, then there are two things which need to be done. First, a
migration job should be run which changes the `precious` state to
`cached+sticky`, so that we can allow truly `precious` files written to the pool
to be distinguished from the old resilient files. This is a copy operation which
is run as follows using the admin command on the pool:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
migration copy -state=precious -smode=cached+system -tmode=cached+system -target=pgroup <pgroupname>
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Don't let the 'copy' here deceive: the operation works such that it will change
metadata on the source and only make a copy if it can't find one, which should
be a rare occurrence.

When the migration job has completed, you will also need to remove the setting
which stops `precious` files from being flushed to tape.

In the layout file, find the pool stanza and remove the line with the "lfs"
assignment:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
[resilient1Domain]
...
[resilient1Domain/pool]
pool.name=resilient1
pool.path=/diskd/res-pool-1
pool.wait-for-files=${pool.path}/setup
pool.lfs=precious                      <<<<<<<<<<<<<<<<<<<<<<< REMOVE THIS LINE
pool.tags=hostname=server1.example.org
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Finally, you will need to restart the pool(s) for this last change to take
effect.

After you are done applying one or both of these changes, you might wish to
verify that the changes have taken place using the admin shell:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
\s resilient1 rep ls
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Entries should all look similar to this:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
0000D0C444CD3E7B4F2381380CED9B7F7157 <C-------X--L(0)[0]> 1181 si={...}
0000896FC0AF172C4F0B8BCC52305BC1BA2C <C-------X--L(0)[0]> 1181 si={...}
OR
0000896FC0AF172C4F0B8BCC52305BC1BA2C <P-------X--L(0)[0]> 1181 si={...}
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you did not apply the second change, you will still see `precious` files on
the pool, but they will have the sticky bit set as well (as a consequence of the
bulk operation on the repository).

#### Changes to `poolmanager.conf` and (possibly) to directory tags

There are several different scenarios here, depending on the target setup.

##### (a) Entirely disk-only with only one resilience requirement

This monolithic case is the easiest to handle.

-   In `poolmanager.conf`, give the default storage unit the desired resilience
    setting.

>   Note: due to an oversight in the original version of resilience, support for
>   defining resilience on the globbed global and class units was not included.
>   This situation has been rectified in 2.16.17 and is indeed available in
>   3.0.0, so be sure to upgrade to those versions in order to use this feature.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
psu set storage unit *@* -required=<number of copies>
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

-   Give all pool groups the -resilient flag:

>   This would include the default group.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
psu create pgroup <name1> -resilient
psu create pgroup <name2> -resilient
...
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When the changes are complete, you will need to do (`\sp` = the `PoolManager`):

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
\sp reload -yes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

-   Finally, (optionally) set the default `AccessLatency` and `RetentionPolicy`
    properties (this would require a restart, though):

>   Either in the `dcache.conf` or layout file, add:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
pnfsmanager.default-access-latency   = ONLINE
pnfsmanager.default-retention-policy = REPLICA
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

##### (b) The disk-only part has only one resilience requirement, but the system also has files which are written to tape.

In this case, depending on what the relative proportions of tape-backed to
disk-only storage are, you may wish to leave the default properties above at
`NEARLINE CUSTODIAL`, or change them to `ONLINE REPLICA`.

In any case, one will need to distinguish minimally between two pool groups, and
there will be the need for at least one storage unit to be defined. Again,
depending on what makes sense, you could make the default storage unit resilient
or you could create a special resilient storage unit. In the latter case, the
tags corresponding to the storage information extraction for the underlying HSM
will need to be set on the appropriate directories.

As an example, the following makes the default non-resilient, and defines a
resilient group and storage unit, using the standard tags needed by the
`org.dcache.chimera.namespace.ChimeraOsmStorageInfoExtractor`:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
psu create pgroup resilient -resilient
psu create unit -store resilient:production@osm
psu set storage unit resilient:production@osm -required=2
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You will then need to link up in the usual manner this new unit to the new pool
group, and then in the directories where files of this type will be written,
define the tags:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
echo "production" > ".(tag)(sGroup)"
echo "resilient" > ".(tag)(OSMTemplate)"
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If, on the other hand, you wish the default to remain the resilient group, you
would do something like:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
psu set storage unit *@* -required=2
psu create pgroup tape
psu create unit -store tape:production@osm
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

again, linking the new tape group to the new tape storage class, and then on the
directories used to store files on tape, doing:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
echo "production" > ".(tag)(sGroup)"
echo "tape" > ".(tag)(OSMTemplate)"
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

##### (c) Multiple classes of resilience and/or tape storage

The procedures are identical to those above, except that additional storage
class definitions and tags will be required. More than two pool groups may or
may not be necessary, depending on whether there are other pool or network
characteristics on which the pool groupings are based.

After you have completed the entire set of changes, the resilience service may
be started.
