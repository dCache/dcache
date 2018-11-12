Pool Commands
=============

rep ls
------

rep ls - List the files currently in the repository of the pool.

synopsis
--------
rep ls[pnfsId...]|[-l=s|p|l|u|nc|e...][-s=k|m|g|t]

pnfsId  

The PNFS ID(s) for which the files in the repository will be listed.

**-l**  
List only the files with one of the following properties:

    s      sticky files
    p      precious files
    l      locked files
    u      files in use
    nc     files which are not cached
    e      files with an error condition

**-s**  
Unit, the filesize is shown:

    k      data amount in KBytes
    m      data amount in MBytes
    g      data amount in GBytes
    t      data amount in TBytes

Description
-----------

st set max active
------------------
st set max active - Set the maximum number of active store transfers.

synopsis
---------
st set max active <maxActiveStoreTransfers>
maxActiveStoreTransfers  
The maximum number of active store transfers.

Description
-----------

Any further requests will be queued. This value will also be used by the cost module for calculating the performance cost.

rh set max active
-----------------
rh set max active - Set the maximum number of active restore transfers.

synopsis
--------
rh set max active<maxActiveRetoreTransfers>
maxActiveRetoreTransfers  
The maximum number of active restore transfers.

Description
-----------

Any further requests will be queued. This value will also be used by the cost module for calculating the performance cost.

mover set max active
--------------------
mover set max active- Set the maximum number of active client transfers.

synopsis
--------
mover set max active<maxActiveClientTransfers> [-queue=<moverQueueName>]

maxActiveClientTransfers  
The maximum number of active client transfers.

moverQueueName  
The mover queue for which the maximum number of active transfers should be set. If this is not specified, the default queue is assumed, in order to be compatible with previous versions which did not support multiple mover queues (before version 1.6.6).

Description
-----------

Any further requests will be queued. This value will also be used by the cost module for calculating the performance cost.

mover set max active -queue=p2p
--------------------------------
mover set max active -queue=p2p - Set the maximum number of active pool-to-pool server transfers.

synopsis
--------
mover set max active -queue=p2p<maxActiveP2PTransfers>
maxActiveP2PTransfers  
The maximum number of active pool-to-pool server transfers.

Description
-----------

Any further requests will be queued. This value will also be used by the cost module for calculating the performance cost.

pp set max active
------------------
pp set max active - Set the value used for scaling the performance cost of pool-to-pool client transfers analogous to the other

synopsis
--------
pp set max active<maxActivePPTransfers>
maxActivePPTransfers  
The new scaling value for the cost calculation.

Description
-----------
All pool-to-pool client requests will be performed immediately in order to avoid deadlocks. This value will only used by the cost module for calculating the performance cost.

set gap
--------
set gap-Set the gap parameter - the size of free space below which it will be assumed that the pool is full within the cost calculations.

synopsis
--------
set gap<gapPara>
gapPara  
The size of free space below which it will be assumed that the pool is full. Default is 4GB.

Description
-----------

The gap parameter is used within the space cost calculation scheme described in [the section called “The Space Cost”](rf-glossary.md#space-cost). It specifies the size of free space below which it will be assumed that the pool is full and consequently the least recently used file has to be removed if a new file has to be stored on the pool. If, on the other hand, the free space is greater than gapPara, it will be expensive to store a file on the pool which exceeds the free space.

set breakeven
-------------

set breakeven - Set the breakeven parameter - used within the cost calculations.

synopsis
---------
set breakeven<breakevenPara>

breakevenPara  
The breakeven parameter has to be a positive number smaller than 1.0. It specifies the impact of the age of the [least recently used file](rf-glossary.md#least-recently-used-lru-file) on space cost. It the LRU file is one week old, the space cost will be equal to `(1 +breakeven)`. Note that this will not be true, if the breakeven parameter has been set to a value greater or equal to 1.

Description
-----------
The breakeven parameter is used within the space cost calculation scheme described in [the section called “The Space Cost”](rf-glossary.md#space-cost).

mover ls
--------

mover ls-List the active and waiting client transfer requests.

synopsis
--------
mover ls[-queue|-queue=<queueName>]

queueName  
The name of the mover queue for which the transfers should be listed.

Description
-----------

Without parameter all transfers are listed. With `-queue` all requests sorted according to the mover queue are listed. If a queue is explicitly specified, only transfers in that mover queue are listed.

migration cache
----------------

migration cache - Caches replicas on other pools.

synopsis
---------
migration cache [<options>] <target>... 

DESCRIPTION
===========

Caches replicas on other pools. Similar to `migration copy`, but with different defaults. See `migration copy` for a description of all options. Equivalent to: `migration copy` -smode=same -tmode=cached

migration cancel
----------------
migration cancel - Cancels a migration job

synopsis
----------
migration cancel [-force] job 

DESCRIPTION
===========

Cancels the given migration job. By default ongoing transfers are allowed to finish gracefully.

migration clear
----------------

migration clear — Removes completed migration jobs.

synopsis
---------

migration clear 

DESCRIPTION
===========

Removes completed migration jobs. For reference, information about migration jobs are kept until explicitly cleared.

migration concurrency
-----------------------

migration concurrency - Adjusts the concurrency of a job.

migration concurrency <job> <n> 

DESCRIPTION
===========

Sets the concurrency of <job> to <n>. 

migration copy
--------------

migration copy-Copies files to other pools.

synopsis
--------
migration copy [<options>] <target>... 


DESCRIPTION
===========

Copies files to other pools. Unless filter options are specified, all files on the source pool are copied.

The operation is idempotent, that is, it can safely be repeated without creating extra copies of the files. If the replica exists on any of the target pools, then it is not copied again. If the target pool with the existing replica fails to respond, then the operation is retried indefinitely, unless the job is marked as eager.

Please notice that a job is only idempotent as long as the set of target pools does not change. If pools go offline or are excluded as a result of an exclude or include expression then the job may stop being idempotent.

Both the state of the local replica and that of the target replica can be specified. If the target replica already exists, the state is updated to be at least as strong as the specified target state, that is, the lifetime of sticky bits is extended, but never reduced, and cached can be changed to precious, but never the opposite.

Transfers are subject to the checksum computiton policy of the target pool. Thus checksums are verified if and only if the target pool is configured to do so. For existing replicas, the checksum is only verified if the verify option was specified on the migration job.

Jobs can be marked permanent. Permanent jobs never terminate and are stored in the pool setup file with the `save` command. Permanent jobs watch the repository for state changes and copy any replicas that match the selection criteria, even replicas added after the job was created. Notice that any state change will cause a replica to be reconsidered and enqueued if it matches the selection criteria MDASH also replicas that have been copied before.

Several options allow an expression to be specified. The following operators are recognized: `<`, `<=`, `==`, `!=`, `>=`, `>`, `lt`, `le`, `eq`, `ne`, `ge`, `gt`, `~=`, `!~`, `+`, `-`, `*`, `/`, `%`, `div`, `mod`, `|`, `&`, `^`, `~`, `&&`, `||`, `!`, `and`, `or`, `not`, `?:`, `=`. Literals may be integer literals, floating point literals, single or double quoted string literals, and boolean true and false. Depending on the context, the expression may refer to constants.

Please notice that the list of supported operators may change in future releases. For permanent jobs we recommend to limit expressions to the basic operators `<`, `<=`, `==`, `!=`, `>=`, `>`, `+`, `-`, `*`, `/`, `&&`, `||` and `!`.

Options

-accessed=n|\[n\]..\[m\]  
Only copy replicas accessed n seconds ago, or accessed within the given, possibly open-ended, interval; e.g. `-accessed=0..60` matches files accessed within the last minute; `-accesed=60..` matches files accessed one minute or more ago.

-al=ONLINE|NEARLINE  
Only copy replicas with the given access latency.

-pnfsid=pnfsid\[,pnfsid\] ...  
Only copy replicas with one of the given PNFS IDs.

-rp=CUSTODIAL|REPLICA|OUTPUT  
Only copy replicas with the given retention policy.

-size=n|\[n\]..\[m\]  
Only copy replicas with size n, or a size within the given, possibly open-ended, interval.

-state=cached|precious  
Only copy replicas in the given state.

-sticky\[=owner\[,owner...\]\]  
Only copy sticky replicas. Can optionally be limited to the list of owners. A sticky flag for each owner must be present for the replica to be selected.

-storage=class  
Only copy replicas with the given storage class.

-concurrency=concurrency  
Specifies how many concurrent transfers to perform. Defaults to 1.

-order=\[-\]size|\[-\]lru  
Sort transfer queue. By default transfers are placed in ascending order, that is, smallest and least recently used first. Transfers are placed in descending order if the key is prefixed by a minus sign. Failed transfers are placed at the end of the queue for retry regardless of the order. This option cannot be used for permanent jobs. Notice that for pools with a large number of files, sorting significantly increases the initialization time of the migration job.

size  
Sort according to file size.

lru  
Sort according to last access time.

-pins=move|keep  
Controls how sticky flags owned by the CELL-PINMNGR are handled:

move  
Ask CELL-PINMNGR to move pins to the target pool.

keep  
Keep pins on the source pool.

-smode=same|cached|precious|removable|delete\[+owner\[(lifetime)\] ...\]  
Update the local replica to the given mode after transfer:

same  
does not change the local state (this is the default).

cached  
marks it cached.

precious  
marks it precious.

removable  
marks it cached and strips all existing sticky flags exluding pins.

delete  
deletes the replica unless it is pinned.

An optional list of sticky flags can be specified. The lifetime is in seconds. A lifetime of 0 causes the flag to immediately expire. Notice that existing sticky flags of the same owner are overwritten.

-tmode=same|cached|precious\[+owner\[(lifetime)\]...\]  
Set the mode of the target replica:

same  
applies the state and sticky bits excluding pins of the local replica (this is the default).

cached  
marks it cached.

precious  
marks it precious.

An optional list of sticky flags can be specified. The lifetime is in seconds.

-verify  
Force checksum computation when an existing target is updated.

-eager  
Copy replicas rather than retrying when pools with existing replicas fail to respond.

-exclude=pool\[,pool...\]  
Exclude target pools. Single character (`?`) and multi character (`*`) wildcards may be used.

-exclude-when=expression  
Exclude target pools for which the expression evaluates to true. The expression may refer to the following constants:

source.name or target.name  
pool name

source.spaceCost or target.spaceCost  
space cost

source.cpuCost or target.cpuCost  
cpu cost

source.free or target.free  
free space in bytes

source.total or target.total  
total space in bytes

source.removable or target.removable  
removable space in bytes

source.used or target.used  
used space in bytes

-include=pool\[,pool...\]  
Only include target pools matching any of the patterns. Single character (`?`) and multi character (`*`) wildcards may be used.

-include-when=expression  
Only include target pools for which the expression evaluates to true. See the description of -exclude-when for the list of allowed constants.

-refresh=time  
Specifies the period in seconds of when target pool information is queried from the pool manager. The default is 300 seconds.

-select=proportional|best|random  
Determines how a pool is selected from the set of target pools:

proportional  
selects a pool with a probability inversely proportional to the cost of the pool.

best  
selects the pool with the lowest cost.

random  
selects a pool randomly.

The default is proportional.

-target=pool|pgroup|link  
Determines the interpretation of the target names. The default is 'pool'.

-pause-when=expression  
Pauses the job when the expression becomes true. The job continues when the expression once again evaluates to false. The following constants are defined for this pool:

queue.files  
The number of files remaining to be transferred.

queue.bytes  
The number of bytes remaining to be transferred.

source.name  
Pool name.

source.spaceCost  
Space cost.

source.cpuCost  
CPU cost.

source.free  
Free space in bytes.

source.total  
Total space in bytes.

source.removable  
Removable space in bytes.

source.used  
Used space in bytes.

targets  
The number of target pools.

-permanent  
Mark job as permanent.

-stop-when=expression  
Terminates the job when the expression becomes true. This option cannot be used for permanent jobs. See the description of -pause-when for the list of constants allowed in the expression.

migration info
--------------

migration info - Shows detailed information about a migration job.

synopsis
---------
migration info <job> 

DESCRIPTION
===========

Shows detailed information about a migration job. Possible job states are:

INITIALIZING  
Initial scan of repository

RUNNING  
Job runs (schedules new tasks)

SLEEPING  
A task failed; no tasks are scheduled for 10 seconds

PAUSED  
Pause expression evaluates to true; no tasks are scheduled for 10 seconds.

STOPPING  
Stop expression evaluated to true; waiting for tasks to stop.

SUSPENDED  
Job suspended by user; no tasks are scheduled

CANCELLING  
Job cancelled by user; waiting for tasks to stop

CANCELLED  
Job cancelled by user; no tasks are running

FINISHED  
Job completed

FAILED  
Job failed. Please check the log file for details.

Job tasks may be in any of the following states:

Queued  
Queued for execution

GettingLocations  
Querying PnfsManager for file locations

UpdatingExistingFile  
Updating the state of existing target file

CancellingUpdate  
Task cancelled, waiting for update to complete

InitiatingCopy  
Request send to target, waiting for confirmation

Copying  
Waiting for target to complete the transfer

Pinging  
Ping send to target, waiting for reply

NoResponse  
Cell connection to target lost

Waiting  
Waiting for final confirmation from target

MovingPin  
Waiting for pin manager to move pin

Cancelling  
Attempting to cancel transfer

Cancelled  
Task cancelled, file was not copied

Failed  
The task failed

Done  
The task completed successfully

migration ls
-------------

migration ls - Lists all migration jobs.

synopsis
-----------
migration ls

DESCRIPTION
===========

Lists all migration jobs.

migration move
---------------
migration move - Moves replicas to other pools.

synopsis
--------
migration move [<options>] <target>... 


DESCRIPTION
===========

Moves replicas to other pools. The source replica is deleted. Similar to `migration copy`, but with different defaults. Accepts the same options as `migration copy`. Equivalent to: `migration copy` -smode=delete -tmode=same -pins=move

migration suspend
-------------------

migration suspend - Suspends a migration job.

synopsis
----------
migration suspend job

DESCRIPTION
===========
Suspends a migration job. A suspended job finishes ongoing transfers, but is does not start any new transfer.

migration resume
-----------------

migration resume - Resumes a suspended migration job.

synopsis
---------
migration resume job

DESCRIPTION
===========

Resumes a suspended migration job.

  [???]: #cf-pm-classic-space
