CHAPTER 17. THE BULK SERVICE
===============================

The purpose of the bulk service is to manage mass file system operations involving lists of files
and directories, the latter possibly treated recursively.  In one sense it is meant to incorporate 
into dCache proper some of the functionality provided by SRM (such as prestaging of files).

-----
[TOC bullet hierarchy]
-----

## Configuring

The service can run in a shared domain, but it is probably best to isolate it in its own.  

```
[bulkDomain]
[bulkDomain/bulk]
[bulkDomain/ping]
```

The module comes with an internal service (``ping``) which can be enabled, but this is 
strictly for the convenience of testing without requiring the test jobs to do
any real work (to simulate how jobs that wait synchronously –– such as
those which stage from tape –– perform under load, for instance); 
the ``ping`` service is not relevant to running in a production environment.

The rule-of-thumb for memory requirements is described in the properties file:

```
#       Given approximately 1KB of memory for a job, multiply this figure
#       by 2, and you will have a minimal memory requirement for just the
#       queues in which jobs could be present for a longer period of time.
#       Thus the default value given below means 2GB (in which
#       case the domain JVM should be given somewhat more than 2GB
#       of heap memory).
```

The service has a number of settings for controlling the size of the queues;
adjusting these upwards will also mean making sure the JVM is given enough
heap memory to accommodate them.

See the ``bulk.properties`` file for further information on settings.

## Requests, Jobs, and Fairness

The bulk service receives requests from the frontend service 
(see the User Manual [Chapter 3. Frontend](frontend.md#bulk-operations])) 
via dCache messaging and processes them on a first-come-first-served basis. 
Only a fixed number of requests can be running at a given time. 
A very simple algorithm is used that loops through the active requests 
and picks one job at a time from each until the maximum number of running 
jobs is reached.   Any given user is allowed to have only a fixed number of 
requests active at a given time (this is adjustable). 

## Storage and Recovery

Requests are serialized and written out to a file when they are received.  Upon
completion, the request is updated and marked complete if it is to be retained
(the option exists to remove a request immediately upon completion, and can
be selected for successful, failed or both for any given request).
Request and job information is otherwise stored in memory.  When the service
fails or goes down, an attempt will be made to replay incomplete requests upon
restart.  Since job and request state is not persistent, every such request 
is treated as if it were idempotent, and rerun in its entirety; the original
queueing order for the incomplete requests (based on timestamp) is, however,
preserved.  In the future, depending on need/demand, we may review whether to
implement finer grained checkpointing.

## Job plugins

Job types are defined via an SPI ("Service Provider Interface") as plugins to
the service.  Aside from two built-in test jobs which simply list directories
using either breadth-first or depth-first tree walks, three plugins to the
service are currently provided:

- **Pin/Unpin**: the pin is issued on behalf of the user.  The default lifetime is
             five minutes (the same as for the NFS dot-command).
- **QoS**:  for issuing disk-to-tape and tape-to-disk transitions.
- **Delete**: file deletion, with or without removal of empty directories.

The arguments and defaults for each of these tasks can be inspected via the
``admin`` shell (see below).

New types of jobs require implementing a job class which 
extends``org.dcache.services.bulk.job.SingleTargetJob``
as well as a job provider class 
implementing ``org.dcache.services.bulk.job.BulkJobProvider``.   A job type
can receive dCache endpoints to the ``namespace``, ``poolmanager``, ``pinmanager`` 
and ``poolmonitor`` by implementing the appropriate -``Aware`` interface.  Every
job inherits access to the file path, attributes, and user subject and restriction.
This makes it fairly straightforward to add new plugins in the future.

## Admin commands

The bulk service comes with a full set of admin shell commands which allow one
to list the running jobs and stored requests, to launch and cancel requests, 
to list the available plugin types and to inspect the arguments used when
submitting a request of a particular type. 