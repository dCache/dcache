CHAPTER 26. ADVANCED TUNING
===========================

The use cases described in this chapter are only relevant for large-scale dCache instances which require special tuning according to a longer experience with client behaviour.

-----
[TOC bullet hierarchy]
-----

## MULTIPLE QUEUES FOR MOVERS IN EACH POOL

### Description

Client requests to a dCache system may have rather diverse behaviour. Sometimes it is possible to classify them into several typical usage patterns. An example are the following two concurrent usage patterns:

Example:

  Data is copied with a high transfer rate to the dCache system from an external source. This is done via the `GridFTP` protocol. At the same time batch jobs on a local farm process data. Since they only need a small part of each file, they use the `dCap` protocol via the `dCap` library and seek to the position in the file they are interested in, read a few bytes, do a few hours of calculations, and finally read some more data.
As long as the number of active requests does not exceed the maximum number of allowed active requests, the two types of requests are processed concurrently. The `GridFTP` transfers complete at a high rate while the processing jobs take hours to finish. This maximum number of allowed requests is set with [mover set max active](reference.md#mover-set-max-active) and should be tuned according to capabilities of the pool host.
However, if requests are queued, the slow processing jobs might clog up the queue and not let the fast `GridFTP` request through, even though the pool just sits there waiting for the processing jobs to request more data. While this could be temporarily remedied by setting the maximum active requests to a higher value, then in turn `GridFTP` request would put a very high load on the pool host.

The above example is pretty realistic: As a rule of thumb, `GridFTP` requests are fastest, `dCap` requests with the dccp program are a little slower and dCap requests with the `dCap` library are very slow. However, the usage patterns might be different at other sites and also might change over time.

### Solution

 Use separate queues for the movers, depending on the door initiating them. This easily allows for a separation of requests of separate protocols. (Transfers from and to a [tape backend](rf-glossary.md#tape-backend) and [pool-to-pool](rf-glossary.md#pool-to-pool-transfer) are handled by separate queues, one for each of these transfers.)

A finer grained queue selection mechanism based on, e.g. the `IP` address of the client or the file which has been requested, is not possible with this mechanism. However, the [pool selection unit (PSU)](rf-glossary.md#pool-selection-unit) may provide a separation onto separate pools using those criteria.

In the above example, two separate queues for fast `GridFTP`  transfers and slow `dCap` library access would solve the problem. The maximum number of active movers for the `GridFTP`  queue should be set to a lower value compared to the `dCap` queue since the fast `GridFTP` transfers will put a high load on the system while the `dCap` requests will be mostly idle.

### Configuration

For a multi mover queue setup, the pools have to be told to start
several queues and the doors have to be configured to use one of
these. It makes sense to create the same queues on all pools. This is
done by the following change to the file `/etc/dcache/dcache.conf`:

```ini
pool.queues=queueA,queueB
```

Each door may be configured to use a particular mover queue. The pool, selected for this request, does not depend on the selected mover queue. So a request may go to a pool which does not have the particular mover queue configured and will consequently end up in the default mover queue of that pool.

```ini
ftp.mover.queue=queueA
dcap.mover.queue=queueB
```

All requests send from this kind of door will ask to be scheduled to the given mover queue. The selection of the pool is not affected.

The doors are configured to use a particular mover queue as in the following example:


Example:

Create the queues `queueA` and `queueB`, where `queueA` shall be the queue for the `GridFTP` transfers and `queueB` for `dCap`.

```ini
pool.queues=queueA,queueB
ftp.mover.queue=queueA
dcap.mover.queue=queueB
```

If the pools should not all have the same queues you can define queues for pools in the layout file. Here you might as well define that a specific door is using a specific queue.

Example:

In this example `queueC`is defined for `pool1` and `queueD` is defined for `pool2`. The GRIDFTP door running in the domain `myDoors` is using the queue `queueB`.

```ini
[myPools]
[myPools/pool1]
pool.queues=queueC
[myPools/pool2]
pool.queues=queueD

[myDoors]
[myDoors/dcap]
dcap.mover.queue=queueC
[myDoors/ftp]
ftp.authn.protocol = gsi
ftp.mover.queue=queueD
```

There is always a default queue called `regular`. Transfers not requesting a particular mover queue or requesting a mover queue not existing on the selected pool, are handled by the `regular` queue.

The pool cell commands [mover ls](reference.md#mover-ls) and [mover set max active ](reference.md#mover-set-max-active) have a `-queue` option to select the mover queue to operate on. Without this option, [mover set max active](reference.md#mover-set-max-active) will act on the default queue while [mover ls](reference.md#mover-ls) will list all active and waiting client transfer requests.

For the `dCap` protocol, it is possible to allow the client to choose
another queue name than the one defined in the file `dcache.conf`. To
achieve this the property `dcap.authz.mover-queue-overwrite` needs to
be set to allowed.

Example:

Create the queues `queueA` and `queue_dccp`, where `queueA` shall be the queue for `dCap`.

```ini
pool.queues=queueA,queue_dccp
dcap.mover.queue=queueA
dcap.authz.mover-queue-overwrite=allowed
```

With the `dccp` command the queue can now be specified as follows:

```console-user
dccp -X-io-queue=queue_dccp <source> <destination>
```

Since `dccp` requests may be quite different from other requests with the `dCap` protocol, this feature may be used to use separate queues for `dccp`  requests and other dCap library requests. Therefore, the `dccp`  command may be changed in future releases to request a special `dccp` -queue by default.

### Tunable Properties for Multiple Queues

| Property                         | Default Value | Description                                                          |
|:---------------------------------|---------------|:---------------------------------------------------------------------|
| pool.queues                      | NO-DEFAULT    | I/O queue name                                                       |
| dcap.mover.queue                 | NO-DEFAULT    | Insecure DCAP I/O queue name                                         |
| dcap.mover.queue                 | NO-DEFAULT    | GSIDCAP I/O queue name                                               |
| dcap.authz.mover-queue-overwrite | denied        | Controls whether an application is allowed to overwrite a queue name |
| dcap.authz.mover-queue-overwrite | denied        | Controls whether an application is allowed to overwrite a queue name |
| dcap.authz.mover-queue-overwrite | denied        | Controls whether an application is allowed to overwrite a queue name |
| ftp.mover.queue                  | NO-DEFAULT    | GSIFTP I/O queue name                                                |
| nfs.mover.queue                  | NO-DEFAULT    | NFS I/O queue name                                                   |
| transfermanagers.mover.queue     | NO-DEFAULT    | queue used for third-party transfers           |
| webdav.mover.queue               | NO-DEFAULT    | WEBDAV and HTTP I/O queue name                                       |
| xrootd.mover.queue               | NO-DEFAULT    | XROOTD I/O queue name                                                |

## Tunable Properties

### DCAP

| Property                         | Default Value | Description                                     |
|:---------------------------------|---------------|:------------------------------------------------|
| dcap.mover.queue                 | NO-DEFAULT    | GSIDCAP I/O queue name                          |
| dcap.mover.queue                 | NO-DEFAULT    | Insecure DCAP I/O queue name                    |
| dcap.authz.mover-queue-overwrite | denied        | Is application allowed to overwrite queue name? |
| dcap.authz.mover-queue-overwrite | denied        | Is application allowed to overwrite queue name? |

### GRIDFTP

| Property                        | Default Value                              | Description                                    |
|:--------------------------------|--------------------------------------------|:-----------------------------------------------|
| ftp.net.port.gsi                | 2811                                       | GSIFTP port listen port                        |
| spaceReservation                | FALSE                                      | Use the space reservation service              |
| spaceReservationStrict          | FALSE                                      | Use the space reservation service              |
| ftp.performance-marker-period   | 180                                        | Performance markers in seconds                 |
| ftp.service.poolmanager.timeout | 5400                                       | Pool Manager timeout in seconds                |
| ftp.service.pool.timeout        | 600                                        | Pool timeout in seconds                        |
| ftp.service.pnfsmanager.timeout | 300                                        | Pnfs timeout in seconds                        |
| ftp.limits.retries              | 80                                         | Number of PUT/GET retries                      |
| ftp.limits.streams-per-client   | 10                                         | Number of parallel streams per FTP PUT/GET     |
| ftp.enable.delete-on-failure    | TRUE                                       | Delete file on connection closed               |
| ftp.limits.clients              | 100                                        | Maximum number of concurrently logged in users |
| ftp.net.internal                | NO-DEFAULT                                 | In case of two interfaces                      |
| ftp.net.port-range              | 20000:25000                                | The client data port range                     |
| gplazma.kpwd.file               | `${ourHomeDir}/etc/dcache.kpwd`            | Legacy authorization                           |

  [???]: #cmd-mover_set_max_active
  [1]: #cmd-mover_ls
