THE PINMANAGER SERVICE
==================================

The purpose of the `pinmanager` service in dCache is ensuring the presence of a file replica on disk.

It can be used by explicitly _(un)pinning_ files via the `admin` interface or the `bulk service`, but is also used by the `resilience` component to ensure having a certain number of replicas available. PinManager is also used for keeping a replica _online_ after fetching (staging) it from a connected tertiary storage system (sometimes called HSM - hierarchical storage manager), if staging is allowed.

-----
[TOC bullet hierarchy]
-----

## (Un-)Pinning Concept

A `pin`, also called `sticky`-ness, is a concept describing a file replica on a pool that cannot be deleted for a certain duration. The pin effectively suppresses automatic garbage collection for the lifetime of the pin.

Pins may have a finite or infinite lifetime. Pins also have an owner, which may be a dCache service (such as `resilience`) or client through a protocol such as `srm`. Only the owner is allowed to remove unexpired pins.
Several pins (for different users) can exist for the same `pnfsid`, and a file is considered pinned as long as at least one unexpired pin exists.

## The Pin Life Cycle

When a pin is created, it will initially appear in state `PINNING`, then transition to state `PINNED` once the attempt is successful.

When a pin either has a finite lifetime that has expired or is directly requested to be removed, it is put into state `READY_TO_UNPIN`. An 'unpinning' background task runs regularly (default every minute), which selects a certain number of pins (default 200) in state `READY_TO_UNPIN` and attempts to remove them, during which the pins are in state `UNPINNING`.

On success, the pin is deleted from the pool in question as well as the database, of failure the pin is put into state `FAILED_TO_UNPIN`. Another background process regularly (default every 2h) resets all pins in state `FAILED_TO_UNPIN` back to state `READY_TO_UNPIN` in order to make them eligible to be attempted again.


## Configuring

The PinManager service can be run in a shared domain. It may also be deployed in high availability (HA) mode (coordinated via [ZooKeeper](config-zookeeper.md)) by having several PinManager cells in a dCache instance, which then need to share the same database and configuration.

```
pinmanager.db.host=pinman-db-hostname
pinmanager.db.name=dcache
pinmanager.db.password=
pinmanager.db.user=dcache
```

Pins are managed in this central database as well as on the pools containing the replicas.

Pin expiration and pin unpinning are background tasks which are executed regularly. The property `pinmanager.expiration-period` controls how often to execute these tasks. The default value is 60 seconds.

The number of pins that should at most be attempted to be removed per unpinning task run can be configured with the property `pinmanager.max-unpins-per-run` and default to 200. A value of -1 indicates that there is no limit on the number of pins the `PinManager` will attempt to unpin per run, which might lead to large CPU and memory loads if there are many pending unpin operations.

Another background task takes care of resetting pins that previously failed to be removed. It can be configured via `pinmanager.reset-failed-unpins-period` and defaults to 2h.
