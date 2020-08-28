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

The property `pinmanager.limits.pin-duration` allows to configure the maximum allowed lifetime of pins. The default value is `-1`, which corresponds to an infinite lifetime.