THE CLEANER SERVICE
==================================

The `cleaner` service is the component that watches for files being deleted in the namespace. When files are deleted, the cleaner will notify the pools that hold a copy of the deleted files' data and tell the pools to remove that data. Optionally, the cleaner can instruct HSM-attached pools to remove copies of the file's data stored on tape.

The cleaner runs periodically, so there may be a delay between a file being deleted in the namespace and the corresponding deletion of that file's data in pools and on tape. The cleaner maintains a list of pools that it was unable to contact: pools are either offline or sufficiently overloaded that
they couldn't respond in time. The cleaner will periodically try to delete data on pools in this list, but between such retries these pools are excluded from cleaner activity.

-----
[TOC bullet hierarchy]
-----

## Configuration

The cleaner service can be run in high availability (HA) mode (coordinated via [ZooKeeper](config-zookeeper.md)) by having several cleaner cells in a dCache instance, which then need to share the same database. Only one such cleaner instance will be active at any point in time. If the currently active one ("master") is unavailable, another one is automatically taking its place.

### Disk Cleaner

The disk cleaner is responsible for removing disk-resident file replicas.

The `cleaner.limits.batch-size` property places an upper limit on the number of files' data to be deleted in a message. If more than this number of files are to be deleted then the pool will receive multiple messages.

### HSM Cleaner

If the property `cleaner.enable.hsm = true` is set to true, the cleaner will also instruct HSM-attached pools to remove deleted files' data stored in the HSM. To enable this feature, the property must be enabled at all the pools that are supposed to delete files from an HSM.

Regularly, the cleaner fetches hsm locations of files to delete from the database and caches them for batched dispatch via the property `cleaner.limits.hsm-batch-size`. In order to not overload the memory of a cleaner cell, the number of hsm delete locations that are cached at any point in time should be limited. The `cleaner.limits.hsm-max-cached-locations = 12000` allows to set such a limit.

