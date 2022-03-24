Chapter 3. Tertiary storage
===========================

**Table of Contents**

+ [Writing data to tertiary storage](#writing-data-to-tertiary-storage)
+ [Reading data back from tertiary storage](#reading-data-back-from-tertiary-storage)
+ [Manually staging data](#manually-staging-data)
+ [Removing a pin](#removing-a-pin)
+ [File locality](#file-locality)

Tertiary storage is storage that is, in some sense, external to
dCache's directly managed disk storage.  This is typically a tape
silo: a device where data is stored on different magnetic tape
cassettes that are handled by a robotic system.  However, the
interface between dCache and the tertiary storage is quite generic and
dCache may be integrated with many different types of external storage
systems (e.g., cloud storage).

In some sense, files stored on tertiary storage are no different from
regular files: they may be uploaded and subsequently read back using
any of the supported protocols.  There are, however, some differences.
These are discussed below.

There may be a delay in reading data back from tertiary storage.  This
delay can be significant, depending on the technology involved and how
many concurrent requests the tertiary storage is handling.  Therefore,
tertiary storage is sometimes referred to as "nearline" storage, to
distinguish the storage from "online" storage.

dCache maintains a limited cache that it uses to store the data of
tertiary-stored files.  When a client wishes to read a tertiary-stored
file from dCache and the file's data is already in this cache then
reading the file takes place as usual.  If the file's data is not in
this cache then dCache must first fetch the data, which may take some
time.  There are some strategies you can adopt for mitigating this
delay.

### Writing data to tertiary storage

The simplest approach is for the dCache admins to have configured
certain directories so that any file written there will end up on
tertiary storage.  This approach is available to all network protocols
and only requires that you write to the correct directory.

Another approach is to indicate that the file should end up on
tertiary storage when uploading the file.  Currently only the SRM
protocol supports this option.

The final option is to write a file to dCache normally, using any
supported protocol, and subsequently change the Quality of Service
(QoS) for that file, indicating that the file should be stored on
tertiary storage.  The [section on modifying
QoS](frontend.md#modifying-qos) provides more details.

[Monitoring the file's locality](#file-locality) provides a way to
discover when a file has been written to tertiary storage.

### Reading data back from tertiary storage

A file may be read back by any protocol.  If the file's data is
already in the disk cache then the file may be read without any delay,
otherwise reading the file will automatically trigger staging of the
file.

The request to read the file will block until the file has been
successfully staged back from the tertiary storage.  Most clients will
not wait indefinitely, so you may see timeout errors if the staging
process takes too long.

### Manually staging data

A common strategy for handling tertiary-stored files is to prestage
the data back from the tertiary storage.  This does not make the
process any faster, but it is an asynchronous operation, allowing the
client to wait patiently for the task to complete.

For example, when analysing a dataset, the client can first prestage
all the files in that dataset.  Once all files are available on disk,
the client can launch an analysis job in a batch system.  This has the
advantage that the analysis can start quickly, without the risk of the
batch system terminating the job while it waits for files to be
staged.

#### Staging with a pin

A pin is a promise from dCache to protect the file from garbage
collection for the desired period.  Without a pin, the disk-resident
replica of a tertiary-only file's data could be garbage collected
before you are finished with it.

It is very common to pin a file when staging its data back from
tertiary storage.

The SRM, Frontend, NFS and dcap protocols support prestaging a file
and then pinning the file.

#### Staging without a pin

The dcap protocol supports this.  However, there are limited use-cases
for staging without a pin.  In most cases, clients are recommended to
stage a file with a pin.

#### QoS transition

It is possible to adjust the QoS from `tape` to `disk+tape`.  Doing
this will trigger staging the file back from the tertiary storage.

This QoS change has no time limit: there will be a copy of the file's
data on dCache managed disks until there is a subsequent QoS
transition.

The frontend may be used to initiate QoS transitions.

### Removing a pin

When staging a tertiary-stored file's data, that data takes up disk
space.  When the file is pinned, dCache cannot garbage-collect the
data.  Under extreme circumstances, this may result in dCache running
out of disk space: preventing it from receiving further files.

Therefore, it is recommended to remove pins promptly when the file's
data is no longer needed with online latency.

Both the SRM, REST and NFS protocols allow a client to remove pins.

### File locality

A file's locality describes on which media the file's data resides.
Under normal circumstances, a file has one of three localities:
`ONLINE`, `ONLINE+NEARLINE` and `NEARLINE`.  The terms may vary
slightly, depending on which the protocol is used to query the
locality.

The locality `ONLINE` means that file's data is stored only on
dCache's directly managed disk storage.  This is the locality of files
that are not intended for tertiary storage, or have not yet been
written to tertiary storage.

A file with `NEARLINE` locality has its data stored only on tertiary
storage or reading the file requires dCache to make an internal copy
of the file.  In either case, the file cannot be read immediately.

A file with `ONLINE+NEARLINE` locality has its data stored both on
dCache's directly managed disk storage and on the tertiary storage.
Reading a file with `ONLINE+NEARLINE` locality is possible with no
delay; however, the file locality (by itself) does not mean there is
any guarantee the file will remain on disk.  There are other
mechanisms to provide such a guarantee: pinning and QoS.

There are two main reasons why you might want to learn a file's
locality.  First, it may take some time for a newly written file to be
stored on the tertiary storage.  Monitoring the file locality provides
a way to discover when the file is actually written to the tertiary
storage.  Second, a file that was written to tertiary storage might
still be cached in dCache's directly managed disk storage.  The file
locality provides a way of discovering this information.

#### Discovering file locality

The SRM protocol supports the `srmLs` command, which returns (amongst
other information) the file locality.

The WebDAV protocol supports properties as key-value pairs describing
a file or directory.  The `srm:FileLocality` property (where `srm` is
the XML namespace `http://srm.lbl.gov/StorageResourceManager`) is the
file's locality.  Properties are described further in [the WebDAV
chapter](webdav.md#properties).

The Frontend provides a REST interface to dCache.  It allows querying
of dCache file locality via the `namespace` resource.  More details on
how to discover metadata about files are available in [the Frontend
chapter](frontend.md#discovering-metadata).

The NFS interface's `get` dot command supports querying locality with
commands like `.(get)(<filename>)(locality)`.  This file contains the
locality of the file `<filename>`.  More details are available in [the
NFS chapter](nfs.md).

The Frontend provides a notification system, where an event is
triggered when something "interesting" happens.  More details are
available in [the section on storage
events](frontend.md#storage-events).

An inotify `IN_ATTR` event is generated when a file is written to the
tertiary storage.  A client can subscribe to such events to learn when
that file's data has been written to the tertiary storage.  Note that
other activity may generate an `IN_ATTR` event, so a client receiving
the `IN_ATTR` event must also check the current locality of a file
using any of the above methods.
