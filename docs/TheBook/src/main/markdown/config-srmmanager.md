CHAPTER 12. dCache SRM MANAGER
===========================================

Storage Resource Managers (SRMs) are middleware components whose function is to
provide dynamic space allocation and file management on shared storage components on the Grid.

The SRM service in dCcahe is split between a front end `srm` and a backend `srmmanager` cell for
scalability. To instantiate the SRM service both cells need to be started, not necessarily on the
same host.

The frontend `srm` service can be scaled like any other door. Each instance will
appear as a separate endpoint and the instances will typically talk to the same
`srmmanager` backend. The same techniques as for other doors may be used to make
these appear as a single endpoint to the enduser.

More general information, the `srm` part and dCache-specific details can be
found [here](config-SRM.md).

-----
[TOC bullet hierarchy]
-----

## Configuring the srm manager service

### Basic setup

Example:

To enable SRM in dCache, add the following lines to your layout file:

```ini
[<srm-${host.name}Domain>]
[<srm-${host.name}Domain>/srm]

[srmmanager-${host.name}Domain]
[srmmanager-${host.name}Domain/srmmanager]
[srmmanager-${host.name}Domain/transfermanagers]
```

The additional `transfermanagers` service is required to perform 3rd party copy transfers initiated
by SRM or WebDAV.
This service is not required to be co-located with th SRM service (domain or host).

The srm service requires an authentication setup,
see [Chapter 10, Authorization in dCache](config-gplazma.md) for a general description or the
section [“Authentication and Authorization in dCache”](intouch.md#authentication-and-authorization-in-dcache)
for an example setup with X.509 certificates.

### SRM manager configuration options

The defaults for the following configuration parameters can be found
in the `srmmanager.properties`, file, which is located in the directory
`/usr/share/dcache/defaults`.

If you want to modify parameters, copy them to
`/etc/dcache/dcache.conf` or to your layout file
`/etc/dcache/layouts/mylayout` and update their value.

Example:

Change the value for `srmmanager.db.host` in the layout file.

```ini
[srm-${host.name}Domain]
[srm-${host.name}Domain/srmmanager]
srmmanager.db.host=hostname
```

If a dCache instance contains more than one `srmmanager`, it is necessary that each one has a
_distinct_ database.

## Tape recall scheduling

Srm allows to retrieve data from connected tape archival storage via the `bring-online` command.
Optimally recalling data from tape is achieved by reducing the number of tape mounts and on-tape
seeks by recalling as much volume as possible per mount. To that end, there is a dedicated
scheduling strategy exclusively for `bring-online` requests that is capable of clustering requests
by tape according to a set of configurable criteria.

> **NOTE**
>
> The stage requests that leave the srmmanager tape recall scheduler are spread over all configured
> stage pools according to the PoolManager configuration.

### Overview and behaviour configuration

The bring-online scheduling strategy is for now integrated in the `srmmanager` component within
dCache and allows clustering bring-online requests by tape before passing them on to the rest of the
system. It is important to note that the scheduler can only be effective when a dCache instance
contains exactly one SrmManager. In its current state it requires two files with information on
targeted tapes, their capacity and occupancy as well as the mapping of tape-resident files to tape
name.

When requests arrive at the tape recall scheduler, they are collected and regularly attempted to be
associated with this tape information. When it is missing for a request, the request will be passed
on eventually according to the configured behaviour.

```ini
srmmanager.boclustering.time-in-queue-for-jobs-without-tapeinfo = 10
srmmanager.boclustering.time-in-queue-for-jobs-without-tapeinfo.unit = MINUTES
```

If this property is disabled, the request will stay in the schduler until it exceeds its maximum
allowed lifetime (described below). It might make sense to configure a rather small duration until
these requests without tape info leave the scheduler if several experiments targeting different sets
of tapes use the same dCache instance, and not all of them require recall scheduling. Otherwise,
unassociated requests might severely affect the recall efficiency.

Requests are then clustered by tape. Tapes are _activated_ and their associated requests leave the
scheduler to the PinManager, then the pools to be fetched from the tape system. The `SrmManager` has
a limit on the maximum number of requests that can be in flight per request type at any point in
time, which, depending on the number of requests associated with a tape, limits the number of tapes
that might be active and thus potentially mounted at any point in time. Because most sites use more
than one tape drive, the number of active tapes may be configured

```ini
srmmanager.boclustering.max-active-tapes = 1
```

, which ensures that requests for at least this number of tapes will be leaving the scheduler in
parallel (if requests for that many tapes exist in the scheduler).

A tape is activated if a tape slot is available (number of `max-active-tapes` described above) and
the oldest request for that tape has expired its maximum allowed time in the queue

```ini
srmmanager.boclustering.max-time-in-queue = 2
srmmanager.boclustering.max-time-in-queue.unit = DAYS
```

which is _independent_ of the request's lifetime (!! -- so take care that it is long enough, both in
the client and `srmmanager`).

If no such tape exists, only tapes for which all requests have stayed in the queue for the minimum
required time are considered further:

```ini
srmmanager.boclustering.min-time-in-queue = 2
srmmanager.boclustering.min-time-in-queue.unit = MINUTES
```

If those exist, the tape with the highest request volume that exceeds the configured minimum tape
capacity percentage is selected:

```ini
srmmanager.boclustering.min-tape-recall-percentage = 60
```

If the requested volume for a tape targets over 95 percent of that tape's contained volume, it is
treated as if it exceeds the `min-tape-recall-percentage` as well.

If no such tape exists, one might finally be selected if a configured minimum number of requests is
associated with that tape (if this criterion is not disabled):

```ini
srmmanager.boclustering.min-request-count-for-tape = 1000
```

A small number would ensure that most tapes are activated when no requests targeting it have arrived
for the `min-time-in-queue` duration and a slot for activation is available.

Otherwise, requests for a tape will remain in the queue until its oldest request expires according
to the `max-time-in-queue` parameter.

All these behavioural properties can be changed at runtime via the admin interface as well. These
changes are not persisted on restart, however.

```ini
(SrmManager) admin > trs set tape selection -active=2 -volume=70 -requests=-1
maximum active tapes set to 2
minimum recall percentage set to 70
minimum number of requests per tape disabled

(SrmManager) admin > trs set request stay -min=30 -max=180 -tapeinfoless=10
minimum job waiting time set to 30 minutes
maximum job waiting time set to 180 minutes
tapeinfoless job waiting time set to 10 minutes
```

### Additional configuration options

The scheduler is activated by including the following in the `srmmanager.properties` file:

```ini
srmmanager.plugins.enable-bring-online-clustering = true
```

Because the scheduler potentially handles more requests over a longer period, it might be sensible
to adjust several of `srmmanager`'s `bring-online` related properties, including the following:

```ini
srmmanager.request.bring-online.max-requests = 100000

srmmanager.request.bring-online.lifetime = 5
srmmanager.request.bring-online.lifetime.unit= DAYS
```

Don't forget to adjust request lifetimes on the client side as well.

### Tape information files

In order to make use of the scheduling strategy, tape location information needs to be provided.
Their default location is `/etc/dcache/tapeinfo` but may be configured. When `bring-online` requests
first enter the scheduler, tape information is attempted to be loaded from the provided files and
cached for further usage. It is possible to clear this cache via admin interface to trigger a reload
if the contents of the tape information files have changed:

```ini
(SrmManager) admin > trs reload tape info
Tape information will be reloaded during the next run
```

The tape info provider is pluggable and currently supports two different file types: `CSV`
and `JSON`. The provider can be configured and added to.

```ini
srmmanager.boclustering.plugins.tapeinfoprovider = json
```

Two different tape info files are needed. The `tapes` file contains an entry per tape which includes
its name, capacity and occupancy. The `tapefiles` file includes an entry per file that may be read
from tape, which includes a file identifier, its size and the tape name it is on, which has to match
an entry in the `tapes` file. The file identifi
er is the full srm request path the file is `bring-online` requested with, which the scheduler uses
to match the requested file to entries in the `tapefiles` file.

> **NOTE**
>
> Make sure that the paths in the `tapefiles` file _exactly_ match the request path! The scheduler
> logs the arriving file requests with their full path which can be compared to file entries.

#### JSON format

If the format of the tape info files is configured to be `JSON`, the files need to be
named `tapes.json` and `tapefiles.json`.

The `tapes.json` needs to contain a map with entries of the form

`"<tape name>":{"capacity":<capacity in kB>,"filled":<filled in kB>}`.

Example of `tapes.json` content:

```ini
{
  "tape1":{"capacity":8000000000,"filled":8000000000},
  "tape2":{"capacity":8000000000,"filled":3141592653},
  "tape3":{"capacity":4000000000,"filled":8000000000}
}
```

The `tapefiles.json` needs to contain a map with entries of the form

`"<full srm file path>":{"size":<file size in kB>,"tapeid":"<tape name>"}`.

Example of `tapefiles.json` content:

```ini
{
  "/tape/file-0.log":{"size":1111,"tapeid":"tape1"},
  "/tape/file-1.log":{"size":31415,"tapeid":"tape1"},
  "/tape/file-2.log":{"size":1000000,"tapeid":"tape1"},
  "/tape/file-3.log":{"size":1000,"tapeid":"tape2"},
  "/tape/file-4.log":{"size":5000,"tapeid":"tape3"},
  "/tape/file-5.log":{"size":7000,"tapeid":"tape3"}
}
```

#### CSV format

If the format of the tape info files is configured to be `CSV`-like, the files need to be
named `tapes.txt` and `tapefiles.txt`.

The `tapes.txt` needs to contain a line per tape, each of the form

`<tape name>,<capacity in kB>,<filled in kB>`.

Example of `tapes.txt` content:

```ini
tape1,8000000000,8000000000
tape2,8000000000,3141592653
tape3,4000000000,4000000000
```

The `tapefiles.txt` needs to contain a line per tape file, each of the form

`<full srm file path>,<file size in kB>,<tape name>`.

Example of `tapefiles.txt` content:

```ini
/tape/file-0.log,1111,tape1
/tape/file-1.log,31415,tape1
/tape/file-2.log,1000000,tape1
/tape/file-3.log,1000,tape2
/tape/file-4.log,5000,tape3
/tape/file-5.log,7000,tape3
```
