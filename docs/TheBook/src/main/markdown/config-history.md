CHAPTER 18. dCache History Service
=====================================

### Collection of monitoring data

The purpose of this service is to provide a disk-backed cache for time-windowed
state data extracted from backend dCache components, most importantly, pools.

The service executes collection updates periodically to its write-through cache
so that the data is both stored on disk and held in memory.  In the case of
restart, the existing files are read back.

The Frontend service contacts the history service using normal dCache messaging
in order to serve this data through the RESTful API.

Currently, pool state information is saved by the History service into a
set of .json files; their default location is

    /var/lib/dcache/pool-history

on the file system local to the node running the service.  This location, along
with the collection intervals, can be configured from properties;
see **/usr/share/dcache/defaults/history.properties**.

Running this service is essential for the delivery of histogram data from the
frontend.  It can be run out of the box with no special properties
set:

    [historyDomain]
    [historyDomain/history]

While it is not required to run this service in its own domain, it is usually
good practice to isolate a non-essential service this way, allowing 
a restart which does not affect other services, should that be necessary.
