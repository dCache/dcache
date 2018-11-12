Preface
=======

#### Table of Contents

[Minimum System Requirements](#minimum-system-requirements)  
[Structure of this book](#structure-of-this-book)  

Welcome to dCache.

dCache is a distributed storage system proven to scale to hundreds of Petabytes. 
Originally conceived as a disk cache (hence the name) in front of a tertiary storage to
provide efficient data access for data intensive scientific experiments in the field of High
Energy Physics (HEP) it has evolved into highly scalable general-purpose open source
storage solution.   
 
dCache is a joint effort between the Deutsches Elektronen-Synchrotron (DESY) in
Hamburg, Nordic Data Grid Facility (NeIC, NDGF) based in Copenhagen, the Fermi
National Accelerator Laboratory near Chicago with significant distributions and
support from the University of California, San Diego, INFN, Bari as well as
Rutherford Appleton Laboratory, UK and CERN in Geneva.

## Features and Concepts

### Usable storage backends

A dCache instance will generally consist of many storage (or "pool") nodes.
On those nodes, normal Linux filesystems (btrfs, ext4, XFS, ZFS) are used 
to store data. 

Alternatively, dCache pools can use storage space provided by a Ceph object
storage system.

In addition to those possibilities, dCache can use its hierarchical storage
management capabilities to transparently use  storage systems with different characteristics (like tape libraries for lower-cost, but higher-latency 
storage). Built-in mechanisms can be used to increase performance and balance loads,
increase resilience and availability. dCache also supplies advanced control systems
to manage data as well as data flows. 

### Data access protocols

dCache supports the following I/O (and data management) protocols:

-   **dCap** 

-   **FTP** (including **GridFTP**)

-   **HTTP** (and **WEBDAV**)
 
-   **NFS** (parallel NFSv4)

-   **SRM** 

-   **XRootD**

### Authentication and Authorization

dCache supports **X.509** certificate based authentication through the Grid Security
Infrastructure used as well as **username/password** authentication and **LDAP**. For
some workloads, users can also authenticate using **macaroons** or **OpenID Connect**.

dCache provides fine-grained POSIX and **NFS**-style access control list (ACLs) 
based file/directory authorization. 

### Other features

-   Resilience and high availability can be configured by enabling multiple 
    file replicas and flexible variety of replica placement policies. 

-   Easy migration of data via the migration module.

-   The "billing" system provides a  powerful cost calculation system 
    that allows to control the data flow
    (reading and writing from/to data servers, between data servers and 
    also between data servers and tape).

-   Load balancing and performance tuning by hot pool replication (via cost
    calculation and replicas created by pool-to-pool-transfers).

-   Space management and support for space tokens.

-   Detailed logging and debugging as well as accounting and statistics.

-   XML information provider with detailed live information about the system.

-   Scriptable adminstration interface with a terminal-based front-end.

-   Web-interface with live information of the most important information.

-   Automatic checksumming for data integrity.


## Minimum System Requirements

For a minimal test installation:

-   Hardware: contemporary CPU, 1 GiB of RAM , 500 MiB free hard disk space

-   Software: OpenJDK 8 or later, Postgres SQL Server (9.6 or later)

For high performance production scenarios, the hardware requirements greatly
differ, which makes it impossible to provide such parameters here. However, if
you wish to setup a dCache-based storage system, just let us know and we will
help you with your system specifications. Just contact us: <support@dcache.org>.

## Structure of this book

This book shall introduce you to dCache and provide you with the details of the
installation. It describes the configuration and customization of dCache as well as the
usage of several protocols that dCache supports. Additionally, it provides
cookbooks for standard tasks.

Here is an overview part by part:

Part 1, Getting started: This part introduces you to the cell and domain
concepts in dCache. It provides a detailed description of installing, the basic
configuration, and upgrading dCache.

Part 2, Configuration of dCache: Within this part the configuration of several
additional features of dCache is described. They are not necessary to run dCache
but will be needed by some users depending on their requirements.

Part 3, Cookbook: This part comprises guides for specific tasks a system
administrator might want to perform.
