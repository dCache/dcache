Preface
=======

**Table of Contents**

[Minimum System Requirements?](#minimum-system-requirements)  
[What is inside?](#what-is-inside)  

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

dCache can use hierarchical storage management (e.g., hard disk and tape),
provides mechanisms to automatically increase performance and balance loads,
increase resilience and availability. It also supplies advanced control systems
to manage data as well as data flows. Normal Linux filesystems (btrfs, ext4,
XFS, ZFS) are used to store data on storage nodes.

dCache supports the following I/O (and data management) protocols:

-   **dCap** 

-   **FTP** (including **GridFTP**)

-   **HTTP** (end **WEBDAV**)
 
-   **NFS** 

-   **SRM** 

-   **XRootD**


dCache supports X.509 certificate based authentication through the Grid Security
Infrastructure used as well as username/password authentication and LDAP.

dCache provides fine-grained POSIX and **NFS**-style access control list (ACLs) 
based file/directory authorization. 

Other features of dCache are:

-   Resilience and high availability can be configured by enabled multiple 
    file replicas and flexible variety of replica placement policies. 

-   Easy migration of data via the migration module.

-   A powerful cost calculation system that allows to control the data flow
    (reading and writing from/to data servers, between data servers and 
    also between data servers and tape).

-   Load balancing and performance tuning by hot pool replication (via cost
    calculation and replicas created by pool-to-pool-transfers).

-   Space management and support for space tokens.

-   Garbage collection of replicas, depending on their flags, age and other criteria.

-   Detailed logging and debugging as well as accounting and statistics.

-   XML information provider with detailed live information about the system.

-   Scriptable adminstration interface with a terminal-based front-end.

-   Web-interface with live information of the most important information.

-   Automatic checksumming for data integrity.

<!---dCache / **SRM** can transparently manage data distributed among dozens of disk
storage nodes (sometimes distributed over several countries). The system has
shown to significantly improve the efficiency of connected tape storage systems,
by caching, gather and flush and scheduled staging techniques. Furthermore, it
optimizes the throughput to and from data clients by dynamically replicating
datasets on the detection of load hot spots. The system is tolerant against
failures of its data servers, which allows administrators to deploy commodity
disk storage components.

Access to the data is provided by various standard protocols. Furthermore the
software comes with an implementation of the Storage Resource Manager protocol
(**SRM**), which is an open standard for grid middleware to communicate with
site specific storage fabrics.-->



Minimum System Requirements
---------------------------

For a minimal test installation:

-   Hardware: contemporary CPU, 1 GiB of RAM , 100 MiB free hard disk space

-   Software: Oracle Java or OpenJDK, Postgres SQL Server

For high performance production scenarios, the hardware requirements greatly
differ, which makes it impossible to provide such parameters here. However, if
you wish to setup a dCache-based storage system, just let us know and we will
help you with your system specifications. Just contact us: <support@dcache.org>.

What is inside?
---------------

This book shall introduce you to dCache and provide you with the details of the
installation. It describes configuration, customization of dCache as well as the
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
