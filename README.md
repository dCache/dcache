dCache
======

<img src="dCache.png" height="165" width="200">

__dCache__ is a system for storing and retrieving huge amounts of data,
distributed among a large number of heterogeneous server nodes, under
a single virtual filesystem tree with a variety of standard access
methods. Depending on the Persistency Model, dCache provides methods
for exchanging data with backend (tertiary) Storage Systems as well
as space management, pool attraction, dataset replication, hot spot
determination and recovery from disk or node failures. Connected to
a tertiary storage system, the cache simulates unlimited direct
access storage space. Data exchanges to and from the underlying HSM
are performed automatically and invisibly to the user. Beside HEP
specific protocols, data in dCache can be accessed via __NFSv4.1
(pNFS)__, __FTP__ as well as through __WebDav__.

[![DOI](https://zenodo.org/badge/9113580.svg)](https://zenodo.org/badge/latestdoi/9113580)

Documentation
=============

[The dCache book](docs/TheBook/src/main/markdown/index.md)

[User Guide](docs/UserGuide/src/main/markdown/index.md)

Getting Started
===============

The file [BUILDING.md](BUILDING.md) describes how to compile dCache
code and build various packages.

The file also describes how to create the __system-test__ deployment,
which provides a quick and easy way to get a working dCache.  Running
system-test requires no special privileges and all the generated files
reside within the code-base.

There are also packages of stable releases at https://www.dcache.org/downloads/.

License
=======

The project is licensed under __AGPL v3__. Some parts licensed under __BSD__ and __LGPL__. See the source code for details.

For more info, check the official [dCache.ORG](http://www.dcache.org) web page.

Contributors
============
dCache is a joint effort between
[Deutsches Elektronen-Synchrotron DESY](http://www.desy.de),
[Fermi National Accelerator Laboratory](http://www.fnal.gov)
and [Nordic DataGrid Facility](http://www.ndgf.org).

Contributions are welcome! Please check out our [CONTRIBUTING guide](CONTRIBUTING.md).
