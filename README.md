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

How to contribute
=================

**dCache** uses the linux kernel model where git is not only source repository,
but also the way to track contributions and copyrights.

Each submitted patch must have a "Signed-off-by" line.  Patches without
this line will not be accepted.

The sign-off is a simple line at the end of the explanation for the
patch, which certifies that you wrote it or otherwise have the right to
pass it on as an open-source patch.  The rules are pretty simple: if you
can certify the below:
```

    Developer's Certificate of Origin 1.1

    By making a contribution to this project, I certify that:

    (a) The contribution was created in whole or in part by me and I
         have the right to submit it under the open source license
         indicated in the file; or

    (b) The contribution is based upon previous work that, to the best
        of my knowledge, is covered under an appropriate open source
        license and I have the right under that license to submit that
        work with modifications, whether created in whole or in part
        by me, under the same open source license (unless I am
        permitted to submit under a different license), as indicated
        in the file; or

    (c) The contribution was provided directly to me by some other
        person who certified (a), (b) or (c) and I have not modified
        it.

    (d) I understand and agree that this project and the contribution
        are public and that a record of the contribution (including all
        personal information I submit with it, including my sign-off) is
        maintained indefinitely and may be redistributed consistent with
        this project or the open source license(s) involved.

```
then you just add a line saying ( git commit -s )

    Signed-off-by: Random J Developer <random@developer.example.org>

using your real name (sorry, no pseudonyms or anonymous contributions.)

We use an adapted version of the [`Google style guide for Java`](https://github.com/google/styleguide) that can be found in the root of this project for IntelliJ.
The used reformatting involves optimization of imports (reordering), application of all syntactical sugar settings, but does not include code rearrangement (fields, methods, classes) or code cleanup for existing code. Reformatting should be applied to the changed code before submitting a patch.
