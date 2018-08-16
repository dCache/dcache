General
=======

Installing dCache on Opteron Machines
=====================================

The PNFS server, dCache server, and dCache client software have to be taken care of: 

The dCache Server
-----------------

The major part of the dCache server software is written in Java. Therefore the Java Virtual Machine with 64 bit extension needs to be installed. It either is included in the regular Java distribution or additional packages have to be downloaded.

The dCache Client
-----------------

The DCAP library and the command line tool `dccp` may be downloaded from [http://www.dcache.org/downloads/] for several architectures. The source of the client software may also be downloaded from <http://www.dcache.org/downloads/cvs.shtml> and compiled. As of this writing, this has not been tested for the Opteron architecture. Please, contact <support@dcache.org> when encountering any problems with this.

PNFS Server
-----------

The current version of the PNFS server software is written in C and has never been compiled for any 64-bit architecture. Since a Java implementation is in preparation, there are no plans to do that. Therefore the PNFS server has to be run in “compat mode”.

  [http://www.dcache.org/downloads/]: http://www.dcache.org/downloads.shtml
