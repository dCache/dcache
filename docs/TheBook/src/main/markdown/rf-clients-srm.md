Chapter 27. dCache Clients
===========================

Table of Contents
-----------------

+ [The SRM Client Suite](#the-srm-client-suite)  

  [srmcp](#srmcp) — Copy a file from or to an SRM or between two SRMs.  
  [srmstage](#srmstage) — Request staging of a file.  

+ [dccp](#dccp)  
  [dccp](#dccp) — Copy a file from or to a dCache server.  


The SRM Client Suite
====================

An SRM URL has the form <srm://dmx.lbl.gov:6253//srm/DRM/srmv1?SFN=/tmp/try1> and the file URL looks like <file:////tmp/aaa>.

srmcp
-----
srmcp - Copy a file from or to an SRM or between two SRMs.

synopsis
--------

srmcp|option...|sourceUrl destUrl

Arguments
  sourceUrl  
    The URL of the source file.

destUrl  
    The URL of the destination file.

Options

**gss_expected_name**

To enable the user to specify the gss expected name in the DN (Distinguished Name) of the srm server. The default value is `host`.

Example:
If the CN of host where srm server is running is `CN=srm/tam01.fnal.gov`, then `gss_expected_name` should be `srm`.

    [user] $ srmcp --gss_expected_name=srm sourceUrl destinationUrl


**globus_tcp_port_range**

To enable the user to specify a range of ports open for tcp connections as a pair of positive integers separated by “`:`”, not set by default.

This takes care of compute nodes that are behind firewall.

Example:
`globus_tcp_port_range=40000:50000` 

    [user] $ srmcp --globus_tcp_port_range=minVal:maxVal sourceUrl destinationUrl

**streams_num**

To enable the user to specify the number of streams to be used for data transfer. If set to 1, then stream mode is used, otherwise extended block mode is used.

Example:
    [user] $  srmcp --streams_num=1 sourceUrl destinationUrl

**server_mode**  
To enable the user to set the (gridftp) server mode for data transfer. Can be `active` or `passive`, `passive` by default.

This option will have effect only if transfer is performed in a stream mode (see `streams_num`)

Example:
    [user] $  srmcp --streams_num=1 --server_mode=active sourceUrl destinationUrl

Description
-----------

srmstage
--------
srmstage - Request staging of a file.

synopsis
--------
srmstage[srmUrl]

Arguments
srmUrl  
The URL of the file which should be staged.

Description
-----------

Provides an option to the user to stage files from HSM to dCache and not transfer them to the user right away. This case will be useful if files are not needed right away at user end, but its good to stage them to dcache for faster access later.

dccp
----

dccp — Copy a file from or to a dCache server.

Synopsis
---------
dccp [option...] <sourceUrl> <destUrl>

Arguments
---------

The following arguments are required:

**sourceUrl**  

    The URL of the source file. 

**destUrl**

    The URL of the destination file. 

Description
------------
The dccp utility provides a cp(1) like functionality on the dCache file system. The source must be a single file while the destination could be a directory name or a file name. If the directory is a destination, a new file with the same name as the source name will be created there and the contents of the source will be copied. If the final destination file exists in dCache, it won’t be overwritten and an error code will be returned. Files in regular file systems will always be overwritten if the -i option is not specified. If the source and the final destination file are located on a regular file system, the dccp utility can be used similar to the cp(1) program.

Options
--------
The following arguments are optional:

**-a**

    Enable read-ahead functionality. 

**-b <bufferSize>**

    Set read-ahead buffer size. The default value is 1048570 Bytes. To disable the buffer this can be set to any value below the default. dccp will attempt to allocate the buffer size so very large values should be used with care. 

**-B <bufferSize>**

    Set buffer size. The size of the buffer is requested in each request, larger buffers will be needed to saturate higher bandwidth connections. The optimum value is network dependent. Too large a value will lead to excessive memory usage, too small a value will lead to excessive network communication. 
-d <debug level>

    Set the debug level. <debug level> is a integer between 0 and 127. If the value is 0 then no output is generated, otherwise the value is formed by adding together one or more of the following values:
    
**Value	Enabled output**
  1	    Error messages
  2	    Info messages
  4	    Timing information
  8	    Trace information
  16  	Show stack-trace
  32	  IO operations
  32  	IO operations
  64	  Thread information

**-h <replyHostName>**

    Bind the callback connection to the specific hostname interface. 

**-i**

    Secure mode. Do not overwrite the existing files. 

**-l <location>**

    Set location for pre-stage. if the location is not specified, the local host of the door will be used. This option must be used with the -P option. 

**-p <first_port>:<last_port>**

    Bind the callback data connection to the specified TCP port/rangeSet port range. Delimited by the ’:’ character, the <first_port> is required but the <last_port> is optional. 

**-P**

    Pre-stage. Do not copy the file to a local host but make sure the file is on disk on the dCache server. 

**-r <bufferSize>**

    TCP receive buffer size. The default is 256K. Setting to 0 uses the system default value. Memory useage will increase with higher values, but performance better. 

**-s <bufferSize>**

    TCP send buffer size. The default is 256K. Setting to 0 uses the system default value. 

**-t <time>**

    Stage timeout in seconds. This option must be used with the -P option. 

Examples:
---------

To copy a file to dCache:

[user] $ dccp /etc/group dcap://example.org/pnfs/desy.de/gading/

To copy a file from dCache:

[user] $ dccp dcap://example.org/pnfs/desy.de/gading/group /tmp/

Pre-Stage request:

[user] $ dccp -P -t 3600 -l example.org /acs/user_space/data_file

stdin:

[user] $ tar cf - data_dir | dccp - /acs/user_space/data_arch.tar

stdout:

[user] $ dccp /acs/user_space/data_arch.tar - | tar xf - 

See also
--------

cp 

