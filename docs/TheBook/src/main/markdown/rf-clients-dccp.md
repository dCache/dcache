PROG-DCCP
=========

PROG-DCCP
PROG-DCCP
Copy a file from or to a dCache server.
PROG-DCCP
option
sourceUrl
destUrl
Arguments
=========

The following arguments are required:

`sourceUrl`  
The URL of the source file.

`destUrl`  
The URL of the destination file.

Description
===========

The PROG-DCCP utility provides a `cp`(1) like functionality on the dCache file system. The source must be a single file while the destination could be a directory name or a file name. If the directory is a destination, a new file with the same name as the source name will be created there and the contents of the source will be copied. If the final destination file exists in dCache, it won't be overwritten and an error code will be returned. Files in regular file systems will always be overwritten if the `-i` option is not specified. If the source and the final destination file are located on a regular file system, the PROG-DCCP utility can be used similar to the `cp`(1) program.

Options
=======

The following arguments are optional:

`-a`  
Enable read-ahead functionality.

`-b` bufferSize  
Set read-ahead buffer size. The default value is `1048570` Bytes. To disable the buffer this can be set to any value below the default. dccp will attempt to allocate the buffer size so very large values should be used with care.

`-B` bufferSize  
Set buffer size. The size of the buffer is requested in each request, larger buffers will be needed to saturate higher bandwidth connections. The optimum value is network dependent. Too large a value will lead to excessive memory usage, too small a value will lead to excessive network communication.

`-d` debug level  
Set the debug level. debug level is a integer between `0` and `127`. If the value is `0` then no output is generated, otherwise the value is formed by adding together one or more of the following values:
**Value:** 1
**Enabled output:** Error messages
**Value:** 2
**Enabled output:** Info messages
**Value:** 4
**Enabled output:** Timing information
**Value:** 8
**Enabled output:** Trace information
**Value:** 16
**Enabled output:** Show stack-trace
**Value:** 32
**Enabled output:** IO operations
**Value:** 32
**Enabled output:** IO operations
**Value:** 64
**Enabled output:** Thread information

`-h` replyHostName  
Bind the callback connection to the specific hostname interface.

`-i`  
Secure mode. Do not overwrite the existing files.

`-l` location  
Set location for pre-stage. if the location is not specified, the local host of the door will be used. This option must be used with the -P option.

`-p` first\_port:last\_port  
Bind the callback data connection to the specified TCP port/rangeSet port range. Delimited by the ':' character, the first\_port is required but the last\_port is optional.

`-P`  
Pre-stage. Do not copy the file to a local host but make sure the file is on disk on the dCache server.

`-r` bufferSize  
TCP receive buffer size. The default is `256K`. Setting to `0` uses the system default value. Memory useage will increase with higher values, but performance better.

`-s` bufferSize  
TCP send buffer size. The default is `256K`. Setting to `0` uses the system default value.

`-t` time  
Stage timeout in seconds. This option must be used with the `-P` option.

Examples:
=========

To copy a file to dCache:

    PROMPT-USER dccp /etc/group dcap://example.org/pnfs/desy.de/gading/

To copy a file from dCache:

    PROMPT-USER dccp dcap://example.org/pnfs/desy.de/gading/group /tmp/

Pre-Stage request:

    PROMPT-USER dccp -P -t 3600 -l example.org /acs/user_space/data_file

stdin:

    PROMPT-USER tar cf - data_dir | dccp - /acs/user_space/data_arch.tar

stdout:

    PROMPT-USER dccp /acs/user_space/data_arch.tar - | tar xf - 

See also
========

cp 1
