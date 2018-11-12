CHAPTER 22. dCache CLIENTS.
==========================

Table of Contents
-----------------

+ [GSI-FTP](#gsi-ftp)  

    [Listing a directory](#listing-a-directory)   
    [Checking a file exists](#checking-a-file-exists)  
    [Deleting files](#deleting-files)  
    [Copying files](#copying-files)  

+ [dCap](#dcap)  

    [dccp](#dccp)  
    [Using the dCache client interposition library.](#using-the-dcache-client-interposition-library.)  

+ [SRM](#srm)  

    [Creating a new directory.](#creating-a-new-directory)  
    [Removing files from dCache](#removing-files-from-dcache)  
    [Removing empty directories from dCache](#removing-empty-directories-from-dcache)  
    [srmcp for SRM v1](#srmcp-for-srm-v1)  
    [srmcp for SRM v2.2](#srmcp-for-srm-v2.2)  

+ [ldap](#ldap)  
+ [Using the LCG commands with dCache](#using-the-lcg-commands-with-dcache)  

    [The lcg-gt Application](#the-lcg-gt-application)  
    [The lcg-sd Application](#the-lcg-sd-application)  

There are many client tools for dCache. These can most easily be classified by communication protocol. 


As can be seen from above even a single node standard install of dCache returns a considerable number of lines and for this reason we have not included the output, in this case 205 lines where written. 


GSI-FTP
=======

dCache provides a GSI-FTP door, which is in effect a GSI authenticated FTP access point to dCache

Listing a directory
-------------------

To list the content of a dCache directory, the GSI-FTP protocol can be used;

    [user] $ edg-gridftp-ls gsiftp://gridftp-door.example.org/pnfs/example.org/data/dteam/



Checking a file exists
----------------------

To check the existence of a file with GSI-FTP.

    [user] $ edg-gridftp-exists gsiftp://gridftp-door.example.org/pnfs/example.org/data/dteam/filler_test20050819130209790873000
    [user] $ echo $?
    0
    [user] $ edg-gridftp-exists gsiftp://gridftp-door.example.org/pnfs/example.org/data/dteam/filler_test200508191302097908730002
    error the server sent an error response: 451 451 /pnfs/example.org/data/dteam/filler_test200508191302097908730002  not found
    [user] $ echo $?
    1

> **Use the return code**
>
> Please note the `echo $?` show the return code of the last run application. The error message returned from the client this should not be scripted against as it is one of many possible errors.

Deleting files
--------------

To delete files with GSI-FTP use the `edg-gridftp-rm` command.

    [user] $ edg-gridftp-rm gsiftp://gridftp-door.example.org/pnfs/example.org/data/dteam/filler_test20050811160948926780000

This deletes the file **filler_test20050811160948926780000** from the **/pnfs/example.org/data/dteam** using the door running on the host gridftp-door.example.org within the dCache cluster example.org

Copying files
-------------

globus-url-copy
\[command line options\]
[<srcUrl>]
[<destinationUrl>] 
...
Copying file with `globus-url-copy` follows the syntax source, destination.

Example:
The following example copies the file **/etc/group** into dCache as the file **/pnfs/example.org/data/dteam/test_GlobusUrlCopy.clinton.504.22080.20071102160121.2**

    [user] $ globus-url-copy \
    file://///etc/group \
    gsiftp://gridftp-door.example.org/pnfs/example.org/data/dteam/test_GlobusUrlCopy.clinton.504.22080.20071102160121.2

Please note that the five slashes are really needed.

DCAP
====

When using `dccp` client or using the interposition library the errors `Command failed!` can be safely ignored.

DCCP
----

The following example shows `dccp` being used to copy the file **/etc/group** into dCache as the the file **/pnfs/example.org/data/dteam/test6**. The `dccp` program will connect to dCache without authenticating. 

     [user] $ /opt/d-cache/dcap/bin/dccp /etc/group dcap://dcap-door.example.org:22125/pnfs/example.org/data/dteam/test6
     Command failed!
     Server error message for [1]: "path /pnfs/example.org/data/dteam/test6 not found" (errno 10001).
     597 bytes in 0 seconds

The following example shows `dccp` being used to upload the file **/etc/group**. In this example, dccp will authenticate with dCache using the GSI protocol. 

    [user] $ /opt/d-cache/dcap/bin/dccp /etc/group gsidcap://gsidcap-door.example.org:22128/pnfs/example.org/data/dteam/test5
    Command failed!
    Server error message for [1]: "path /pnfs/example.org/data/dteam/test5 not found" (errno 10001).
    597 bytes in 0 seconds

The following example shows `dccp` with the debugging enabled. The value `63` controls how much information is displayed.

    [user] $ /opt/d-cache/dcap/bin/dccp -d 63   /etc/group dcap://dcap-door.example.org:22128/pnfs/example.org/data/dteam/test3
    Dcap Version version-1-2-42 Jul 10 2007 19:56:02
    Using system native stat64 for /etc/group.
    Allocated message queues 0, used 0

    Using environment variable as configuration
    Allocated message queues 1, used 1

    Creating a new control connection to dcap-door.example.org:22128.
    Activating IO tunnel. Provider: [libgsiTunnel.so].
    Added IO tunneling plugin libgsiTunnel.so for dcap-door.example.org:22128.
    Setting IO timeout to 20 seconds.
    Connected in 0.00s.
    Removing IO timeout handler.
    Sending control message: 0 0 client hello 0 0 2 42 -uid=501 -pid=32253 -gid=501
    Server reply: welcome.
    dcap_pool:  POLLIN on control line [3] id=1
    Connected to dcap-door.example.org:22128
    Sending control message: 1 0 client stat "dcap://dcap-door.example.org:22128/pnfs/example.org/data/dteam/test3" -uid=501
    Command failed!
    Server error message for [1]: "path //pnfs/example.org/data/dteam/test3 not found" (errno 10001).
    [-1] unpluging node
    Removing unneeded queue [1]
    [-1] destroing node
    Real file name: /etc/group.
    Using system native open for /etc/group.
    extra option:  -alloc-size=597
    [Fri Sep  7 17:50:56 2007] Going to open file dcap://dcap-door.example.org:22128/pnfs/example.org/data/dteam/test3 in cache.
    Allocated message queues 2, used 1

    Using environment variable as configuration
    Activating IO tunnel. Provider: [libgsiTunnel.so].
    Added IO tunneling plugin libgsiTunnel.so for dcap-door.example.org:22128.
    Using existing control connection to dcap-door.example.org:22128.
    Setting hostname to dcap-door.example.org.
    Sending control message: 2 0 client open "dcap://dcap-door.example.org:22128/pnfs/example.org/data/dteam/test3" w -mode=0644 -truncate dcap-door.example.org 33122 -timeout=-
    1 -onerror=default  -alloc-size=597  -uid=501
    Polling data for destination[6] queueID[2].
    Got callback connection from dcap-door.example.org:35905 for session 2, myID 2.
    cache_open -> OK
    Enabling checksumming on write.
    Cache open succeeded in 0.62s.
    [7] Sending IOCMD_WRITE.
    Entered sendDataMessage.
    Polling data for destination[7] queueID[2].
    [7] Got reply 4x12 bytes len.
    [7] Reply: code[6] response[1] result[0].
    get_reply: no special fields defined for that type of response.
    [7] Got reply 4x12 bytes len.
    [7] Reply: code[7] response[1] result[0].
    get_reply: no special fields defined for that type of response.
    [7] Expected position: 597 @ 597 bytes written.
    Using system native close for [5].
    [7] unpluging node
    File checksum is: 460898156
    Sending CLOSE for fd:7 ID:2.
    Setting IO timeout to 300 seconds.
    Entered sendDataMessage.
    Polling data for destination[7] queueID[2].
    [7] Got reply 4x12 bytes len.
    [7] Reply: code[6] response[4] result[0].
    get_reply: no special fields defined for that type of response.
    Server reply: ok destination [2].
    Removing IO timeout handler.
    Removing unneeded queue [2]
    [7] destroing node
    597 bytes in 0 seconds
    Debugging

Using the dCache client interposition library.
----------------------------------------------

> **Finding the GSI tunnel.**
>
> When the LD\_PRELOAD library `libpdcap.so` variable produces errors finding the GSI tunnel it can be useful to specify the location of the GSI tunnel library directly using the following command:
>
>     [user] $ export
>     dCache_IO_TUNNEL=/opt/d-cache/dcap/lib/libgsiTunnel.so
>
> Please see [http://www.dcache.org/manuals/experts_docs/tunnel-HOWTO.html](https://www.dcache.org/manuals/experts_docs/tunnel-HOWTO.html) for further details on tunnel setup for the server.

dCap is a POSIX like interface for accessing dCache, allowing unmodified applications to access dCache transparently. This access method uses a proprietary data transfer protocol, which can emulate POSIX access across the LAN or WAN.

Unfortunately the client requires inbound connectivity and so it is not practical to use this protocol over the WAN as most sites will not allow inbound connectivity to worker nodes.

To make non dCache aware applications access files within dCache through DCAP all that is needed is set the LD\_PRELOAD environment variable to `/opt/d-cache/dcap/lib/libpdcap.so`.

    [user] $ export LD_PRELOAD=/opt/d-cache/dcap/lib/libpdcap.so

Setting the LD\_PRELOAD environment variable results in the library `libpdcap.so` overriding the operating system calls. After setting this environment variable, the standard shell command should work with DCAP and GSIDCAP URLs.

Example:

The following session demonstrates copying a file into dCache, checking the file is present with the `ls` command, reading the first 3 lines from dCache and finally deleting the file.

    [user] $ cp /etc/group gsidcap://gsidcap-door.example.org:22128/pnfs/example.org/data/dteam/myFile
    [user] $ ls gsidcap://gsidcap-door.example.org:22128/pnfs/example.org/data/dteam/DirOrFile
    [user] $ head -3 gsidcap://gsidcap-door.example.org:22128/pnfs/example.org/data/dteam/myFile
    root:x:0:
    daemon:x:1:
    bin:x:2:
    [user] $ rm gsidcap://gsidcap-door.example.org:22128/pnfs/example.org/data/dteam/MyFile
    
SRM
===

dCache provides a series of clients one of which is the `SRM?  client which supports a large number operations, but is just one Java application, the script name is sent to the Java applications command line to invoke each operation.

This page just shows the scripts command line and not the invocation of the Java application directly.

Creating a new directory.
-------------------------

Usage:

	srmmkdir
	\[[command line options\]]
	[srmUrl]

Example:

The following example creates the directory **/pnfs/example.org/data/dteam/myDir**.

    [user] $ srmmkdir srm://srm-door.example.org:8443/pnfs/example.org/data/dteam/myDir


Removing files from dCache
--------------------------

Usage:

	srmrm
	\[[command line options\]]
	 [srmUrl ...]
	...

Example:
    [user] $ srmrm srm://srm-door.example.org:8443/pnfs/example.org/data/dteam/myDir/myFile

Removing empty directories from dCache
--------------------------------------

It is allowed to remove only empty directories as well as trees of empty directories.

Usage:

    srmrmdir [command line options] [srmUrl]

Examples:

    [user] $ srmrmdir srm://srm-door.example.org:8443/pnfs/example.org/data/dteam/myDir
    
Examples:    

    [user] $ srmrmdir -recursive=true srm://srm-door.example.org:8443/pnfs/example.org/data/dteam/myDir

srmcp for SRM v1
----------------

Usage:

    srmcp [command line options] source... [destination]

or

    srmcp [command line options] [-copyjobfile] file


### Copying files to dCache

Example:
   [user] $ srmcp -webservice_protocol=http \
   file://///etc/group \
   srm://srm-door.example.org:8443/pnfs/example.org/data/dteam/test_Srm.clinton.501.32050.20070907153055.0

### Copying files from dCache

    [user] $ srmcp -webservice_protocol=http \
   srm://srm-door.example.org:8443/pnfs/example.org/data/dteam/test_Srm.clinton.501.32050.20070907153055.0 \
   file://///tmp/testfile1 -streams_num=1

srmcp for SRM v2.2
------------------

### Getting the dCache Version

The `srmping` command will tell you the version of dCache. This only works for authorized users and not just authenticated users.

   [user] $ srmping -2 srm://srm-door.example.org:8443/pnfs
   WARNING: SRM_PATH is defined, which might cause a wrong version of srm client to be executed
   WARNING: SRM_PATH=/opt/d-cache/srm
   VersionInfo : v2.2
   backend_type:dCache
   backend_version:production-1-9-1-11

### Space Tokens

Space token support must be set up and reserving space with the admin interface this is also documented [in the SRM section](config-SRM.md#introduction) and in [the dCache wiki](http://trac.dcache.org/wiki/manuals/SRM_2.2_Setup).

#### Space Token Listing

Usage:

    srm-get-space-tokens [command line options] [srmUrl]

Example 22.1. surveying the space tokens available in a directory.

    [user] $ srm-get-space-tokens srm://srm-door.example.org:8443/pnfs/example.org/data/dteam -srm_protocol_version=2

A successful result:

    return status code : SRM_SUCCESS
    return status expl. : OK
    Space Reservation Tokens:
    148241
    148311
    148317
    28839
    148253
    148227
    148229
    148289
    148231
    148352

Example 22.2. Listing the space tokens for a SRM:

    [user] $ srm-get-space-tokens srm://srm-door.example.org:8443
    Space Reservation Tokens:
    145614
    145615
    144248
    144249
    25099
    145585
    145607
    28839
    145589

#### Space Reservation

Usage:

    srm-reserve-space [[command line options]] [srmUrl]

Example:

    [user] $ srm-reserve-space  \
    -desired_size 2000 \
    -srm_protocol_version=2 \
    -retention_policy=REPLICA \
    -access_latency=ONLINE \
    -guaranteed_size 1024 \
    -lifetime 36000 \
    srm://srm-door.example.org:8443/pnfs/example.org/data/dteam

A successful result:

    Space token =144573

A typical failure

    SRMClientV2 : srmStatusOfReserveSpaceRequest , contacting service httpg://srm-door.example.org:8443/srm/managerv2
    status: code=SRM_NO_FREE_SPACE explanantion= at Thu Nov 08 15:29:44 CET 2007 state Failed :  no space available
    lifetime = null
    access latency = ONLINE
    retention policy = REPLICA
    guaranteed size = null
    total size = 34

Also you can get info for this space token `144573`:

    [user] $ srm-get-space-metadata srm://srm-door.example.org:8443/pnfs/example.org/data/dteam -space_tokens=144573

Possible result:

    Space Reservation with token=120047
                       owner:VoGroup=/dteam VoRole=NULL
                   totalSize:1024
              guaranteedSize:1024
                  unusedSize:1024
            lifetimeAssigned:36000
                lifetimeLeft:25071
               accessLatency:ONLINE
             retentionPolicy:REPLICA

#### Writing to a Space Token

Usage: 

    srmcp \[command line options\] source(s) destination

Examples:

    [user] $ srmcp -protocols=gsiftp -space_token=144573 \
    file://///home/user/path/to/myFile \
    srm://srm-door.example.org:8443/pnfs/example.org/data/dteam/myFile

    [user] $ srmcp -protocols=gsiftp -space_token=144573 \
    file://///home/user/path/to/myFile1 \
    file://///home/user/path/to/myFile2 \
    srm://srm-door.example.org:8443/pnfs/example.org/data/dteam

#### Space Metadata

Users can get the metadata available for the space, but the ability to query the metadata of a space reservation may be restricted so that only certain users can obtain this information.

    [user] $ srm-get-space-metadata srm://srm-door.example.org:8443/pnfs/example.org/data/dteam -space_tokens=120049
    WARNING: SRM_PATH is defined, which might cause a wrong version of srm client to be executed
    WARNING: SRM_PATH=/opt/d-cache/srm
    Space Reservation with token=120049
                       owner:VoGroup=/dteam VoRole=NULL
                   totalSize:1024
              guaranteedSize:1024
                  unusedSize:1024
            lifetimeAssigned:36000
                lifetimeLeft:30204
               accessLatency:ONLINE
             retentionPolicy:REPLICA

#### Space Token Release

Removes a space token from the SRM.

    [user] $ srm-release-space srm://srm-door.example.org:8443 -space_token=15

### Listing a file in SRM

SRM version 2.2 has a much richer set of file listing commands.

Usage:

    srmls [command line options] srmUrl... 

Example 22.3. Using srmls -l:
    [user] $ srmls srm://srm-door.example.org:8443/pnfs/example.org/data/dteam/testdir  -2
      0 /pnfs/example.org/data/dteam/testdir/
          31 /pnfs/example.org/data/dteam/testdir/testFile1
          31 /pnfs/example.org/data/dteam/testdir/testFile2
          31 /pnfs/example.org/data/dteam/testdir/testFile3
          31 /pnfs/example.org/data/dteam/testdir/testFile4
          31 /pnfs/example.org/data/dteam/testdir/testFile5

> **Note**
>
> The `-l` option results in `srmls` providing additional information. Collecting this additional information may result in a dramatic increase in execution time.

     [user] $ srmls -l srm://srm-door.example.org:8443/pnfs/example.org/data/dteam/testdir -2
     0 /pnfs/example.org/data/dteam/testdir/
      storage type:PERMANENT
      retention policy:CUSTODIAL
      access latency:NEARLINE
      locality:NEARLINE
     locality: null
       UserPermission: uid=18118 PermissionsRWX
       GroupPermission: gid=2688 PermissionsRWX
      WorldPermission: RX
     created at:2007/10/31 16:16:32
     modified at:2007/11/08 18:03:39
       - Assigned lifetime (in seconds):  -1
      - Lifetime left (in seconds):  -1
      - Original SURL:  /pnfs/example.org/data/dteam/testdir
     - Status:  null
     - Type:  DIRECTORY
          31 /pnfs/example.org/data/dteam/testdir/testFile1
          storage type:PERMANENT
          retention policy:CUSTODIAL
          access latency:NEARLINE
          locality:NEARLINE
          - Checksum value:  84d007af
          - Checksum type:  adler32
           UserPermission: uid=18118 PermissionsRW
           GroupPermission: gid=2688 PermissionsR
          WorldPermission: R
         created at:2007/11/08 15:47:13
         modified at:2007/11/08 15:47:13
           - Assigned lifetime (in seconds):  -1
          - Lifetime left (in seconds):  -1
          - Original SURL:  /pnfs/example.org/data/dteam/testdir/testFile1
     - Status:  null
     - Type:  FILE

If you have more than 1000 entries in your directory then dCache will return only the first 1000. To view directories with more than 1000 entries, please use the following parameters:

srmls parameter

-count=integer
The number of entries to report.

-offset=integer

Example 22.5. Limited directory listing

The first command shows the output without specifying `-count` or `-offset`. Since the directory contains less than 1000 entries, all entries are listed.

      [user] $ srmls srm://srm-door.example.org:8443/pnfs/example.org/data/dteam/dir1 \
      srm://srm-door.example.org:8443/pnfs/example.org/data/dteam/dir2
      0 /pnfs/example.org/data/dteam/dir1/
          31 /pnfs/example.org/data/dteam/dir1/myFile1
          28 /pnfs/example.org/data/dteam/dir1/myFile2
          47 /pnfs/example.org/data/dteam/dir1/myFile3
      0 /pnfs/example.org/data/dteam/dir2/
          25 /pnfs/example.org/data/dteam/dir2/fileA
          59 /pnfs/example.org/data/dteam/dir2/fileB

The following examples shows the result when using the `-count` option to listing the first three entries.

    [user] $ srmls -count=3 srm://srm-door.example.org:8443/pnfs/example.org/data/dteam/testdir  -srm_protocol_version=2
0 /pnfs/example.org/data/dteam/testdir/
      31 /pnfs/example.org/data/dteam/testdir/testFile1
      31 /pnfs/example.org/data/dteam/testdir/testFile2
      31 /pnfs/example.org/data/dteam/testdir/testFile3

In the next command, the `-offset` option is used to view a different set of entries.

    [user] $ srmls -count=3 -offset=1 srm://srm-door.example.org:8443/pnfs/example.org/data/dteam/testdir  -srm_protocol_version=2
    0 /pnfs/example.org/data/dteam/testdir/
          31 /pnfs/example.org/data/dteam/testdir/testFile2
          31 /pnfs/example.org/data/dteam/testdir/testFile3
          31 /pnfs/example.org/data/dteam/testdir/testFile4

ldap
====

dCache is commonly deployed with the BDII. The information provider within dCache publishes information to BDII. To querying the dCache BDII is a matter of using the standard command ldapsearch. For grid the standard ldap port is set to 2170 from the previous value of 2135.


    [user] $ ldapsearch -x -H ldap://localhost:2170 -b mds-vo-name=resource,o=grid > /tmp/ldap.output.ldif
    [user] $ wc -l  /tmp/ldap.output.ldif
    205 /tmp/ldap.output.ldif

As can be seen from above even a single node standard install of dCache returns a considerable number of lines and for this reason we have not included the output, in this case 205 lines where written.

Using the LCG commands with dCache
==================================

The `lcg_util` RPM contains many small command line applications which interact with SRM implementations, these where developed independently from dCache and provided by the LCG grid computing effort.

Each command line application operates on a different method of the SRM interface. These applications where not designed for normal use but to provide components upon which operations can be built.

`lcg-gt` queries the BDII information server. This adds an additional requirement that the BDII information server can be found by `lcg-gt`, please only attempt to contact servers found on your user interface using.

    [user] $ lcg-infosites --vo dteam se
    
    
The `lcg-gt` Application
------------------------

`SRM` provides a protocol negotiating interface, and returns a TURL (transfer URL). The protocol specified by the client will be returned by the server if the server supports the requested protocol.

To read a file from dCache using `lcg-gt` you must specify two parameters the SURL (storage URL), and the protcol (`GSIdCap` or `GSI-FTP`) you wish to use to access the file. 

    [user] $ lcg-gt srm://srm-door.example.org/pnfs/example.org/data/dteam/group gsidcap
     gsidcap://gsidcap-door.example.org:22128/pnfs/example.org/data/dteam/group
     -2147365977
     -2147365976

Each of the above three lines contains different information. These are explained below.

`gsidcap://gsidcap-door.example.org:22128/pnfs/example.org/data/dteam/group` is the transfer URL (TURL).

`-2147365977` is the SRM `Request
	  Id`, Please note that it is a negative number in this example, which is allowed by the specification.

`-2147365976` is the Unique identifier for the file with respect to the `Request
	  Id`. Please note that with this example this is a negative number.

> **Remember to return your Request Id**
>
> dCache limits the number of Request Ids a user may have. All Request Ids should be returned to dCache using the command `lcg-sd`. 

If you use `lcg-gt` to request a file with a protocol that is not supported by dCache the command will block for some time as dCache's SRM interface times out after approximately 10 minutes.

The `lcg-sd` Application
------------------------

This command should be used to return any TURLs given by dCache's SRM interface. This is because dCache provides a limited number of TURLs available concurrently.

`lcg-sd` takes four parameters: the SURL, the `Request Id`, the `File
          Id` with respect to the `Request
          Id`, and the direction of data transfer.

The following example is to complete the get operation, the values are taken form the above example of `lcg-gt`.

    [user] $ lcg-sd srm://srm-door.example.org:22128/pnfs/example.org/data/dteam/group " -2147365977" " -21

> **Negative numbers**
>
> dCache returns negative numbers for `Request
> 	    Id` and `File Id`. Please note that `lcg-sd` requires that these values are places in double-quotes with a single space before the `-` sign.

The `Request Id` is one of the values returned by the `lcg-gt` command. In this example, the value (`-2147365977`) comes from the above example `lcg-gt`.

The `File Id` is also one of the values returned returned by the `lcg-gt` command. In this example, the value (`-2147365976`) comes from the above example `lcg-gt`.

The direction parameter indicates in which direction data was transferred: `0` for reading data and `1` for writing data.

<!--  []: http://www.dcache.org/manuals/experts_docs/tunnel-HOWTO.html
  [in the SRM section]: #cf-srm-intro
  [the dCache wiki]: http://trac.dcache.org/projects/dcache/wiki/manuals/SRM_2.2_Setup
