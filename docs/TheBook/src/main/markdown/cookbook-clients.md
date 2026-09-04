CHAPTER 22. dCache CLIENTS.
==========================

There are many client tools for dCache. These can most easily be classified by communication protocol.

-----
[TOC bullet hierarchy]
-----

## GSI-FTP

dCache provides a GSI-FTP door, which is in effect a GSI authenticated FTP access point to dCache

### Listing a directory

To list the content of a dCache directory, the GSI-FTP protocol can be used;

```console-user
edg-gridftp-ls gsiftp://dcache.example.org/pnfs/example.org/data/dteam/
```


### Checking a file exists

To check the existence of a file with GSI-FTP.

```console-user
edg-gridftp-exists gsiftp://dcache.example.org/pnfs/example.org/data/dteam/filler_test20050819130209790873000
echo $?
|0
edg-gridftp-exists gsiftp://dcache.example.org/pnfs/example.org/data/dteam/filler_test200508191302097908730002
|error the server sent an error response: 451 451 /pnfs/example.org/data/dteam/filler_test200508191302097908730002  not found
echo $?
|1
```

> **Use the return code**
>
> Please note the `echo $?` show the return code of the last run application. The error message returned from the client this should not be scripted against as it is one of many possible errors.

### Deleting files

To delete files with GSI-FTP use the `edg-gridftp-rm` command.

```console-user
edg-gridftp-rm gsiftp://dcache.example.org/pnfs/example.org/data/dteam/filler_test20050811160948926780000
```

This deletes the file `filler_test20050811160948926780000` from the
`/pnfs/example.org/data/dteam` using the door running on the host
dcache.example.org within the dCache cluster example.org

### Copying files

Copying file with `globus-url-copy` follows the syntax source, destination:

    globus-url-copy [command line options] [<srcUrl>] [<destinationUrl>]

Example: The following example copies the file `/etc/group` into
dCache as the file `/data/test.txt`

```console-user
globus-url-copy file://///etc/group \
|    gsiftp://dcache.example.org/data/test.txt
```

Please note that the five slashes in `file://///` are really needed.

## DCAP

When using `dccp` client or using the interposition library the errors `Command failed!` can be safely ignored.

### DCCP

The following example shows `dccp` being used to copy the file
`/etc/group` into dCache as the the file `/data/test.txt`. The `dccp`
program will connect to dCache without authenticating.

```console-user
dccp /etc/group dcap://dcache.example.org:22125/data/test.txt
|Command failed!
|Server error message for [1]: "path /data/test.txt not found" (errno 10001).
|597 bytes in 0 seconds
```

The following example shows `dccp` being used to upload the file
`/etc/group`. In this example, dccp will authenticate with dCache
using the GSI protocol.

```console-user
dccp /etc/group gsidcap://dcache.example.org:22128/data/test.txt
|Command failed!
|Server error message for [1]: "path /pnfs/example.org/data/test.txt not found" (errno 10001).
|597 bytes in 0 seconds
```

The following example shows `dccp` with the debugging enabled. The value `63` controls how much information is displayed.

```console-user
dccp -d 63   /etc/group dcap://dcache.example.org:22128/data/test.txt
|Dcap Version version-1-2-42 Jul 10 2007 19:56:02
|Using system native stat64 for /etc/group.
|Allocated message queues 0, used 0
|
|Using environment variable as configuration
|Allocated message queues 1, used 1
|
|Creating a new control connection to dcache.example.org:22128.
|Activating IO tunnel. Provider: [libgsiTunnel.so].
|Added IO tunneling plugin libgsiTunnel.so for dcache.example.org:22128.
|Setting IO timeout to 20 seconds.
|Connected in 0.00s.
|Removing IO timeout handler.
|Sending control message: 0 0 client hello 0 0 2 42 -uid=501 -pid=32253 -gid=501
|Server reply: welcome.
|dcap_pool:  POLLIN on control line [3] id=1
|Connected to dcache.example.org:22128
|Sending control message: 1 0 client stat "dcap://dcache.example.org:22128/data/test.txt" -uid=501
|Command failed!
|Server error message for [1]: "path //data/test.txt not found" (errno 10001).
|[-1] unpluging node
|Removing unneeded queue [1]
|[-1] destroing node
|Real file name: /etc/group.
|Using system native open for /etc/group.
|extra option:  -alloc-size=597
|[##TODAY_DAY_OF_WEEK## ##TODAY_MONTH_NAME## ##TODAY_DAY_OF_MONTH## ##HH:MM:SS## ##TODAY_YEAR##] Going to open file dcap://dcache.example.org:22128/data/test.txt in cache.
|Allocated message queues 2, used 1
|
|Using environment variable as configuration
|Activating IO tunnel. Provider: [libgsiTunnel.so].
|Added IO tunneling plugin libgsiTunnel.so for dcache.example.org:22128.
|Using existing control connection to dcache.example.org:22128.
|Setting hostname to dcache.example.org.
|Sending control message: 2 0 client open "dcap://dcache.example.org:22128/data/test.txt" w -mode=0644 -truncate dcache.example.org 33122 -timeout=-
|1 -onerror=default  -alloc-size=597  -uid=501
|Polling data for destination[6] queueID[2].
|Got callback connection from dcache.example.org:35905 for session 2, myID 2.
|cache_open -> OK
|Enabling checksumming on write.
|Cache open succeeded in 0.62s.
|[7] Sending IOCMD_WRITE.
|Entered sendDataMessage.
|Polling data for destination[7] queueID[2].
|[7] Got reply 4x12 bytes len.
|[7] Reply: code[6] response[1] result[0].
|get_reply: no special fields defined for that type of response.
|[7] Got reply 4x12 bytes len.
|[7] Reply: code[7] response[1] result[0].
|get_reply: no special fields defined for that type of response.
|[7] Expected position: 597 @ 597 bytes written.
|Using system native close for [5].
|[7] unpluging node
|File checksum is: 460898156
|Sending CLOSE for fd:7 ID:2.
|Setting IO timeout to 300 seconds.
|Entered sendDataMessage.
|Polling data for destination[7] queueID[2].
|[7] Got reply 4x12 bytes len.
|[7] Reply: code[6] response[4] result[0].
|get_reply: no special fields defined for that type of response.
|Server reply: ok destination [2].
|Removing IO timeout handler.
|Removing unneeded queue [2]
|[7] destroing node
|597 bytes in 0 seconds
|Debugging
```

### Using the dCache client interposition library.

> **Finding the GSI tunnel.**
>
> When the LD\_PRELOAD library `libpdcap.so` variable produces errors
> finding the GSI tunnel it can be useful to specify the location of
> the GSI tunnel library directly using the following command:
>
> ```console-user
> export dCache_IO_TUNNEL=/opt/d-cache/dcap/lib/libgsiTunnel.so
> ```
>
> Please see [http://www.dcache.org/manuals/experts_docs/tunnel-HOWTO.html](https://www.dcache.org/manuals/experts_docs/tunnel-HOWTO.html) for further details on tunnel setup for the server.

dCap is a POSIX like interface for accessing dCache, allowing unmodified applications to access dCache transparently. This access method uses a proprietary data transfer protocol, which can emulate POSIX access across the LAN or WAN.

Unfortunately the client requires inbound connectivity and so it is not practical to use this protocol over the WAN as most sites will not allow inbound connectivity to worker nodes.

To make non dCache aware applications access files within dCache through DCAP all that is needed is set the LD\_PRELOAD environment variable to `/opt/d-cache/dcap/lib/libpdcap.so`.

```console-user
export LD_PRELOAD=/opt/d-cache/dcap/lib/libpdcap.so
```

Setting the LD\_PRELOAD environment variable results in the library `libpdcap.so` overriding the operating system calls. After setting this environment variable, the standard shell command should work with DCAP and GSIDCAP URLs.

Example:

The following session demonstrates copying a file into dCache, checking the file is present with the `ls` command, reading the first 3 lines from dCache and finally deleting the file.

```console-user
cp /etc/group gsidcap://dcache.example.org:22128/pnfs/example.org/data/dteam/myFile
ls gsidcap://dcache.example.org:22128/pnfs/example.org/data/dteam/DirOrFile
head -3 gsidcap://dcache.example.org:22128/pnfs/example.org/data/dteam/myFile
|root:x:0:
|daemon:x:1:
|bin:x:2:
rm gsidcap://dcache.example.org:22128/pnfs/example.org/data/dteam/MyFile
```

## ldap

dCache is commonly deployed with the BDII. The information provider within dCache publishes information to BDII. To querying the dCache BDII is a matter of using the standard command ldapsearch. For grid the standard ldap port is set to 2170 from the previous value of 2135.


```console-user
ldapsearch -x -H ldap://localhost:2170 -b mds-vo-name=resource,o=grid > /tmp/ldap.output.ldif
wc -l  /tmp/ldap.output.ldif
|205 /tmp/ldap.output.ldif
```

As can be seen from above even a single node standard install of dCache returns a considerable number of lines and for this reason we have not included the output, in this case 205 lines where written.

