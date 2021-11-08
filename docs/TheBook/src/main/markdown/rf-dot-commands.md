Specialized NFS Dot Commands
============================

The following commands provide information concerning a file's
locality, checksums and storage location.

## GET LOCALITY

Returns the media types on which the file is currently stored.

### USAGE:

    cat ".(get)(<filename>)(locality)"

### RETURNS:

* ``ONLINE``:                  stored on disk
* ``NEARLINE``:                requires staging or data replication before open succeeds
* ``ONLINE_AND_NEARLINE``:     stored both on disk and tape
* ``UNAVAILABLE``:             not stored on any media (i.e., lost)

### EXAMPLE:

    $ cat ".(get)(test_file-Thu_Oct_23_10:39:37_CDT_2014-109)(locality)"
    $ ONLINE_AND_NEARLINE

### NOTES:

Currently, the NFS client sends 'localhost' to the poolmanager as its hostname (the protocol match defaults to '*/*').  SRM does something similar. In the future this may be modified to send the actual IP address of the client.

There are no guarantees concerning future availability of the file; in particular, ONLINE_AND_NEARLINE may revert back to NEARLINE at any time unless the file is pinned.

***

## GET CHECKSUM TYPES

Get the valid / recognized checksum types for dCache.

### USAGE:

    cat ".(checksums)()"

### RETURNS:

A new-line delimited list of checksum type names.

### EXAMPLE:

    $ cat ".(checksums)()"
    ADLER32
    MD5
    MD4

***

## GET CHECKSUM(S)

Get checksum types and checksums for a given file.

### USAGE:

    cat ".(get)(<filename>)(checksum[s])"

### RETURNS:

A comma-delimited list of `type:value` pairs for all checksums stored in the database.

### EXAMPLE:

    $ cat ".(get)(test_file-Thu_Oct_23_10:39:37_CDT_2014-109)(checksums)"
    $ ADLER32:66300001

***

## SET CHECKSUM

Set a checksum value for a file.   An ordinary user can only set one
checksum (type, value) pair, and cannot overwrite existing values.  A ROOT
user is allowed to set multiple types (successively) and also to overwrite
values for any type.

### USAGE:

    touch ".(fset)(<filename>)(checksum)(<type>)(<value>)"

### EXAMPLES:

    [arossi@otfrid volatile]$ touch testfile

    [arossi@otfrid volatile]$ cat ".(get)(testfile)(checksum)"

    [arossi@otfrid volatile]$ touch ".(fset)(testfile)(checksum)(ADLER32)(ffffffff)"

    [arossi@otfrid volatile]$ cat ".(get)(testfile)(checksum)"
    ADLER32:ffffffff

    [arossi@otfrid volatile]$ touch ".(fset)(testfile)(checksum)(ADLER32)(fffffff0)"
    touch: setting times of ‘.(fset)(testfile)(checksum)(ADLER32)(fffffff0)’: Operation not permitted

    [arossi@otfrid volatile]$ ksu
    Authenticated arossi@FNAL.GOV
    Account root: authorization for arossi@FNAL.GOV successful
    Changing uid to root (0)

    [root@otfrid volatile]# touch ".(fset)(testfile)(checksum)(ADLER32)(fffffff0)"

    [root@otfrid volatile]# cat ".(get)(testfile)(checksum)"
    ADLER32:fffffff0

    [root@otfrid volatile]# touch ".(fset)(testfile)(checksum)(MD5)(12341234123412345678567856785678)"

    [root@otfrid volatile]# cat ".(get)(testfile)(checksum)"
    ADLER32:fffffff0, MD5:12341234123412345678567856785678

***

## GET PIN(S)

Get pins on a given file.

### USAGE:

    cat ".(get)(<filename>)(pins)"

### RETURNS:

A simple table of information about pins.  Each pin is represented by
a row in the table.  Each row has the following information, separated
by the tab character:

  * The (dCache internal) numerical ID for this pin.
  * When the pin was requested in ISO 8601 format.
  * If the pin will be removed automatically, when this will happen, in
    ISO 8601 format; or '-' if the pin will not be remove
    automatically.
  * The description in quote marks for this pin, if any, otherwise the
    '-' character.
  * Whether you can remove this pin: `REMOVABLE` if you can,
    `NONREMOVABLE` otherwise.
  * The current status of this pin: either `PINNING`, `PINNED`, `READY_TO_UNPIN`, `UNPINNING` or `FAILED_TO_UNPIN`.

The PINNING status indicates the pin request was accepted but the file
is not yet guaranteed with ONLINE latency. The PINNED state indicates
the file has ONLINE latency. The READY_TO_UNPIN state indicates this pin
no longer guarantees ONLINE latency, but additional clean-up
processing is ongoing. The UNPINNING state indicates that the pin removal
is ongoing, while state FAILED_TO_UNPIN denotes that the last unpinning
attempt failed. It will be retried later on.

### EXAMPLES:

Response when a file has no pins:

```console-user
cat ".(get)(my-file.dat)(pins)"
```

Response showing a file has two pins in state `PINNING`:

```console-user
cat ".(get)(my-file.dat)(pins)"
|3       2019-08-11T07:33:30.822Z        2019-08-11T07:35:30.830Z        -       NONREMOVABLE    PINNED
|4       2019-08-11T07:33:36.946Z        2019-08-11T07:35:36.960Z        -       REMOVABLE       PINNED
```

***

## PIN/STAGE

Allows users to pin or stage files.

### USAGE:

    touch ".(fset)(<filename>)(<operation>)(<duration>)(<unit>)"

### OPTIONS:

``operation`` can be replaced by:

* ``pin``
* ``stage``
* ``bringonline``
* ``unpin``

The first three are equivalent options.

``duration`` must be 0 or a positive integer: 0 will unpin all pins
for which you are authorised to remove, a positive integer will create
a new pin with that duration.

For ``pin``, ``stage`` and ``bringonline``, this argument is optional
and defaults to 300 (seconds), or 5 minutes.

``unpin`` takes no arguments and is equivalent to ``pin 0``.

``unit`` can be replaced by:

* ``SECONDS``
* ``MINUTES``
* ``HOURS``
* ``DAYS``

This argument is optional and defaults to ``SECONDS``.

### USAGE DETAILS:

A pin is a promise from dCache to store the file on a low-latency
device, such as a disk.  This is useful if the file would otherwise be
stored only on a low-latency device (such as tape) or (for a
distributed dCache deployment) at a remote location.

There are two steps: creating the pin and fulfilling the pin.  This
touch command is the first step, which triggers the processing
necessary for the second step.

If dCache did not accept the pin request (due to some internal error)
then the dCache NFS server will respond with an NFSERR_INVAL error.
The behaviour of the NFS client is implementation specific, but for a
Linux mounted filesystem, this is presented as an error; e.g.,

```console-user
touch '.(fset)(test_file-1.dat)(pin)(60)'
|touch: setting times of '.(fset)(test_file-1.dat)(pin)(60)': Invalid argument
echo $?
|1
```

If the touch command is successful then no error is returned and the
return-code is zero:

```console-user
touch '.(fset)(test_file-1.dat)(pin)(60)'
echo $?
|0
```

The `.(get)(<filename>)(pins)` dot command may be used to track the
pins progress.  The pin's is initially in state PINNING, but will
change to state PINNED once the pin has been fulfilled.

You may use a zero duration to remove all pins for which you are
authorised.  You can remove all pins you created and pins created by
someone with a primary group of which you are also a member.

Immediately after removing a pin, it has state READY_TO_UNPIN. When the
 pin removal starts, it transitions to state UNPINNING. Once all
processing has completed, the pin will disappear from the pin list. If
the pin removal fails, the pin ends up in state FAILED_TO_UNPIN; it will be retried later on.

### EXAMPLES:

Pin file for 30 seconds:

```console-user
touch ".(fset)(test_file-1.dat)(pin)(30)"
```

Pin file for two minutes:

```console-user
touch ".(fset)(test_file-1.dat)(pin)(2)(MINUTES)"
```

Remove all pins on the file:

```console-user
touch ".(fset)(test_file-1.dat)(pin)(0)"
```

For further explanation of pinning, see [Pinning Files to a
Pool](https://www.dcache.org/manuals/Book-2.16/Book-fhs.shtml#cb-pool-pin).

***

## GET/SET TAPE LOCATION URI

Stores, retrieves and modifies the URI string(s) which define an HSM/tape location.  A normal user can read and write once to this file.  Root can overwrite and append as well.

### USAGE:

    cat ".(suri)(<file name>)"
    echo [...] > ".(suri)(<file name>)"
    echo [...] >> ".(suri)(<file name>)"

### RETURNS:

    cat will return a list of locations (there may be multiple ones).

### EXAMPLE:

    $ cat ".(suri)(data001)"
    $ enstore://enstore/?volume=VOL001&location_cookie=0000_000000000_0000001&size=234653&file_family=standard&map_file=&
       pnfsid_file=0000464E839DEAFC428E8CF52D8028455141&pnfsid_map=&bfid=c085615502758c8bb54db4c30081626f&
       origdrive=localhost:/dev/tmp/tps0d0n:1487271284&crc=813392028&original_name=/pnfs/fs/usr/test/arossi/000/data001

    ### overwrite (cookie) [as root]

    $ echo "enstore://enstore/?volume=VOL001&location_cookie=0000_000000000_0000002&size=234653&file_family=standard&
       map_file=&pnfsid_file=0000464E839DEAFC428E8CF52D8028455141&pnfsid_map=&bfid=c085615502758c8bb54db4c30081626f&
       origdrive=localhost:/dev/tmp/tps0d0n:1487271284&crc=813392028&original_name=/pnfs/fs/usr/test/arossi/000/data001"
       > ".(suri)(data001)"

    $ cat ".(suri)(data001)"
    $ enstore://enstore/?volume=VOL001&location_cookie=0000_000000000_0000002&size=234653&file_family=standard&map_file=&
       pnfsid_file=0000464E839DEAFC428E8CF52D8028455141&pnfsid_map=&bfid=c085615502758c8bb54db4c30081626f&
       origdrive=localhost:/dev/tmp/tps0d0n:1487271284&crc=813392028&original_name=/pnfs/fs/usr/test/arossi/000/data001

    ### append (cookie) [as root]

    $ echo "enstore://enstore/?volume=VOL001&location_cookie=0000_000000000_0000003&size=234653&file_family=standard&
       map_file=&pnfsid_file=0000464E839DEAFC428E8CF52D8028455141&pnfsid_map=&bfid=c085615502758c8bb54db4c30081626f&
       origdrive=localhost:/dev/tmp/tps0d0n:1487271284&crc=813392028&original_name=/pnfs/fs/usr/test/arossi/000/data001"
       > ".(suri)(data001)"

    $ cat ".(suri)(data001)"
    $ enstore://enstore/?volume=VOL001&location_cookie=0000_000000000_0000002&size=234653&file_family=standard&map_file=&
       pnfsid_file=0000464E839DEAFC428E8CF52D8028455141&pnfsid_map=&bfid=c085615502758c8bb54db4c30081626f&
       origdrive=localhost:/dev/tmp/tps0d0n:1487271284&crc=813392028&original_name=/pnfs/fs/usr/test/arossi/000/data001,
       enstore://enstore/?volume=VOL001&location_cookie=0000_000000000_0000003&size=234653&file_family=standard&map_file=&
       pnfsid_file=0000464E839DEAFC428E8CF52D8028455141&pnfsid_map=&bfid=c085615502758c8bb54db4c30081626f&
       origdrive=localhost:/dev/tmp/tps0d0n:1487271284&crc=813392028&original_name=/pnfs/fs/usr/test/arossi/000/data001

    ### remove [as root]

    $ echo -n "" > ".(suri)(data001)"

    $ cat ".(suri)(data001)"
    $

### NOTES:

We record here a peculiar problem concerning permission error reporting using bash built-in 'echo'.

Simple overwrite reports the error correctly:

    $ echo -n "" > ".(suri)(data001)"
    -bash: .(suri)(data001): Operation not permitted

Append, however, does not:

    $ echo "[...]" >> ".(suri)(data001)"
    $ echo $?
    0
    $ cat ".(suri)(data001)"
    [shows the original location]

Note that the behavior is correct (the new location value has not been overwritten), but the return value for the process is 0, and no error is reported.

The executable [/usr]/bin/echo, however, works as it should:

    $ /bin/echo  "[...]" >> ".(suri)(data001)"
    /bin/echo: write error: Operation not permitted

This is also works properly in python.

A similar issue arises with errors involving invalid URIs:  built-in echo does not report the error, but /bin/echo and python do.

***