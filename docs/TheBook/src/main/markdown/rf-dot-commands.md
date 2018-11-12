Specialized NFS Dot Commands
============================

### The following commands provide information concerning a file's locality, checksums and storage location.

#### GET LOCALITY

Returns the media types on which the file is currently stored.

##### USAGE:

    cat ".(get)(<filename>)(locality)"

##### RETURNS:

* ``ONLINE``:                  stored on disk
* ``NEARLINE``:                requires staging or data replication before open succeeds
* ``ONLINE_AND_NEARLINE``:     stored both on disk and tape
* ``UNAVAILABLE``:             not stored on any media (i.e., lost)

##### EXAMPLE:

    $ cat ".(get)(test_file-Thu_Oct_23_10:39:37_CDT_2014-109)(locality)"
    $ ONLINE_AND_NEARLINE

##### NOTES:

Currently, the NFS client sends 'localhost' to the poolmanager as its hostname (the protocol match defaults to '*/*').  SRM does something similar. In the future this may be modified to send the actual IP address of the client.

There are no guarantees concerning future availability of the file; in particular, ONLINE_AND_NEARLINE may revert back to NEARLINE at any time unless the file is pinned.

***

#### GET CHECKSUM(S)

Get checksum types and checksums for a given file. 

##### USAGE:

    cat ".(get)(<filename>)(checksum[s])"

##### RETURNS:

A comma-delimited list of `type:value` pairs for all checksums stored in the database.  The valid stored checksum types are `ADLER32`, `MD4` and `MD5`.

##### EXAMPLE:

    $ cat ".(get)(test_file-Thu_Oct_23_10:39:37_CDT_2014-109)(checksums)"
    $ ADLER32:66300001

***

#### PIN/STAGE

Allows users to pin or stage files.

##### USAGE:

    touch ".(fset)(<filename>)(<operation>)(<duration>)(<unit>)"

##### OPTIONS:

``operation`` can be replaced by:

* ``pin``
* ``stage`` 
* ``bringonline``

These are equivalent options.

``duration`` must be 0 or a positive integer; 0 will unpin the file.

``unit`` can be replaced by:

* ``SECONDS`` 
* ``MINUTES`` 
* ``HOURS`` 
* ``DAYS``

This argument is optional and defaults to ``SECONDS``.

##### EXAMPLES:

Pin file for one minute:

    $ touch ".(fset)(test_file-Thu_Oct_23_10:39:37_CDT_2014-109)(pin)(60)"

Pin file for two minutes:

    $ touch ".(fset)(test_file-Thu_Oct_23_10:39:32_CDT_2014-418)(pin)(2)(MINUTES)"

Remove pins on the file:

    $ touch ".(fset)(test_file-Thu_Oct_23_10:39:32_CDT_2014-418)(pin)(0)"

For further explanation of pinning, see [Pinning Files to a Pool](https://www.dcache.org/manuals/Book-2.16/Book-fhs.shtml#cb-pool-pin).

***

#### GET/SET TAPE LOCATION URI

Stores, retrieves and modifies the URI string(s) which define an HSM/tape location.  A normal user can read and write once to this file.  Root can overwrite and append as well.

##### USAGE:

    cat ".(suri)(<file name>)"
    echo [...] > ".(suri)(<file name>)"
    echo [...] >> ".(suri)(<file name>)"

##### RETURNS:

    cat will return a list of locations (there may be multiple ones).

##### EXAMPLE:

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
    
##### NOTES:

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