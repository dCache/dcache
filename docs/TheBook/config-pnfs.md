PNFS
====

> **Important**
>
> This chapter is for existing installations. New installations should use
> CHIMERA
> and not PNFS.

This chapter gives background information about PNFS. PNFS is the filesystem, dCache used to be based on. Only the aspects of PNFS relevant to dCache will be explained here. A complete set of documentation is available from [the PNFS homepage].

> **Important**
>
> This chapter is about the namespace PNFS and not about the protocol NFS4-PNFS. Information about the protocol NFS4-PNFS is to be found in [???].

The Use of PNFS in dCache
=========================

dCache uses PNFS as a filesystem and for storing meta-data. PNFS is a filesystem not designed for storage of actual files. Instead, PNFS manages the filesystem hierarchy and standard meta-data of a UNIX filesystem. In addition, other applications (as for example dCache) can use it to store their meta-data. PNFS keeps the complete information in a database.

PNFS implements an NFS server. All the meta-data can be accessed with a standard NFS client, like the one in the Linux kernel. After mounting, normal filesystem operations work fine. However, IO operations on the actual files in the PNFS will normally result in an error.

As a minimum, the PNFS filesystem needs to be mounted only by the server running the dCache core services. In fact, the PNFS server has to run on the same system. For details see (has to be written).

The PNFS filesystem may also be mounted by clients. This should be done by

    PROMPT-ROOT mount -o intr,hard,rw pnfs-server:/pnfs /pnfs/site.de

(assuming the system is configured as described in the installation instructions). Users may then access the meta-data with regular filesystem operations, like `ls
      -l`, and by the PNFS-specific operations described in the following sections. The files themselves may then be accessed with the DCAP protocol (see [???][1] Client Access and Protocols).

Mounting the PNFS filesystem is not necessary for client access to the dCache system if URLs are used to refer to files. In the grid context this is the preferred usage.

Communicating with the PNFS Server
==================================

Many configuration parameters of PNFS and the application-specific meta-data is accessed by reading, writing, or creating files of the form `.(command)(para)`. For example, the following prints the pnfsID of the file `/pnfs/site.de/some/dir/file.dat`:

    PROMPT-USER cat /pnfs/site.de/any/sub/directory/'.(id)(file.dat)' 
    0004000000000000002320B8
    PROMPT-USER 

From the point of view of the NFS protocol, the file `.(id)(file.dat)` in the directory `/pnfs/site.de/some/dir/` is read. However, PNFS interprets it as the command `id` with the parameter `file.dat` executed in the directory `/pnfs/site.de/some/dir/`. The quotes are important, because the shell would otherwise try to interpret the parentheses.

Some of these command-files have a second parameter in a third pair of parentheses. Note, that files of the form `.(command)(para)` are not really files. They are not shown when listing directories with `ls`. However, the command-files are listed when they appear in the argument list of `ls` as in

    PROMPT-USER ls -l '.(tag)(sGroup)'
    -rw-r--r--   11 root     root            7 Aug  6  2004 .(tag)(sGroup)

Only a subset of file operations are allowed on these special command-files. Any other operation will result in an appropriate error. Beware, that files with names of this form might accidentally be created by typos. They will then be shown when listing the directory.

pnfsIDs
=======

Each file in PNFS has a unique 12 byte long pnfsID. This is comparable to the inode number in other filesystems. The pnfsID used for a file will never be reused, even if the file is deleted. dCache uses the pnfsID for all internal references to a file.

The pnfsID of the file `filename` can be obtained by reading the command-file `.(id)(filename)` in the directory of the file.

A file in PNFS can be referred to by pnfsID for most operations. For example, the name of a file can be obtained from the pnfsID with the command `nameof` as follows:

    PROMPT-USER cd /pnfs/site.de/any/sub/directory/
    PROMPT-USER cat '.(nameof)(0004000000000000002320B8)'
    file.dat

And the pnfsID of the directory it resides in is obtained by:

    PROMPT-USER cat '.(parent)(0004000000000000002320B8)'
    0004000000000000001DC9E8

This way, the complete path of a file may be obtained starting from the pnfsID. Precisely this is done by the tool `pathfinder`:

    PROMPT-USER . /usr/etc/pnfsSetup
    PROMPT-USER PATH=$PATH:$pnfs/tools
    PROMPT-USER cd /pnfs/site.de/another/dir/
    PROMPT-USER pathfinder 0004000000000000002320B8
    0004000000000000002320B8 file.dat
    0004000000000000001DC9E8 directory
    000400000000000000001060 sub
    000100000000000000001060 any
    000000000000000000001080 usr
    000000000000000000001040 fs
    000000000000000000001020 root
    000000000000000000001000 -
    000000000000000000000100 -
    000000000000000000000000 -
    /root/fs/usr/any/sub/directory/file.dat

The first two lines configure the PNFS-tools correctly. The path obtained by `pathfinder` does not agree with the local path, since the latter depends on the mountpoint (in the example `/pnfs/site.de/`). The pnfsID corresponding to the mountpoint may be obtained with

    PROMPT-USER cat '.(get)(cursor)'
    dirID=0004000000000000001DC9E8
    dirPerm=0000001400000020
    mountID=000000000000000000001080

The `dirID` is the pnfsID of the current directory and `mountID` that of the mountpoint. In the example, the PNFS server path `/root/fs/usr/` is mounted on `/pnfs/site.de/`.

Directory Tags
==============

In the PNFS filesystem, each directory has a number of tags. The existing tags may be listed with

    PROMPT-USER cat '.(tags)()'
    .(tag)(OSMTemplate)
    .(tag)(sGroup)

and the content of a tag can be read with

    PROMPT-USER cat '.(tag)(OSMTemplate)'
    StoreName myStore

A nice trick to list all tags with their contents is

    PROMPT-USER grep "" $(cat ".(tags)()")
    .(tag)(OSMTemplate):StoreName myStore
    .(tag)(sGroup):STRING

Directory tags may be used within dCache to control which pools are used for storing the files in the directory (see [???][2]). They might also be used by a tertiary storage system for similar purposes (e.g. controlling the set of tapes used for the files in the directory).

Even if the directory tags are not used to control the bahaviour of dCache, some tags have to be set for the directories where dCache files are stored. The installation procedure takes care of this: In the directory `/pnfs/site.de/data/` two tags are set to default values:

    PROMPT-USER cd /pnfs/site.de/data/
    PROMPT-USER grep "" $(cat ".(tags)()")
    .(tag)(OSMTemplate):StoreName myStore
    .(tag)(sGroup):STRING

The following directory tags appear in the dCache context:

OSMTemplate  
Contains one line of the form “`StoreName` storeName” and specifies the name of the store that is used by dCache to construct the storage class if the HSM type is `osm`.

hsmType  
The HSM type is normally determined from the other existing tags. E.g., if the tag `OSMTemplate` exists, HSM type `osm` is assumed. With this tag it can be set explicitly. An class implementing that HSM type has to exist. Currently the only implementations are `osm` and `enstore`.

sGroup  
The storage group is also used to construct the storage Class if the HSM type is `osm`.

cacheClass  
The cache class is only used to control on which pools the files in a directory may be stored, while the storage class (constructed from the two above tags) might also be used by the HSM. The cache class is only needed if the above two tags are already fixed by HSM usage and more flexibility is needed.

hsmInstance  
If not set, the HSM instance will be the same as the HSM type. Setting this tag will only change the name as used in the storage class and in the pool commands.

There are more tags used by dCache if the HSM type `enstore` is used.

When creating or changing directory tags by writing to the command-file as in

    PROMPT-USER echo 'content' > '.(tag)(tagName)'

one has to take care not to treat the command-files in the same way as regular files, because tags are different from files in the following aspects:

1.  The tagName is limited to 62 characters and the content to 512 bytes. Writing more to the command-file, will be silently ignored.

2.  If a tag which does not exist in a directory is created by writing to it, it is called a *primary* tag.

    Removing a primary tag invalidates this tag. An invalidated tag behaves as if it does not exist. All filesystem IO operations on that tag produce an “File not found” error. However, a lookup operation ( e.g. ls) will show this tag with a 0 byte size. An invalidated tag can be revalidated with the help of the tool `repairTag.sh` in the `tools/` directory of the PNFS distribution. It has to be called in the directory where the primary tag was with the tag name as argument.

3.  Tags are *inherited* from the parent directory by a newly created directory. Changing a primary tag in one directory will change the tags inherited from it in the same way, even if it is invalidated or revalidated. Creating a new primary tag in a directory will not create a inherited tag in its subdirectories.

    Moving a directory within the PNFS filesystem will not change the inheritance. Therefore, a directory does not necessarily inherit tags from its parent directory. Removing an inherited tag does not have any effect.

4.  Writing to an inherited tag in the subdirectory will break the inheritance-link. A *pseudo-primary* tag will be created. The directories which inherited the old (inherited) tag will inherit the pseudo-primary tag. A pseudo-primary tag behaves exactly like a primary tag, except that the original inherited tag will be restored if the pseude-primary tag is removed.

If directory tags are used to control the behaviour of dCache and/or a tertiary storage system, it is a good idea to plan the directory structure in advance, thereby considering the necessary tags and how they should be set up. Moving directories should be done with great care or even not at all. Inherited tags can only be created by creating a new directory.

Global Configuration with Wormholes
===================================

PNFS provides a way to distribute configuration information to all directories in the PNFS filesystem. It can be accessed in a subdirectory `.(config)()` of any PNFS-directory. It behaves similar to a hardlink. In the default configuration this link points to `/pnfs/fs/admin/etc/config/`. In it are three files: `'.(config)()'/serverId` contains the domain name of the site, `'.(config)()'/serverName` the fully qualified name of the PNFS server, and `'.(config)()'/serverRoot` should contain “`000000000000000000001080 .`”.

The dCache specific configuration can be found in `'.(config)()'/dCache/dcache.conf`. This file contains one line of the format `hostname:port` per DCAP door which may be used by DCAP clients when not using URLs. The `dccp` program will choose randomly between the doors listed here.

Normally, reading from files in PNFS is disabled. Therefore it is necessary to switch on I/O access to the files in `'.(config)()'/` by e.g.:

    PROMPT-ROOT touch '.(config)()/.(fset)(serverRoot)(io)(on)'

After that, you will notice that the file is empty. Therefore, take care, to rewrite the information.

Deleted Files in PNFS
=====================

When a file in the PNFS filesystem is deleted the server stores information about is in the subdirectories of `/opt/pnfsdb/pnfs/trash/`. For dCache, the `cleaner` cell in the `pnfsDomain` is responsible for deleting the actual files from the pools asyncronously. It uses the files in the directory `/opt/pnfsdb/pnfs/trash/2/`. It contains a file with the PNFS ID of the deleted file as name. If a pool containing that file is down at the time the cleaner tries to remove it, it will retry for a while. After that, the file `/opt/pnfsdb/pnfs/trash/2/current/failed.poolName` will contain the PNFS IDs which have not been removed from that pool. The cleaner will still retry the removal with a lower frequency.

Access Control
==============

The files `/pnfs/fs/admin/etc/exports/hostIP` and `/pnfs/fs/admin/etc/exports/netMask..netPart` are used to control the host-based access to the PNFS filesystem via mount points. They have to contain one line per NFS mount point. The lines are made of the following four space-separated fields fields:

-   Mount point for NFS (the part after the colon in e.g. `host:/mountpoint`)

-   The virtual PNFS path which is mounted

-   Permission: 0 means all permissions and 30 means disabled I/O.

-   Options (should always be nooptions)

In the initial configuration there is one file `/pnfs/fs/admin/etc/exports/0.0.0.0..0.0.0.0` containing

    /pnfs /0/root/fs/usr/ 30 nooptions

thereby allowing all hosts to mount the part of the PNFS filesystem containing the user data. There also is a file `/pnfs/fs/admin/etc/exports/127.0.0.1` containing

    /fs /0/root/fs 0 nooptions
    /admin /0/root/fs/admin 0 nooptions

The first line is the mountpoint used by the admin node. If the PNFS mount is not needed for client operations (e.g. in the grid context) and if no tertiary storage system (HSM) is connected, the file `/pnfs/fs/admin/etc/exports/0.0.0.0..0.0.0.0` may be deleted. With an HSM, the pools which write files into the HSM have to mount the PNFS filesystem and suitable export files have to be created.

In general, the user ID 0 of the `root` user on a client mounting the PNFS filesystem will be mapped to nobody (not to the user `nobody`). For the hosts whose IP addresses are the file names in the directory `/pnfs/fs/admin/etc/exports/trusted/` this is not the case. The files have to contain only the number `15`.

The Databases of PNFS
=====================

PNFS stores all the information in GNU dbm database files. Since each operation will lock the database file used globally and since GNU dbm cannot handle database files larger than 2GB, it is advisable to “split” them sutably to future usage. Each database stores the information of a sub-tree of the PNFS filesystem namespace. Which database is responsible for a directory and subsequent subdirectories is determined at creation time of the directory. The following procedure will create a new database and connect a new subdirectory to it.

Each database is handled by a separate server process. The maximum number of servers is set by the variable `shmservers` in file `/usr/etc/pnfsSetup`. Therefore, take care that this number is always higher than the number of databases that will be used (restart PNFS services, if changed).

Prepare the environment with

    PROMPT-ROOT . /usr/etc/pnfsSetup
    PROMPT-ROOT PATH=${pnfs}/tools:$PATH

To get a list of currently existing databases, issue

    PROMPT-ROOT mdb show
    ID Name Type Status Path
    -------------------------------
    0 admin r enabled (r) /opt/pnfsdb/pnfs/databases/admin
    1 data1 r enabled (r) /opt/pnfsdb/pnfs/databases/data1

Choose a new database name databaseName and a location for the database file databaseFilePath (just a placeholder for the PostgreSQL version of PNFS) and create it with

    PROMPT-ROOT mdb create databaseName databaseFilePath

e.g.

    PROMPT-ROOT mdb create data2 /opt/pnfsdb/pnfs/databases/data2

Make sure the file `databaseFilePath` exists with

    PROMPT-ROOT touch databaseFilePath

This might seem a little strange. The reason is that the PostgreSQL version of the PNFS server only uses the file as reference and stores the actual data in the PostgreSQL server.

In order to refresh database information run

    PROMPT-ROOT mdb update
    Starting data2

Running command `mdb show` shows the new database:

    PROMPT-ROOT mdb show
    ID Name Type Status Path
    -------------------------------
    0 admin r enabled (r) /opt/pnfsdb/pnfs/databases/admin
    1 data1 r enabled (r) /opt/pnfsdb/pnfs/databases/data1
    2 data2 r enabled (r) /opt/pnfsdb/pnfs/databases/data2

In the PNFS filesystem tree, create the new directory in the following way

    PROMPT-ROOT cd /pnfs/site.de/some/sub/dir/
    PROMPT-ROOT mkdir '.(newDbID)(newDirectory)'

where newDbID is the `ID` of the new database as listed in the output of `mdb show` and newDirectory is the name of the new directory. E.g.

    PROMPT-ROOT cd /pnfs/desy.de/data/zeus/
    PROMPT-ROOT mkdir '.(2)(mcdata)'

The new database does not know anything about the [wormhole] `'.(config)()'`, yet. For this, the PNFS ID of the wormhole directory (`/pnfs/fs/admin/etc/config/`) has to be specified. It can be found out with

    PROMPT-ROOT sclient getroot ${shmkey} 0
    0 000000000000000000001000 wormholePnfsId

The last pnfsID is the one of the wormhole directory of the database with ID 0 (already set correctly). Now you can set this ID with

    PROMPT-ROOT sclient getroot ${shmkey} newDbID wormholePnfsId
    newDbID 000000000000000000001000 wormholePnfsId

For example, do the following

    PROMPT-ROOT sclient getroot ${shmkey} 0
    0 000000000000000000001000 0000000000000000000010E0
    PROMPT-ROOT sclient getroot ${shmkey} 2 0000000000000000000010E0
    2 000000000000000000001000 0000000000000000000010E0

Finally, add directory tags for the new directories. The default tags are added by

    PROMPT-ROOT cd /pnfs/site.de/some/sub/dir/newDirectory
    PROMPT-ROOT echo 'StoreName myStore' > '.(tag)(OSMTemplate)'
    PROMPT-ROOT echo 'STRING' > '.(tag)(sGroup)'

  [the PNFS homepage]: http://www-pnfs.desy.de/
  [???]: #cf-nfs4
  [1]: #dCacheBook
  [2]: #cf-pm-psu
  [wormhole]: #cf-pnfs-wormholes
