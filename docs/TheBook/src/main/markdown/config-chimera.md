CHIMERA
=======

-----
[TOC bullet hierarchy]
-----

The inner dCache components talk to the namespace via a module called `PnfsManager`, which in turn communicates with the Chimera database using a thin Java layer. In addition to `PnfsManager` a direct access to the file system view is provided by an `NFSv3` and `NFSv4.1` server. Clients can `NFS`-mount the namespace locally. This offers the opportunity to use OS-level tools like `ls, mkdir, mv` for Chimera. Direct I/O-operations like `cp` and `cat` are possible with the `NFSv4.1 door`.

The properties of Chimera are defined in
`/usr/share/dcache/defaults/chimera.properties`. For customisation the
files `/etc/dcache/layouts/mylayout.conf` or `/etc/dcache/dcache.conf`
should be modified (see [the section called “Defining domains and
services”](install.md#defining-domains-and-services).

Example:

This example shows an extract of the
`/etc/dcache/layouts/mylayout.conf` file in order to run dCache with
`NFSv3`.

```ini
[namespaceDomain]
[namespaceDomain/pnfsmanager]
[namespaceDomain/nfs]
nfs.version=3
```

Example:

If you want to run the NFSv4.1 server you need to add the
corresponding nfs service to a domain in the
`/etc/dcache/layouts/mylayout.conf` file and start this domain.

```ini
[namespaceDomain]
[namespaceDomain/pnfsmanager]
[namespaceDomain/nfs]
nfs.version = 4.1
```

If you wish dCache to access your Chimera with a PostgreSQL user other
than chimera then you must specify the username and password in
`/etc/dcache/dcache.conf`.

```ini
chimera.db.user=myuser
chimera.db.password=secret
```

## Attribute consistency policy

On new filesystem object creation in a directory the `modification` and
`change id` attributes must be updated to provide a consistent, up-to-date view of the changes.
In highly concurrent environments such updates might create so-called `hot inodes`
and serialize all updates in a single directory, thus, reducing the namespace throughput. 

As such strong consistency is not always required, to improve concurrent updates to  a single directory
the POSIX constraints can be relaxed. The `chimera.attr-consistency` attribute controls the namespace attribute
update bahaviour of a parent directory on update:

| policy | behaviour                                                                                                                                                                                                                                   |
|:-------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| strong | a creation of a filesystem object will right away update parent directory's mtime, ctime, nlink and generation attributes                                                                                                                   |
| weak   | a creation of a filesystem object will eventually update (after 30 seconds) parent directory's mtime, ctime, nlink and generation attributes. Multiple concurrent modifications to a directory are aggregated into single attribute update. |
| soft   | same as weak, however, reading of directory attributes will take into account pending attribute updates.                                                                                                                                    |

Read-write exported NFS doors SHOULD run with `strong consistency` or `soft consistency` to maintain POSIX
compliance. Read-only NFS doors might run with `weak consistency` if non-up-to-date directory attributes can
be tolerated, for example when accessing existing data, or  `soft consistency`, if up-to-date information
is desired, typically when seeking for newly arrived files through other doors.

```
chimera.attr-consistency=strong
```

## Mounting Chimera through NFS

dCache does not need the Chimera filesystem to be mounted, but a mounted file system is convenient for administrative access. This offers the opportunity to use OS-level tools like `ls` and `mkdir` for Chimera. However, direct I/O-operations like `cp` are not possible, since the `NFSv3` interface provides the namespace part only. This section describes how to start the Chimera `NFSv3` server and mount the name space.

If you want to mount Chimera for easier administrative access, you
need to edit the `/etc/exports` file as the Chimera `NFS` server uses
it to manage exports. If this file doesn’t exist it must be
created. The typical exports file looks like this:

    / localhost(rw)
    /data
    # or
    # /data *.my.domain(rw)

As any RPC service Chimera `NFS` requires `rpcbind` service to run on the host. Nevertheless rpcbind has to be configured to accept requests from Chimera NFS.

On RHEL6 based systems you need to add

    RPCBIND_ARGS="-i"

into `/etc/sysconfig/rpcbind` and restart `rpcbind`. Check your OS
manual for details.

```console-root
service rpcbind restart
|Stopping rpcbind:                     [  OK  ]
|Starting rpcbind:                     [  OK  ]
```

If your OS does not provide `rpcbind` Chimera `NFS` can use an embedded `rpcbind`. This requires to disable the `portmap` service if it exists.

```console-root
/etc/init.d/portmap stop
|Stopping portmap: portmap
```

and restart the domain in which the `NFS` server is running.

```console-root
dcache restart namespaceDomain
```

Now you can mount Chimera by

```console-root
mount localhost:/ /mnt
```

and create the root of the CHIMERA namespace which you can call **data**:

```console-root
mkdir -p /mnt/data
```

If you don’t want to mount chimera you can create the root of the Chimera namespace by

```console-root
chimera mkdir /data
```

You can now add directory tags. For more information on tags see [the section called “Directory Tags”](config-chimera.md#directory-tags).

```console-root
chimera writetag /data sGroup "chimera"
chimera writetag /data OSMTemplate "StoreName sql"
```


### Using DCAP with a mounted file system

If you plan to use `dCap` with a mounted file system instead of the
URL-syntax (e.g. `dccp /data/file1 /tmp/file1`), you need to mount
the root of Chimera locally (remote mounts are not allowed yet). This
will allow us to establish wormhole files so `dCap` clients can
discover the `dCap` doors.

```console-root
mount localhost:/ /mnt
mkdir /mnt/admin/etc/config/dCache
touch /mnt/admin/etc/config/dCache/dcache.conf
touch /mnt/admin/etc/config/dCache/'.(fset)(dcache.conf)(io)(on)'
echo "<door host>:<port>" > /mnt/admin/etc/config/dCache/dcache.conf
```

The default values for ports can be found in [Chapter 29, dCache
Default Port Values](rf-ports.md) (for `dCap` the default port is
22125) and in the file
`/usr/share/dcache/defaults/dcache.properties`. They can be altered in
`/etc/dcache/dcache.conf`

Create the directory in which the users are going to store their data and change to this directory.

```console-root
mkdir -p /mnt/data
cd /mnt/data
```

Now you can copy a file into your dCache

```console-root
dccp /bin/sh test-file
|735004 bytes (718 kiB) in 0 seconds
```

and copy the data back using the `dccp` command.

```console-root
dccp test-file /tmp/testfile
|735004 bytes (718 kiB) in 0 seconds
```

The file has been transferred succesfully.

Now remove the file from the dCache.

```console-root
rm  test-file
```

When the configuration is complete you can unmount Chimera:

```console-root
umount /mnt
```

> **NOTE**
>
> Please note that whenever you need to change the configuration, you
> have to remount the root `localhost:/` to a temporary location like
> `/mnt`.

## Communicating with Chimera

Many configuration parameters of Chimera and the application specific
meta data is accessed by reading, writing, or creating files of the
form `.(command)(para)`. For example, the following prints the
ChimeraID of the file `/data/some/dir/file.dat`:

```console-user
cat /data/any/sub/directory/'.(id)(file.dat)'
|0004000000000000002320B8
```

From the point of view of the `NFS` protocol, the file
**.(id)(file.dat)** in the directory `/data/some/dir/` is
read. However, Chimera interprets it as the command id with the
parameter file.dat executed in the directory `/data/some/dir/`. The
quotes are important, because the shell would otherwise try to
interpret the parentheses.

Some of these command files have a second parameter in a third pair of
parentheses. Note, that files of the form `.(command)(para)` are not
really files. They are not shown when listing directories with
`ls`. However, the command files are listed when they appear in the
argument list of `ls` as in

```console-user
ls -l '.(tag)(sGroup)'
|-rw-r--r-- 11 root root 7 Aug 6 2010 .(tag)(sGroup)
```

Only a subset of file operations are allowed on these special command files. Any other operation will result in an appropriate error. Beware, that files with names of this form might accidentally be created by typos. They will then be shown when listing the directory.

## IDs

Each file in Chimera has a unique 18 byte long ID. It is referred to as ChimeraID or as pnfsID. This is comparable to the inode number in other filesystems. The ID used for a file will never be reused, even if the file is deleted. dCache uses the ID for all internal references to a file.

Example:

The ID of the file `/example.org/data/examplefile` can be obtained by
reading the command-file `.(id)(examplefile)` in the directory of the
file.

```console-user
cat /example.org/data/'.(id)(examplefile)'
|0000917F4A82369F4BA98E38DBC5687A031D
```

A file in Chimera can be referred to by the ID for most operations.

Example:

The name of a file can be obtained from the ID with the command `nameof` as follows:

```console-user
cd /example.org/data/
cat '.(nameof)(0000917F4A82369F4BA98E38DBC5687A031D)'
|examplefile
```

And the ID of the directory it resides in is obtained by:

```console-user
cat '.(parent)(0000917F4A82369F4BA98E38DBC5687A031D)'
|0000595ABA40B31A469C87754CD79E0C08F2
```

This way, the complete path of a file may be obtained starting from the ID.

## Directory tags

In the Chimera namespace, each directory can have a number of tags. These directory tags may be used within dCache to control the file placement policy in the pools (see [the section called “The Pool Selection Mechanism”](config-PoolManager.md#the-pool-selection-mechanism)). They might also be used by a [tertiary storage system](config-hsm.md) for similar purposes (e.g. controlling the set of tapes used for the files in the directory).

> **NOTE**
>
> Directory tags are not needed to control the behaviour of dCache. dCache works well without directory tags.

### Create, list and read directory tags if the namespace is not mounted

You can create tags with

```console-root
chimera writetag <directory> <tagName> "<content>"
```

list tags with

```console-root
chimera lstag <directory>
```

and read tags with

```console-root
chimera readtag <directory> <tagName>
```

Example:
Create tags for the directory `data` with

```console-root
chimera writetag /data sGroup "myGroup"
chimera writetag /data OSMTemplate "StoreName myStore"
```

list the existing tags with

```console-root
chimera lstag /data
|Total: 2
|OSMTemplate
|sGroup
```

and their content with

```console-root
chimera readtag /data OSMTemplate
|StoreName myStore
chimera readtag /data sGroup
|myGroup
```

### Create, list and read directory tags if the namespace is mounted

If the namespace is mounted, change to the directory for which the tag
should be set and create a tag with

```console-user
cd <directory>
echo '<content1>' > '.(tag)(<tagName1>)'
echo '<content2>' > '.(tag)(<tagName2>)'
```


Then the existing tags may be listed with

```console-user
cat '.(tags)()'
|.(tag)(<tagname1>)
|.(tag)(<tagname2>)
```

and the content of a tag can be read with

```console-user
cat '.(tag)(<tagname1>)'
|<content1>
cat '.(tag)(<tagName2>)'
|<content2>
```

In the following example, two tags are created, listed and their
contents shown.

First, create tags for the directory `data` with

```console-user
cd data
echo 'StoreName myStore' > '.(tag)(OSMTemplate)'
echo 'myGroup' > '.(tag)(sGroup)'
```

list the existing tags with

```console-user
cat '.(tags)()'
|.(tag)(OSMTemplate)
|.(tag)(sGroup)
```

and their content with

```console-user
cat '.(tag)(OSMTemplate)'
|StoreName myStore
cat '.(tag)(sGroup)'
|myGroup
```

A nice trick to list all tags with their contents is

```console-user
grep "" $(cat  ".(tags)()")
|.(tag)(OSMTemplate):StoreName myStore
|.(tag)(sGroup):myGroup
```

### Directory tags and command files

When creating or changing directory tags by writing to the command
file as in

```console-user
echo '<content>' > '.(tag)(<tagName>)'
```

one has to take care not to treat the command files in the same way as
regular files, because tags are different from files in the following
aspects:

1. The `tagName` is limited to 62 characters and the `content` to 512 bytes. Writing more to the command file, will be silently ignored.

2. If a tag which does not exist in a directory is created by writing to it, it is called a *primary* tag.

3. Tags are *inherited* from the parent directory by a newly created subdirectory. Changing a primary tag in one directory will change the tags inherited from it in the same way. Creating a new primary tag in a directory will not create an inherited tag in its subdirectories.

    Moving a directory within the CHIMERA namespace will not change the inheritance. Therefore, a directory does not necessarily inherit tags from its parent directory. Removing an inherited tag does not have any effect.

4. Empty tags are ignored.

### Directory tags for dCache

The following directory tags appear in the dCache context:

*OSMTemplate*:

Must contain a line of the form `StoreName <storeName>` and specifies
the name of the store that is used by dCache to construct the [storage
class](#storage-class-and-directory-tags) if the [HSM
Type](rf-glossary.md#hsm-type) is `osm`.

*HSMType*:

The [`HSMType`](rf-glossary.md#hsm-type) tag is normally determined
from the other existing tags. E.g., if the tag `OSMTemplate` exists,
`HSMType`=`osm` is assumed. With this tag it can be set explicitly. A
class implementing that HSM type has to exist. Currently the only
implementations are `osm` and `enstore`.

*sGroup*:

The storage group is also used to construct the [storage
class](#storage-class-and-directory-tags) if the
[`HSMType`](rf-glossary.md#hsm-type) is `osm`.

*cacheClass*:

The cache class is only used to control on which pools the files in a
directory may be stored, while the storage class (constructed from the
two above tags) might also be used by the HSM. The cache class is only
needed if the above two tags are already fixed by HSM usage and more
flexibility is needed.

*hsmInstance*:

If not set, the `hsmInstance` tag will be the same as the `HSMType`
tag. Setting this tag will only change the name as used in the
[storage class](#storage-class-and-directory-tags) and in the pool
commands.

*WriteToken*:

Assign a `WriteToken` tag to a directory in order to be able to write
to a space token without using the SRM.

### Storage class and directory tags

The [storage class](config-PoolManager.md#storage-classes) is a string of the form `StoreName`:`StorageGroup`@`hsm-type`, where `StoreName`is given by the OSMTemplate tag, `StorageGroup` by the sGroup tag and `hsm-type` by the HSMType tag. As mentioned above the HSMType tag is assumed to be osm if the tag OSMTemplate exists.

In the examples above two tags have been created.

Example:

```console-root
chimera lstag /data
|Total: 2
|OSMTemplate
|sGroup
```

As the tag OSMTemplate was created the tag HSMType is assumed to be
osm.  The storage class of the files which are copied into the
directory `/data` after the tags have been set will be
`myStore:myGroup@osm`.

If directory tags are used to control the behaviour of dCache and/or a tertiary storage system, it is a good idea to plan the directory structure in advance, thereby considering the necessary tags and how they should be set up. Moving directories should be done with great care or even not at all. Inherited tags can only be created by creating a new directory.

Example:

Assume that data of two experiments, experiment-a and experiment-b is
written into a namespace tree with subdirectories `/data/experiment-a`
and `/data/experiment-b`. As some pools of the dCache are financed by
experiment-a and others by experiment-b they probably do not like it
if they are also used by the other group. To avoid this the
directories of experiment-a and experiment-b can be tagged.

```console-root
chimera writetag /data/experiment-a OSMTemplate "StoreName exp-a"
chimera writetag /data/experiment-b OSMTemplate "StoreName exp-b"
```

Data from experiment-a taken in 2010 shall be written into the
directory `/data/experiment-a/2010` and data from experiment-a taken
in 2011 shall be written into `/data/experiment-a/2011`. Data from
experiment-b shall be written into `/data/experiment-b`. Tag the
directories correspondingly.

```console-root
chimera writetag /data/experiment-a/2010 sGroup "run2010"
chimera writetag /data/experiment-a/2011 sGroup "run2011"
chimera writetag /data/experiment-b sGroup "alldata"
```

List the content of the tags by

```console-root
chimera readtag /data/experiment-a/2010 OSMTemplate
|StoreName exp-a
chimera readtag /data/experiment-a/2010 sGroup
|run2010
chimera readtag /data/experiment-a/2011 OSMTemplate
|StoreName exp-a
chimera readtag /data/experiment-a/2011 sGroup
|run2011
chimera readtag /data/experiment-b/2011 OSMTemplate
|StoreName exp-b
chimera readtag /data/experiment-b/2011 sGroup
|alldata
```

As the tag OSMTemplate was created the HSMType is assumed to be osm.
The storage classes of the files which are copied into these directories after the tags have been set will be

- `exp-a:run2010@osm` for the files in `/data/experiment-a/2010`
- `exp-a:run2011@osm` for the files in `/data/experiment-a/2011`
- `exp-b:alldata@osm` for the files in `/data/experiment-b`

To see how storage classes are used for pool selection have a look at
the example ’Reserving Pools for Storage and Cache Classes’ in the
PoolManager chapter.

There are more tags used by dCache if the `HSMType` is `enstore`.

<!--
  [???]: #in-install-layout
  [section\_title]: #chimera-tags
  [1]: #rf-ports
  [2]: #cf-pm-psu
  [tertiary storage system]: #cf-tss
  [storage class]: #chimera-tags-storageClass
  [3]: #secStorageClass
--!>
