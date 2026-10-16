Exporting dCache over S3 with VersityGW
=======================================

This chapter describes how to give S3 clients access to dCache through
[VersityGW](https://github.com/versity/versitygw), an S3 gateway that runs on
a separate host and stores objects in a POSIX file system, here an NFS mount
of dCache.

-----
[TOC bullet hierarchy]
-----

## When to use VersityGW

dCache has its own S3 door, which runs inside dCache: every request is
authenticated by gPlazma as a dCache user, data is moved by dCache's
transfer machinery, billing records the user, and files that are only on tape
behave like S3 Glacier objects.  It is the better choice in most cases.

VersityGW is an alternative when S3 features are needed that the S3 door does
not offer, such as bucket versioning, object lock, ACLs and bucket policies,
browser uploads or bucket notifications.  The price is that dCache no longer
sees the S3 users:

| | S3 door | VersityGW over NFS |
|---|---|---|
| Users | dCache users (gPlazma) | VersityGW accounts, mapped to one or more uids |
| Permissions | dCache's (POSIX, ACLs) | VersityGW's (ACLs, policies) |
| Billing | per S3 user | per NFS identity of the gateway |
| Files on tape | GLACIER, RestoreObject | GET blocks until the file is staged |
| Data path | through the door | through the gateway host, which reads and writes the pools directly (pNFS) |
| Versioning, object lock, policies | no | yes |

## How it works

The gateway host mounts a dCache directory over NFS.  VersityGW's `posix`
backend serves that directory: each subdirectory is a bucket, and an object
`a/b/c` in bucket `data` is the file `data/a/b/c`.  Object metadata (ETag,
content type, user metadata, ACLs) is kept in extended attributes, which NFS
supports from version 4.2 on.  New objects are written to a temporary file in
the bucket's hidden directory `.sgwtmp` and renamed into place; multipart
uploads keep their parts there too.

With pNFS, the NFS client on the gateway host reads and writes file content
directly from and to the pools, so the NFS door only handles the namespace.

## Preparing dCache

The NFS door must support NFSv4.1; it then also accepts NFSv4.2 clients,
which are needed for extended attributes:

```ini
[nfsDomain/nfs]
nfs.version = 4.1
```

Create the directory the gateway serves, for example `/data/s3`, and export it
to the gateway host only.  How VersityGW should own files decides the export
options:

- With one service account, all objects are owned by that account.  Squash
  all requests of the gateway host to it:

  ```
  /data/s3 s3gw.example.org(rw,all_squash,anonuid=5000,anongid=5000)
  ```

- With the files of each VersityGW account owned by a uid of its own
  (`--chuid`, `--chgid` below), VersityGW runs as root and changes the owner
  of new files, so root must not be squashed:

  ```
  /data/s3 s3gw.example.org(rw,no_root_squash)
  ```

  dCache then sees the gateway as root and does not restrict what VersityGW
  accounts may do: access control is VersityGW's alone.

Existing directories below `/data/s3` become buckets as they are, so data
already in dCache can be shared over S3.

## Setting up the gateway

Mount the directory with NFSv4.2 (Linux 5.9 or later supports extended
attributes over NFS) and check that extended attributes work:

```console-root
mkdir -p /mnt/dcache-s3
mount -t nfs -o vers=4.2,proto=tcp,hard dcache-nfs.example.org:/data/s3 /mnt/dcache-s3
touch /mnt/dcache-s3/.xattr-test
setfattr -n user.test -v ok /mnt/dcache-s3/.xattr-test
getfattr -n user.test /mnt/dcache-s3/.xattr-test
rm /mnt/dcache-s3/.xattr-test
```

If the mount should be permanent, add it to `/etc/fstab`.

Install VersityGW from the packages of its
[releases](https://github.com/versity/versitygw/releases).  They provide the
systemd unit `versitygw@.service`, which reads its settings from
`/etc/versitygw.d/<name>.conf` and runs as root.  Create
`/etc/versitygw.d/dcache.conf`:

```ini
VGW_BACKEND=posix
VGW_BACKEND_ARG=/mnt/dcache-s3

# The account that administers the gateway.
ROOT_ACCESS_KEY_ID=<root access key>
ROOT_SECRET_ACCESS_KEY=<root secret>

VGW_PORT=:7070
VGW_CERT=/etc/grid-security/hostcert.pem
VGW_KEY=/etc/grid-security/hostkey.pem
VGW_ADMIN_PORT=127.0.0.1:7071
VGW_IAM_DIR=/var/lib/versitygw/iam
VGW_ACCESS_LOG=/var/log/versitygw/access.log

# NFS has no O_TMPFILE: write new objects to temporary files in
# <bucket>/.sgwtmp right away instead of trying O_TMPFILE first.
VGW_DISABLE_OTMP=true
# Copy multipart parts explicitly; copy_file_range may hang with NFS servers.
VGW_DISABLE_COPY_FILE_RANGE=true
# Lock conditional writes within the gateway process.  This protects them
# only if a single gateway serves the directory.
VGW_OBJECT_LOCK_MODE=local

# Own new files by the uid and gid of the VersityGW account
# (needs no_root_squash, see above).
#VGW_CHOWN_UID=true
#VGW_CHOWN_GID=true
```

Then start the gateway:

```console-root
mkdir -p /var/lib/versitygw/iam /var/log/versitygw
systemctl enable --now versitygw@dcache
```

The settings correspond to `versitygw` command-line options; for a test, the
same gateway can be started by hand:

```console-root
ROOT_ACCESS_KEY=<root access key> ROOT_SECRET_KEY=<root secret> \
versitygw --port :7070 --admin-port 127.0.0.1:7071 --iam-dir /var/lib/versitygw/iam \
    posix --disableotmp --disable-copy-file-range --object-lock-mode local /mnt/dcache-s3
```

## Accounts and buckets

Accounts are managed through the admin port.  The role `userplus` may create
buckets; the role `user` may only use buckets it owns or is granted access to.
The user and group ids are those VersityGW gives new files with
`VGW_CHOWN_UID` and `VGW_CHOWN_GID`; choose the uids and gids of the
corresponding dCache users:

```console-root
versitygw admin --access <root access key> --secret <root secret> \
    --endpoint-url http://127.0.0.1:7071 \
    create-user --access alice --secret <secret> --role userplus --user-id 1000 --group-id 1000
```

Directories that already exist in dCache are listed as buckets without an
owner.  Give each one an owner before it is used:

```console-root
versitygw admin --access <root access key> --secret <root secret> \
    --endpoint-url http://127.0.0.1:7071 \
    change-bucket-owner --bucket experiment-data --owner alice
versitygw admin --access <root access key> --secret <root secret> \
    --endpoint-url http://127.0.0.1:7071 list-buckets
```

Instead of the internal account store (`VGW_IAM_DIR`), VersityGW can keep its
accounts in LDAP, Vault or FreeIPA; see its
[documentation](https://github.com/versity/versitygw/wiki).

## Things to know

- **ETags of existing files.** Files that were not written through VersityGW
  have no ETag: HEAD answers without one, and listings show an empty ETag.
  Clients that compare ETags may complain; `VGW_DEFAULT_ETAG` sets a value
  for such files.

- **Metadata.** VersityGW stores metadata in the extended attributes `etag`,
  `content-type`, `metadata`, `checksums`, `acl` and `ownership` (in the
  `user.` namespace on the gateway host), which are visible through other
  dCache doors too.  If NFSv4.2 cannot be used, `VGW_META_SIDECAR` keeps the
  metadata in files below a separate directory instead.

- **Multipart uploads** keep their parts as ordinary dCache files below
  `<bucket>/.sgwtmp/multipart`.  Completing an upload copies the parts into
  the object through the gateway host, so the data is written to the pools
  twice.  In tape-backed directories the parts may be written to tape before
  the upload completes.

- **Files on tape.** VersityGW knows nothing about tape: reading a file whose
  only copy is on tape blocks until dCache has staged it, which may take
  longer than the S3 client waits.  Stage such files in advance, for example
  with the bulk service, or serve tape-backed data with the S3 door instead.

- **Accounting.** dCache bills the NFS transfers of the gateway host as the
  uid VersityGW uses (root or the service account), not as the S3 user.
  VersityGW's access log (`VGW_ACCESS_LOG`) records the S3 accounts.

- **Quotas** apply to the owner of the files, so per-account quotas need
  `VGW_CHOWN_UID` and `VGW_CHOWN_GID`.

- **Storage tags.** Objects are renamed from `.sgwtmp` into their directory.
  If `pnfsmanager.enable.move-to-directory-with-different-storageclass` is
  set to `false`, such renames fail for directories whose storage tags differ
  from the bucket's.

- **Bucket names.** Directories whose names are not valid S3 bucket names,
  such as names with upper-case letters or underscores, are not listed as
  buckets.  `--disable-strict-bucket-names` (`VGW_DISABLE_STRICT_BUCKET_NAMES`)
  relaxes the check.
