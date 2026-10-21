The S3 Service
==============

The `s3` service is a door that lets clients of the Amazon S3 protocol (the
AWS CLI and SDKs, rclone, s3cmd and others) read and write dCache.

-----
[TOC bullet hierarchy]
-----

## Introduction

S3 clients address data as objects in buckets.  The S3 door maps buckets to
directories and objects to files, so that the same data is available over S3
and over every other door.  S3 clients authenticate with an access key and a
secret key; the door verifies their requests and asks gPlazma which dCache
user an access key belongs to.  From then on the request is handled like any
other: dCache's permissions apply, data is read from and written to pools, and
billing records the user.

The door supports:

- listing buckets and objects, with and without the delimiter `/`;
- reading objects, including ranges and conditional requests;
- writing, copying and deleting objects, including multipart uploads;
- creating and deleting buckets;
- object metadata: content type, the other standard content headers, and user
  metadata;
- AWS signatures version 4 and, optionally, version 2, also in presigned URLs;
- files on tape, which behave like objects in the S3 Glacier storage class.

What it does not support is listed under [Limitations](#limitations).

The S3 protocol layer of the door is based on
[S3Proxy](https://github.com/gaul/s3proxy).

## Setting up

### Running the door

Add the door to a domain of the layout file:

```ini
[s3Domain]
[s3Domain/s3]
```

By default the door listens on port 9000 with HTTPS, using the host
certificate (`/etc/grid-security/hostcert.pem` and `hostkey.pem`) and the CA
certificates in `/etc/grid-security/certificates`.  Client certificates are
not used.  For tests, plain HTTP can be enabled with

```ini
s3.authn.protocol = http
```

but then data travels unencrypted.  S3 clients sign their requests with the
secret key, which itself is never sent, so the keys are protected either way.

### Access keys

The door looks up the secret key of each request's access key in the file
`/etc/dcache/s3-keys.conf` (property `s3.authn.keystore.file`), which holds one
pair per line:

```
# access key            secret key
AKIAEXAMPLE0001         wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

Empty lines and lines starting with `#` are ignored.  The file holds secrets in
plain text, so it should be readable only by the user running dCache:

```console-root
chmod 600 /etc/dcache/s3-keys.conf
```

The door rereads the file when it changes; a removed or changed key takes
effect with the next request.  If any line of the file is invalid, the door
accepts no key at all until the file is fixed.  Access keys must be unique and
should be random, like the secret keys; for example:

```console-user
echo AKIA$(openssl rand -hex 8 | tr a-f A-F) $(openssl rand -base64 30)
```

Each door host needs the file.

### Mapping access keys to users

After verifying a request, the door logs in the access key with gPlazma as a
principal of type `s3`.  No `auth` plugin is involved; a `map` plugin maps the
access key to a user, and the `session` phase must give the user a root and
home directory, as for any other door.  With the `multimap` plugin:

```
map      sufficient  multimap
session  sufficient  omnisession
```

`/etc/dcache/multi-mapfile`:

```
s3:AKIAEXAMPLE0001    username:alice uid:1000 gid:1000,true
```

`/etc/dcache/omnisession.conf`:

```
username:alice root:/ home:/
```

The door caches the results of gPlazma for a minute
(`s3.service.gplazma.cache.timeout`).  After changing the gPlazma
configuration, wait that long or clear the cache with the admin command

```
\s S3-<hostname> login clear cache
```

Failed logins are cached too.

## Buckets and objects

### Where the buckets are

Buckets are the directories below the door's root directory, `s3.root`
(default `/`).  If a user's root directory, as assigned by gPlazma, is below
`s3.root`, the user's buckets are the directories below the user's root
instead.  Users whose root is outside `s3.root` cannot use the door.

An object key is a path below the bucket: the object `run1/data/file.dat` in
bucket `experiment` of a door with root `/data` is the file
`/data/experiment/run1/data/file.dat`.  Writing an object creates missing
directories; deleting it keeps them.

As a consequence, some keys that S3 allows have no place in dCache, and
writing them fails with `InvalidArgument`:

- keys with empty segments (`a//b`), a trailing `/` or the segments `.` or `..`;
- a key below another object (`a/b` when `a` is an object), and an object at
  a key that is a prefix of others (`a` when `a/b` exists).

Directories whose names start with `.` are not buckets.  The door keeps
uploads in progress in files named `.s3-tmp-*` and in a directory
`.s3-multipart` in each bucket; listings do not show them.

Creating a bucket creates its directory, so the user needs permission to
create directories in the bucket root.  Deleting a bucket removes its
directory and any empty directories below it; it fails with `BucketNotEmpty`
as long as the bucket holds a file.

### Permissions

The door does not implement S3 access control lists and bucket policies: what
a user may do is decided by dCache's permissions (POSIX permissions and, if
enabled, ACLs), as for every other door.  In particular, objects are readable
by other users if the file's permissions allow it, unlike S3's default of
private objects.  Requests to set ACLs other than `private` fail with
`NotImplemented`.

### ETags and metadata

The ETag of an object is its MD5 checksum.  The door computes it while
writing an object and stores it as the file's checksum.  Files written through
other doors have an MD5 ETag only if dCache has an MD5 checksum for them;
otherwise their ETag is an opaque value that changes with the file and ends
with `-1`, which tells clients not to compare it with an MD5 checksum.  The
ETag of an object uploaded in parts is its MD5 checksum too, not the checksum
of the parts' checksums that S3 uses.

Content type, `Cache-Control`, `Content-Disposition`, `Content-Encoding`,
`Content-Language`, `Expires` and user metadata (`x-amz-meta-*`) are stored as
extended attributes of the file, named `s3.content-type`, `s3.cache-control`,
… and `s3.meta.<name>`.

## Writing objects

An object is written to a hidden file in its directory, which replaces the
object once all data has arrived and its checksum is recorded.  Until then,
readers see the previous version of the object.  If the upload fails, the
hidden file is removed.

Clients upload large objects in parts.  The door keeps the parts of an upload
in progress as files in the bucket's directory `.s3-multipart`.  Completing
the upload copies the parts into the object through the door, after which the
parts are removed; until then the data needs twice its size on the pools.  In
directories whose files go to tape, the parts are flushed like any other file.

Uploads that are neither completed nor aborted are removed after
`s3.limits.multipart.max-age` (default 7 days): every hour from the buckets
directly below `s3.root`, and from any bucket when an upload is started in it.
The admin commands `s3 uploads ls`, `s3 uploads rm` and `s3 uploads sweep`
list, remove and sweep the uploads of a bucket:

```
\s S3-<hostname> s3 uploads ls /data/experiment
```

Hidden files of uploads that the door could not finish, for example because
it was restarted, are removed when their bucket is deleted, once they are
older than the same age.

## Files on tape

Files whose only copy is on tape, without a replica on any pool, are listed
with the storage class `GLACIER`.  Reading them fails with `InvalidObjectState`
until they are restored with the S3 operation RestoreObject:

```console-user
aws --endpoint-url https://dcache.example.org:9000 s3api restore-object \
    --bucket experiment --key run1/data/file.dat --restore-request Days=2
```

A restore pins the file on disk with the PinManager for the number of days
requested, at most `s3.restore.max-days` (default 30).  While the file is
being staged, HEAD answers with `x-amz-restore: ongoing-request="true"`; once
it is on disk, it is listed as `STANDARD` and can be read.  Restoring a file
that has a copy on disk succeeds without pinning it.

## Clients

Most clients need to be told to put the bucket into the path of their
requests (`https://dcache.example.org:9000/bucket/key`), unless the door is
configured for [virtual-host style](#virtual-host-style-requests) requests.

The AWS CLI:

```console-user
aws configure set default.s3.addressing_style path
aws --endpoint-url https://dcache.example.org:9000 s3 ls s3://experiment/
aws --endpoint-url https://dcache.example.org:9000 s3 cp file.dat s3://experiment/run1/
```

rclone:

```console-user
rclone --s3-provider Other --s3-endpoint https://dcache.example.org:9000 \
       --s3-access-key-id AKIAEXAMPLE0001 --s3-secret-access-key <secret> \
       copy ./run1 :s3:experiment/run1
```

s3cmd:

```console-user
s3cmd --host dcache.example.org:9000 --host-bucket dcache.example.org:9000 \
      --access_key AKIAEXAMPLE0001 --secret_key <secret> ls s3://experiment
```

Clients that do not trust the CA of the host certificate need it explicitly,
for example with the AWS CLI's `--ca-bundle` option.

## Operation

### Data path and scaling

The door relays all data between clients and pools: S3 clients cannot follow
redirects to pools.  Pools accept the door's connections from the address in
`s3.net.internal`, by default the address of the door host.  To scale, run
several doors and put them behind a load balancer or a DNS alias; any door can
serve any request, also the parts of the same multipart upload.

Transfers use the HTTP movers of the pools, in the queue `s3.mover.queue`.

### Virtual-host style requests

Clients that put the bucket into the host name
(`https://experiment.s3.example.org/key`) need a wildcard DNS entry and a
wildcard host certificate, and the door must know the host name below which
buckets are addressed:

```ini
s3.net.virtual-host = s3.example.org
```

### Logging and accounting

The door writes an access log (logger `org.dcache.access.s3`) with the access
key and the S3 operation of each request.  Billing records each transfer with
the dCache user, like the transfers of other doors.

### Admin commands

Besides `s3 uploads`, the door has the admin commands `login clear cache` and
`login dump cache` for the gPlazma cache.  `info` shows the active transfers,
the cache statistics and the last sweep of multipart uploads.

## Limitations

The door does not support:

- versioning and object lock (objects have the version id `null`, as in an S3
  bucket that never had versioning), lifecycle rules, replication;
- ACLs other than `private`, bucket policies, public access blocks;
- server-side encryption and object tagging;
- browser uploads (POST with a form);
- conditional writes (`If-Match` and `If-None-Match` on PUT and DELETE);
- listing with a delimiter other than `/`;
- anonymous access, and accounts managed through IAM or STS.

Unsupported requests fail with `NotImplemented`.

A listing without delimiter walks the directory tree below the prefix.  It
returns a shorter page, which clients continue from, once it has read
`s3.limits.list.max-entries` directory entries.  Empty directories appear as
common prefixes in listings with delimiter.

For features the door lacks, dCache can also be exported over S3 with a
gateway on an NFS mount; see
[Exporting dCache over S3 with VersityGW](cookbook-s3-versitygw.md).

## Properties

The properties of the door are described in
`/usr/share/dcache/defaults/s3.properties`.
