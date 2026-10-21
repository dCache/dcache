The S3 Service
==============

The `s3` service is a door that lets clients of the Amazon S3 protocol
(the AWS CLI and SDKs, rclone, s3cmd and others) read and write data in
dCache.

-----
[TOC bullet hierarchy]
-----

## Introduction

S3 clients address data as objects in buckets.  The S3 door maps a
bucket to a directory and an object to a file, so that the same data is
available through S3 and through every other dCache door.

S3 clients authenticate with an access key and a secret key.  The door
looks the secret key up in a key file, verifies the request's signature
against it, and asks gPlazma which dCache user the access key belongs
to.  From then on the request is handled like a request from any other
door: the user's permissions apply, data is read from and written to
pools, and billing records the transfer.

The door supports listing buckets and objects, reading objects
(including ranges and conditional requests), writing, copying and
deleting objects, multipart uploads, creating and deleting buckets,
object metadata, and files that have a copy only on tape.  Signature
version 4 is always accepted; the deprecated version 2 is accepted by
default as well and can be turned off for clients that support version
4 only.  See [Limitations](#limitations) for what is not supported.

The S3 protocol layer is based on
[S3Proxy](https://github.com/gaul/s3proxy).

## Setting up

To run the door, add it to a domain in the layout file:

```ini
[<domainName>/s3]
```

For example:

```ini
[s3-${host.name}Domain]
[s3-${host.name}Domain/s3]
```

By default the door listens on port 9000 and uses HTTPS.  For that it
needs a server certificate and the CA certificates, by default the host
certificate in `/etc/grid-security`.  Client certificates are not used.
The port and interface are controlled by `s3.net.port` and
`s3.net.listen`.

### Access keys

The door reads the secret key of each access key from the file in
`s3.authn.keystore.file`, by default
`${dcache.paths.etc}/s3-keys.conf`.  The file holds one access key and
secret per line, separated by whitespace:

```
# access-key   secret
AKIAEXAMPLE0001  wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

Empty lines and lines starting with `#` are ignored.  The file contains
the secrets in plain text, so it should be readable only by the user
running dCache:

```console-root
chmod 600 /etc/dcache/s3-keys.conf
```

The door re-reads the file when it changes, so adding a key takes effect
with the next request.  If any line of the file is invalid, the door
rejects the whole file and accepts no access key until it is fixed.  A
secret is checked against the file on every request, so removing a key
from the file stops it working at once.

> **NOTE**
>
> Each host that runs an S3 door needs its own copy of the key file.

### Mapping access keys to users

After verifying a request's signature, the door logs the access key in
as a principal of type `s3`, which gPlazma maps to a dCache user like
any other authenticated identity.  As usual, a `map` plugin associates
the principal with a user and a `session` plugin gives that user a root
and home directory:

```
map      sufficient  multimap
session  sufficient  omnisession
```

With the multimap plugin, the entry for an access key looks like:

```
s3:AKIAEXAMPLE0001    username:alice uid:1000 gid:1000,true
```

The `s3` principal is an input to gPlazma only; it does not appear in
the login result.

The door caches gPlazma's answer for an access key, by default for 60
seconds (`s3.service.gplazma.cache.*`).  A failed login is cached as
well.  The secret is still checked against the key file on every
request.

### The door root

By default the bucket root is `/` (`s3.root`), so every top-level
directory is a bucket.  See [Buckets and objects](#buckets-and-objects)
for how the root interacts with the user's own root directory.

## Buckets and objects

### Where buckets live

A bucket is a directory below the door's root (`s3.root`, default `/`).
An object key is a path below the bucket: the key `run1/data/file.dat`
in bucket `experiment` is the file
`/<root>/experiment/run1/data/file.dat`.  Writing an object creates any
missing parent directories; deleting an object keeps them.

If the root directory that gPlazma assigns to the user lies below
`s3.root`, the user's buckets are the directories below the user's root
instead, so different users can be confined to different areas.  A user
whose root is outside `s3.root` cannot use the door and is refused at
login.

Creating a bucket creates its directory; deleting a bucket removes its
directory (and any empty directories below it) and fails with
`BucketNotEmpty` while the bucket still holds a file.

Some keys that S3 allows have no place in dCache and are refused with
`InvalidArgument`:

- keys with an empty segment (`a//b`), a leading or trailing `/`, or a
  `.` or `..` segment;
- a key below another object (`a/b` while `a` is an object), or an
  object at a key that is a prefix of existing keys (`a` while `a/b`
  exists), since dCache cannot have a file and a directory of the same
  name.

Names the door reserves for itself are hidden from clients: a directory
`.s3-multipart` in each bucket and files named `.s3-tmp-*`.  Directories
whose name starts with `.` are not listed as buckets, since S3 bucket
names never start with a dot.

### Permissions

The door does not implement S3 access control: what a user may do is
decided by dCache's permissions (POSIX permissions and, if enabled,
ACLs), as for every other door.  In particular, an object is readable by
another user if the file's permissions allow it, unlike S3's default of
private objects.  Requests to set an ACL other than `private` fail with
`NotImplemented`.

### ETags and metadata

The ETag of an object is its MD5 checksum.  The door computes the MD5
while it writes an object and stores it as the file's checksum, so an
object written through an S3 door always has an MD5 ETag.  A file that
dCache has no MD5 checksum for gets an opaque ETag derived from the
file's identity and modification time, with a `-1` suffix that tells
clients not to compare it with an MD5 checksum.  The ETag of an object
that was uploaded in parts is the MD5 checksum of the whole object, not
S3's checksum of the parts' checksums.

The content type, the other standard content headers (`Cache-Control`,
`Content-Disposition`, `Content-Encoding`, `Content-Language`,
`Expires`) and user metadata (`x-amz-meta-*`) are stored as extended
attributes of the file, under names starting with `s3.` (for example
`s3.content-type` and `s3.meta.<name>`).

## Writing objects

An object is written to a hidden file in the object's directory, which
replaces the object's file once all the data has arrived and its
checksum has been recorded.  Readers therefore see either the old or the
new object, never a partly written one.  If the upload fails, the hidden
file is removed.

Clients may upload a large object in parts.  The door keeps the parts of
a multipart upload in a hidden directory `.s3-multipart` in the bucket,
one file per part.  Completing the upload streams the parts through the
door into the object; until then, the upload needs space for both the
parts and the finished object.  The ETag of the completed object is its
MD5 checksum.

An upload that is neither completed nor aborted is removed once it is
older than `s3.limits.multipart.max-age` (default 7 days).  Every hour
the door sweeps the buckets directly below `s3.root`; a bucket below a
user's own root directory is swept when a new upload is started in it.
The admin commands `s3 uploads ls`, `s3 uploads rm` and
`s3 uploads sweep` list, remove and sweep the uploads of a bucket:

```console-root
\s S3-${host.name} s3 uploads ls /experiment
```

Files of uploads that were interrupted, for example by a door restart,
are removed with their bucket once they are older than the same maximum
age.

## Files on tape

A file whose only copy is on tape, with no replica on any pool, is
listed with the storage class `GLACIER`.  Reading it fails with
`InvalidObjectState` until it is restored with the S3 operation
RestoreObject:

```console-user
aws --endpoint-url https://<door>:9000 s3api restore-object \
    --bucket experiment --key run1/data/file.dat --restore-request Days=2
```

A restore pins the file on disk through the PinManager for the number of
days the client asks for, but never for more than `s3.restore.max-days`
(default 30).  While the file is being staged, HEAD answers with
`x-amz-restore: ongoing-request="true"`.  Once the file is on disk it is
listed as `STANDARD` and can be read.  A file that already has a replica
on a pool is treated as restored and is not pinned.

A restore is answered with HTTP status 202 while it is in progress, and
with 200 if the object was already restored.

## Clients

The door serves path-style requests, in which the bucket is part of the
path of the URL (`https://<door>:9000/<bucket>/<key>`).  Clients must be
configured accordingly unless the door also supports
[virtual-host style](#virtual-host-style-requests).  The AWS CLI:

```console-user
aws configure set default.s3.addressing_style path
aws --endpoint-url https://<door>:9000 s3 ls s3://experiment/
aws --endpoint-url https://<door>:9000 s3 cp file.dat s3://experiment/run1/
```

rclone:

```console-user
rclone --s3-provider Other --s3-endpoint https://<door>:9000 \
       --s3-access-key-id AKIAEXAMPLE0001 --s3-secret-access-key <secret> \
       copy ./run1 :s3:experiment/run1
```

s3cmd:

```console-user
s3cmd --host <door>:9000 --host-bucket <door>:9000 --no-ssl \
      --access_key AKIAEXAMPLE0001 --secret_key <secret> ls s3://experiment
```

Clients that do not already trust the CA which issued the door's server
certificate need to be given it explicitly, for example with the AWS
CLI's `--ca-bundle` option.

## Operation

### Data path and scaling

The door relays all data between S3 clients and pools; an S3 client
cannot follow a redirect to a pool.  A pool accepts the door's data
connection from the address in `s3.net.internal`, by default the address
of the door's own host.

To handle more load, run several S3 doors, for example behind a
load-balancer or a DNS alias.  Any door can serve any request, including
the parts of one multipart upload.  Transfers use the pools' HTTP
movers, in the queue configured by `s3.mover.queue` (empty for the
default queue).

### Virtual-host style requests

A client may address a bucket in the host name
(`https://<bucket>.s3.example.org/<key>`) instead of the path.  This
needs a wildcard DNS entry and a server certificate that covers the
bucket names, and the door must be told the host name below which
buckets are addressed:

```ini
s3.net.virtual-host = s3.example.org
```

Leave the property empty to serve path-style requests only.

### Logging

The door writes an access log with the access key and the S3 operation
of each request; the logger is `org.dcache.access.s3`.  Billing records
each transfer with the dCache user, like a transfer through any other
door.

### Admin commands

The door provides the admin commands

- `s3 uploads ls <bucket>` -- list the multipart uploads in progress;
- `s3 uploads rm <bucket> <upload-id>` -- remove an upload;
- `s3 uploads sweep [<bucket>]` -- remove uploads older than the maximum
  age.

The bucket argument is the path of the bucket's directory.

### Timeouts

The door checks that a signed request's date is close to the door's
clock; requests further away than `s3.authn.max-time-skew` (default 900
seconds) are refused with `RequestTimeTooSkewed`.  Timeouts for the
other services and the movers are configured with the
`s3.service.*` and `s3.mover.*` properties.

## Limitations

The door does not support:

- versioning and object lock (objects report the version id `null`, as
  in a bucket that never had versioning), lifecycle rules, replication
  and other bucket configuration;
- S3 access control lists other than `private`, bucket policies and
  public-access blocks;
- server-side encryption;
- object tagging;
- browser uploads (an HTTP POST with a form);
- conditional writes (`If-Match` and `If-None-Match` on PUT and DELETE);
- a listing delimiter other than `/`;
- anonymous requests, and users managed through IAM or STS.

Unsupported requests are answered with `NotImplemented`.

A listing without a delimiter walks the directory tree below the prefix.
Once it has read `s3.limits.list.max-entries` (default 10000) directory
entries and has found at least one key, it returns a shorter page, which
the client continues from its last key; a single directory is always
read in full.  At most 1000 keys are returned in one page.

The properties of the door are described in
`/usr/share/dcache/defaults/s3.properties`.
