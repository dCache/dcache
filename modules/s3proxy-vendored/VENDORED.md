# Vendored s3proxy

S3 protocol handling (request parsing, SigV2/SigV4 verification, aws-chunked
bodies, XML responses) copied from [s3proxy](https://github.com/gaul/s3proxy)
and patched to run on dCache's Jetty 9 (`javax.servlet`).

The dCache S3 door (`modules/dcache-s3`) implements
`org.gaul.s3proxy.blobstore.BlobStore` and `org.gaul.s3proxy.BlobStoreLocator`
and calls `S3ProxyHandler.doHandle`. Nothing else in dCache should depend on
this module.

## Upstream

| | |
|---|---|
| Tag | `s3proxy-4.1.1` |
| Commit | `fb9cf7fd917fbdb9aed9cfc6d788f043468c9d91` |
| License | Apache-2.0 (`LICENSE`); upstream file headers are kept |

Classes are kept in their original packages (`org.gaul.s3proxy.*`) so that
replacing this module by the upstream artifact needs no import changes in
dCache code.

## What is copied

Only the closure of `S3ProxyHandler`, `BlobStoreLocator` and `AccessGrant`:
the handler, `auth/`, `checksum/`, `blobstore/` (interface, helpers, domain
classes, `ForwardingBlobStore`) and the request XML classes.

Not copied: `S3Proxy`, `Main`, `S3ProxyHandlerJetty`, `S3ErrorHandler`,
metrics, `middleware/`, and all provider blob stores (`nio2blob/`, `awssdk/`,
`azureblob/`, `gcloudsdk/`, `openstackswift/`, `sftp/`).

Tests copied: `auth/*Test`, `checksum/*Test`, converted to JUnit 4 (see
patch 5).

## dCache patches

The full diff against upstream is in `patches/dcache.patch`. Changed code is
marked with `dCache patch` comments.

1. `jakarta.servlet` → `javax.servlet` (imports only) in `S3ProxyHandler`,
   `S3ErrorCode`, `auth/AwsSignature`, `checksum/FlexChecksum`,
   `checksum/MpuChecksums`.
2. `S3ProxyHandler.handlePostBlob` (browser POST upload) throws
   `NOT_IMPLEMENTED`: it needs Jetty 12 `MultiPartFormData`. The four Jetty 12
   imports are removed.
3. Dependencies differ from upstream: Jackson 3 (`tools.jackson`) is used as
   is; `jackson-annotations` is managed at 2.22 for all of dCache (Jackson 3
   needs `JsonSerializeAs`); AWS SDK v2 `s3` is used for model classes only,
   with its HTTP clients excluded.
4. `checksum/MpuChecksums`: the call to
   `AbstractNio2BlobStore.multipartPartName` is replaced by a private copy of
   that one-line helper, so `nio2blob/` need not be vendored. It is only used
   for nio2blob stores.
5. Tests are converted from JUnit 6 + AssertJ to JUnit 4 + Hamcrest, like
   the rest of dCache: `assertThat(x).isEqualTo(y)` → `assertEquals(y, x)`
   (`assertArrayEquals` for `byte[]`), `assertThatThrownBy` →
   `Assert.assertThrows` followed by assertions on the exception.

6. RestoreObject (`POST /bucket/key?restore`): `restore` is removed from
   `UNSUPPORTED_PARAMETERS`; `S3ProxyHandler.handleRestoreObject` reads the
   request's `Days` (`RestoreObjectXmlRequest`, a dCache file) and calls the
   new `BlobStore.restoreObject`, which answers through the HTTP status of
   its response (202 started, 200 already restored). `ForwardingBlobStore`
   forwards it; `S3Operation.RESTORE_OBJECT` and `AwsHttpHeaders.RESTORE`
   are added, and `addMetadataToResponse` sends `x-amz-restore` from
   `HeadObjectResponse.restore()`.

`patches/dcache.patch` is generated with `diff -u` of each vendored source
against the upstream file (`/dev/null` for dCache files), subdirectories
before files as `diff -r` orders them.

## Updating to a newer upstream release

1. Check out the new tag; recompute the class closure of the three entry
   classes (same-package references count, not only imports).
2. Copy the files over this module, then apply `patches/dcache.patch`
   (`patch -p1 -d modules/s3proxy-vendored < patches/dcache.patch`) and fix
   rejects.
3. Update the tag and commit above, the version properties in the root
   `pom.xml` (`version.awssdk`, `version.jackson3`,
   `version.jackson-annotations`, `version.jspecify`), and regenerate
   `patches/dcache.patch`.
4. `mvn -am test -pl modules/s3proxy-vendored`, then the `dcache-s3` tests.

## Dropping this module

Once dCache runs on Jetty 12 with `jakarta.servlet`:

1. Delete `modules/s3proxy-vendored` and its `<module>` entry.
2. Depend on `org.gaul:s3proxy` (exclude the provider SDKs not needed).
3. Switch `javax.servlet` to `jakarta.servlet` in the door's Jetty handler.
