Chapter 5. WebDAV
=================

**Table of Contents**

+ [Authentication](#authentication)
+ [File operations](#file-operations)
    + [Redirection](#redirection)
    + [Checksums](#checksums)
+ [Directory operations](#directory-operations)
+ [Requesting macaroons](#requesting-macaroons)
+ [Third-party transfers](#third-party-transfers)

From the corresponding [English language Wikipeda
entry](https://en.wikipedia.org/wiki/WebDAV),

> Web Distributed Authoring and Versioning (WebDAV) is an extension of
> the Hypertext Transfer Protocol (HTTP) that allows clients to
> perform remote Web content authoring operations.

In simple terms, HTTP allows a client to upload, download and delete
files, while WebDAV allows filesystem-like operations, such as to
rename files and list directory contents.

Due to its overwhelming popularity, there are many HTTP clients.
Although WebDAV is less popular, there are still many clients from
which you can choose.  In this chapter, we will use `curl` to
illustrate most HTTP operations, and `rclone` as a specific WebDAV
client.  Other clients should also work and you should not read these
choices as an endorsement of those clients over others.

## Authentication

Authentication is the process where the client proves the identity of
the user.  Perhaps the most common is password based authentication,
where the client proving to the server that it knows some secret code
supplied by the user.

HTTP is very flexible in how it handles authentication, with may
different ways a client can prove its identity.  Several of these
options are available to dCache clients.

Broadly speaking there are two ways of authenticating: the
`Authorization` HTTP request header (which often uses some bearer
token) and through SSL/TLS.

A bearer token is a token that requires no interaction to
authenticate: supplying the token as part of the request is
sufficient.  This is simpler than the alternatives, but comes at a
cost: any agent able to observe the HTTP request has the token and can
subsequently impersonate the valid client.  Encryption is mandatory
when using bearer tokens; however, even with transport encryption
(such as SSL/TLS), bearer tokens are inherently risky, and often use
restrictions to reduce the impact should they be stolen.

SSL/TLS authentication, in contrast to `Authorization` header
authentication, happens before the HTTP requests.  After establishing
the TCP connection, a TLS handshake takes place to ensure the
connection is encrypted.  During this TLS handshake, the client can
authenticate.  Unlike bearer tokens, this process is iterative.  This
allows the client to authenticate without revealing all information,
allowing the authentication to take place before the encrypted
connection is established.  When using TLS-based authentication, the
client makes requests without any `Authorization` HTTP request
headers.

This section describes the different authentication options that
dCache supports.

Please note that the actual authentication supported by any specific
dCache instance is controlled by the server's configuration, so you
may not have access to all these authentication options.



### Basic

Basic authentication is the simplest scheme.  It involves the client
sending the username and complete password to dCache.

The following example shows curl using Basic authentication, prompting
the user to enter their password.

```console-user
curl -u paul https://dcache.example.org/users/paul/private-file
|Enter host password for user 'paul':
```

Although this approach is very simple and widely supported by clients,
it relies on the network connection to encrypt the content.  If basic
authentication is used with an unencrypted request (a URL starting
with `http://`) then the password will be sent unencrypted over the
network.  Anyone who is able to capture the network traffic will learn
the username and password, and the user's account is compromised.

To counter this problem, by default dCache will reject _all_ basic
authentication if the connection is unencrypted, with only URLs
starting `https://` being accepted.

### X.509

X.509 is a technology that uses asymmetric encryption for
authentication.  Asymmetric encryption means each identity has two
keys: the public key and the private key.  After some identity vetting
process, an organisation (known as the certification authority) will
issue a certificate, which contains the public key and some identity
information.  This certificate (along with the private key) is then
used to prove the identity over a network connection.

TLS is the protocol used to establish encrypted network connections
between web-browsers and servers.  This protocol supports X.509 for
authentication.

This X.509 authentication is heavily used in the world-wide web. When
establishing encrypted network connections, web-browsers will check
the identity of the web server.  This TLS authentication involves the
web-server sends its certificate and subsequently responding to the
browser's challenge, so proving the server has the corresponding
private key.

The TLS protocol also supports the client authentication with X.509:
the client sends a certificate and responds to the server's subsequent
challenge, so proving the client has the corresponding private key.
This is much less common, but most popular web-browsers provide at
least some support for this.

By default, dCache allows clients to authenticate using X.509,
although this can only work for encrypted connections.

The following example shows curl authenticating with X.509:

```console-user
curl -E ~/.globus/usercert.pem --key ~/.globus/userkey.pem https://dcache.example.org/users/paul/private-file
Enter PEM pass phrase:
```

It is possible to create a proxy credential from an existing X.509
credential.  A proxy credential is a credential that identifies the
same person, but with a much shorter lifetime (e.g., 12 hours).  Such
credentials may be stored on the filesystem without a password
(trusting the filesystem permissions) and transferred to remote
agents, so they can operate on behalf of the user.

The following example shows curl authentication with a proxy X.509
credential.

```console-user
curl -E /tmp/x509up_u1000 https://dcache.example.org/users/paul/private-file
```

The file `/tmp/x509up_u1000` contains the user's certificate, the
proxy certificate and the proxy private key.

Earlier versions of curl require both the `-E` and the `--key`
options:

```console-user
curl -E /tmp/x509up_u1000 --key /tmp/x509up_u1000 https://dcache.example.org/users/paul/private-file
```

Yet earlier versions of curl require a hack to ensure it sends both
the user and proxy certificates.  The `--cacert` options must also be
specified:

```console-user
curl --cacert /tmp/x509up_u1000 -E /tmp/x509up_u1000 --key /tmp/x509up_u1000 https://dcache.example.org/users/paul/private-file
```

### Kerberos

The TLS protocol, which allows the web client and web server to secure
a TCP connection, has an extension called SPNEGO.  SPNEGO allows the
client and server to negotiate which authentication scheme is used
(instead of X.509).  The most common reason to use SPNEGO is to
support Kerberos-based authentication.

Here is an example where curl uses Kerberos (via SPNEGO) to
authenticate:

```console-user
curl --negotiate -u : https://dcache.example.org/users/paul/private-file
```

### Macaroons

Macaroons are bearer tokens that have caveats embedded within the
token.  In general, these caveats restrict who can use the macaroon,
for how long the token may be used, which operations are allowed, or
which files or directories may be targeted.  A typical use-case is to
create a macaroon that allows the bearer to download a specific file
for a limited period.

A client can use a macaroon in two ways: in the Authorization HTTP
request header or in the URL query part.

#### Request header

The authorisation request header allows the HTTP client to provide
dCache with information about the clients identity.  To support
macaroon-based request authorisation, the client may include the
`bearer MACAROON` value to the `Authorization` request header, where
`MACAROON` is the actual macaroon.

The following example shows curl making a request authorised using a
macaroon:

```console-user
curl -H "Authorization: bearer $MACAROON" https://dcache.example.org/users/paul/private-file
```

This assumes that the macaroon is stored in the environment variable
`MACAROON`.

#### URL query part

Not all clients support adding a custom request `Authorization`
request header.  Web-browsers are common examples of such clients. For
such clients, dCache supports an alternative approach: including the
token in the URL.

The query part of the URL contains key-value pairs.  dCache will
recognise the `authz` key and accept the corresponding value as a
bearer token.

The following example shows curl making a request authorised using a
macaroon embedded within the URL:

```console-user
curl https://dcache.example.org/users/paul/private-file?authz=MACAROON
```

Where `MACAROON` is replaced by the actual macaroon.

Embedding the macaroon within the URL has the advantage of providing a
URL that will "just work" for most clients.  For example, if a
macaroon is created that targets a specific file and is valid for 10
minutes then a macaroon-embedded URL could be shared with any client,
allowing that client to fetch the content from dCache without further
authentication.

Such embedded macaroons may be used to support many advanced
work-flows.


### SciToken

A SciToken server is an OAuth2 server that issues its clients with a
token that describes what that client is allowed to do.

If configured to do so, dCache will accept SciTokens and allow
operations that are compatible with the list of operations contained
within the SciToken token.

The client supplies the SciToken token as a bearer token: either using
the HTTP Authorization request header or embedded within the URL.


### OpenID Connect

OpenID-Connect is a protocol, based on OAuth2, that allows the client
to obtain a token that may be used to identify the user.

If configured to do so, dCache will accept OpenID-Connect access
tokens and authenticate the user based on those tokens.

The client supplies the access token as a bearer token: either using
the HTTP Authorization request header or embedded within the URL.


## File operations

This section describes operations that target a file, such as
uploading, downloading, discovering a file's metadata and deleting a
file.

### Redirection

Redirection is an important feature of dCache's HTTP support.  With
redirection, dCache will respond to a file transfer operation (an
upload or a download) with the HTTP response that tells the client to
transfer the data directly with the server that has the files data
(for downloads) or that will accept the data (for uploads).

#### Redirection on download

By default, when the client makes a request for data, dCache will
respond with a `307` status code, with the `Location` response header
containing the URL that describes the data server from which the
client can download the data directly.

This redirection is a standard feature of the HTTP protocol.
Therefore most HTTP clients support redirection on download; for
example, curl supports following redirection if the `-L` command-line
option is supplied.

Although WebDAV is based on HTTP, WebDAV servers typically operate
with a restricted set of responses.  Therefore, many WebDAV clients do
not support the full set of valid HTTP responses.  As a specific
example of this, most WebDAV servers never issue a redirection status
code.  Therefore, support in WebDAV clients for redirection on
download is much poorer than for simple HTTP clients.

#### Redirection on upload

Redirection for upload is a non-standard HTTP extension, first
introduced by the Amazon S3 service.  Through the popularity of the S3
protocol, this extension is now widely supported by HTTP clients.

The redirection is an extension of the standard Expect-100
interaction.

With Expect-100, the client initiates the upload by sending just the
request headers, without yet sending any of the file's data.  One of
the HTTP request headers requests dCache makes an intermediate
(non-final) reply, confirming it will accept the pending data.  This
allows dCache to check the user is authorised to upload the data and
that it has sufficient capacity to store the file before the client
sends the file's data.

If not redirecting uploads and it will accept the upload then dCache
replies with the expected `100` intermediate status code; on receiving
this status code, the client sends the file's data.  However, if
dCache knows that the upload cannot succeed (e.g., because the user is
not authorised to upload) then it replies with the appropriate error
status code.  This process allows the client to avoid sending large
files if dCache knows from the request headers that the upload will be
rejected.

##### Support in HTTP clients

To support redirection, the S3 extension uses Expect-100 as above.
However, instead of replying with a 100-Continue response, dCache
replies with a `307` (temporary redirect) status code, with the
`Location` response header indicating the URL to which the client
should send the data data.

Clients that do not support redirection-on-upload will likely consider
any non `100` status code as an error, and fail the upload.

dCache has a fall-back behaviour: if it replies with a redirection
response, but the client sends the data (to the WebDAV door) anyway
then dCache will proxy the data.

##### Support in WebDAV

As with redirection on download, support within WebDAV for redirection
on upload is poor.

##### curl expect-100 timeout

The Expect-100 behaviour was introduced with HTTP v1.1.  For HTTP v1.0
servers, uploading data using Expect-100 would result in a deadlock:
the server is waiting for the data and the client is waiting for the
100 status code.

To avoid this deadlock, curl contains a timeout for Expect-100
responses: if dCache does not reply to the initial (header-only)
request with a 100-Continue response quickly enough then curl believes
dCache to be an HTTP v1.0 server and will send the file's content.

Note that, under these circumstances, the WebDAV door will accept the
file's data and send it to the pool (data server) on behalf of the
client.  However, this places a higher load on the WebDAV door and
will likely result in poorer performance.

dCache replies with a redirection only once it knows to data server
will accept the data, and that the server is ready to accept the data.
If all the storage nodes are busy, this may take some time to
establish.  While this is happening, dCache has not replied to the
client's initial header-only request.  If this process takes too long,
curl considers the server as implementing HTTP v1.0 and will send the
data.

By default, curl will wait one second after sending the expect-100
request.  Since curl v7.47.0, this timeout is controlled by the
`--expect100-timeout` command-line option.

Therefore, it is recommended to use curl v7.47.0 or later and to
specify the `--expect100-timeout` option with a large timeout value.

## Checksums

Checksums are often used to verify a file's integrity.

There are three ways of discovering or otherwise influencing checksums
for files stored in dCache: RFC 3230 headers, WebDAV properties, and
Content-MD5 headers.

#### RFC 3230

RFC 3230 allows the client to request that a file's checksum is sent
as part of the server's response.  This is done by specifying the
`Want-Digest` request header with a value describing which checksum
algorithms are acceptable to the client; for example, the request
header `Want-Digest: ADLER32` requests the server provides the ADLER32
checksum in the response, while `Want-Digest: MD5,ADLER32;q=0.5`
indicates the client would prefer the MD5 checksum but to supply the
ADLER32 if the MD5 checksum is unavailable.

dCache supports RFC 3230 requests on HEAD, GET, PUT and some COPY
requests.

The HEAD request is typically used to fetch a file's metadata without
fetching the content of the file.  The following example shows curl
fetching a file's checksum information through a HEAD request:

```console-user
curl -H 'Want-Digest: adler32' -I http://dcache.example.org/public/my-data
|HTTP/1.1 200 OK
|Date: Mon, 23 Sep 2019 10:06:24 GMT
|Server: dCache/6.0.0-SNAPSHOT
|Accept-Ranges: bytes
|ETag: "000002BE4A36C1C84AD08BC5693EABB040E7_1548461363"
|Last-Modified: Mon, 23 Sep 2019 04:05:24 GMT
|Content-Type: application/octet-stream
|Digest: adler32=5ae07809
|Content-Length: 63296477
```

The `Digest` response header contains the request adler32 checksum value.

If the file's content is also required, both the file's data and its
checksum value may be obtained in a single request, by specifying the
`Want-Digest` request header in the GET request.

The following example shows curl obtaining the file's data along with
its checksum.

```console-user
curl -L -D my-data.headers -H 'Want-Digest: adler32' -o my-data http://dcache.example.org/public/my-data
|  0     0    0     0    0     0      0      0 --:--:-- --:--:-- --:--:--     0
|100 60.3M  100 60.3M    0     0   820k      0  0:01:15  0:01:15 --:--:--  731k
grep Digest: /tmp/headers 
|Digest: adler32=5ae07809
```

When uploading data, it may be useful to know the checksum of the
uploaded data.  This is possible by issuing a HEAD request after a
successful PUT request.  As an optimisation, it is also possible to
obtain the checksum as part of the PUT request's response headers.

The following example shows curl obtaining the checksum of a freshly
uploaded file.  Unlike the previous examples, this upload request is
authenticated (`-E /tmp/x509up_u1000`).

```console-user
curl -D- -L -T /bin/bash -E /tmp/x509up_u1000 -H 'Want-Digest: adler32' https://dcache.example.org/public/file
|HTTP/1.1 100 Continue
|
|HTTP/1.1 201 Created
|Date: Mon, 23 Sep 2019 10:17:05 GMT
|Server: dCache/6.0.0-SNAPSHOT
|Accept-Ranges: bytes
|ETag: "00009E56B0A0DAC5481C9BC339FAE6F7D196_1570761966"
|Digest: adler32=af543afc
|Transfer-Encoding: chunked
```

There is an important benefit to including the RFC-3230 `Want-Digest`
header in the PUT request.  Typically, dCache will only calculate
checksum value for configured algorithms when it receives a new file.
By default, dCache calculates the ADLER32 checksum and not the MD5
checksum.  Therefore, by default, subsequent HEAD (or GET) requests
for the MD5 checksum will not provide this information:

```console-user
curl -I -H 'Want-Digest: md5' https://dcache.example.org/public/file
|HTTP/1.1 200 OK
|Date: Mon, 23 Sep 2019 10:26:27 GMT
|Server: dCache/6.0.0-SNAPSHOT
|Accept-Ranges: bytes
|ETag: "00009E56B0A0DAC5481C9BC339FAE6F7D196_1570761966"
|Last-Modified: Mon, 23 Sep 2019 10:17:05 GMT
|Content-Length: 1099016
```

However, by specifying a desire for the MD5 checksum using the
`Want-Digest` in the PUT request, dCache can ensure this value is
calculated as it receives the file's data:

```console-user
curl -D- -L -T /bin/bash -E /tmp/x509up_u1000 -H 'Want-Digest: md5' https://dcache.example.org/public/file-with-MD5
|HTTP/1.1 100 Continue
|
|HTTP/1.1 201 Created
|Date: Mon, 23 Sep 2019 10:28:27 GMT
|Server: dCache/6.0.0-SNAPSHOT
|Accept-Ranges: bytes
|ETag: "00006BC3FF06CC1047A291006C36DCDC252A_1571445045"
|Digest: md5=rFb0uPrFc5zNtFd30xO+zw==
|Transfer-Encoding: chunked
```

Even if the upload `Digest` response header (containing the desired
MD5 checksum value) is ignored, a subsequent HEAD request will yield
the desired checksum value:

```console-user
curl -I -H 'Want-Digest: md5' https://dcache.example.org/public/file-with-MD5
|HTTP/1.1 200 OK
|Date: Mon, 23 Sep 2019 10:31:14 GMT
|Server: dCache/6.0.0-SNAPSHOT
|Accept-Ranges: bytes
|ETag: "00006BC3FF06CC1047A291006C36DCDC252A_1571445045"
|Last-Modified: Mon, 23 Sep 2019 10:28:27 GMT
|Digest: md5=rFb0uPrFc5zNtFd30xO+zw==
|Content-Length: 1099016
```

Checksums may also be requested as part of HTTP third-party copy
requests.  This is discussed in the following section on third-party
copies.

#### WebDAV checksum properties

The list of all known checksums of a specific file is available as a
(read-only) WebDAV property.

The property's namespace is `http://www.dcache.org/2013/webdav` and
the property name is `Checksums`.  The property value is a
comma-separated list of checksum values, following the format
described in RFC 3230 for `Digest` header values.

Note that the Checksums property is not returned by default.
Therefore a simple `PROPFIND` request without specifying any desired
properties will not include the checksum details.

The following example shows curl requesting the `Checksums` property
for the file `file-with-MD5` created in an earlier example.  Note that
the output from dCache is passed through the `xmllint` command.  This
is not essential and is used only to make the output easier to read.

```console-user
echo '<?xml version="1.0"?><propfind xmlns="DAV:"><prop><d:Checksums xmlns:d="http://www.dcache.org/2013/webdav"/></prop></propfind>' | curl -s -T - -X PROPFIND https://dcache.example.org/public/file-with-MD5 | xmllint -format -
|<?xml version="1.0" encoding="utf-8"?>
|<d:multistatus xmlns:cal="urn:ietf:params:xml:ns:caldav" xmlns:cs="http://calendarserver.org/ns/" xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:ns1="http://www.dcache.org/2013/webdav" xmlns:d="DAV:">
|  <d:response>
|    <d:href>/public/file-with-MD5</d:href>
|    <d:propstat>
|      <d:prop>
|        <ns1:Checksums>md5=rFb0uPrFc5zNtFd30xO+zw==,adler32=af543afc</ns1:Checksums>
|      </d:prop>
|      <d:status>HTTP/1.1 200 OK</d:status>
|    </d:propstat>
|  </d:response>
|</d:multistatus>
```

#### Failing upload if data is corrupt

A common objective is to upload a file and to be certain that the
stored file has identical data to the locally stored file; i.e., the
file's data was not corrupted while being delivered to the server.

It is possible to achieve this by uploading the file and subsequently
requesting the file's checksum.  With RFC 3230 and placing the
`Want-Digest` request header within the PUT request, it is possible to
discover the uploaded file's checksum without making a subsequent
request.

However, if the file is discovered to be corrupt, the client is then
responsible for either removing the corrupt file or attempting another
upload.  Until either is done, the file exists in dCache with corrupt
data.

Placing this responsibility on the client may be problematic: the
client could halt (or be interrupted) before the recovery procedure
completes, or may be authorised only to upload data and not overwrite
existing data nor delete existing data.

An alternative approach is to supply a known checksum value when
uploading the data.  dCache then verifies this known checksum value
matches that of the data it receives.  If the two checksums do not
match then the upload fails.

RFC 1864 describes a standard approach for sending a known checksum:
the `Content-MD5` header.

A `Content-MD5` header is similar to the RFC-3230 `Digest` header.
One important difference is that a server that does not support the
`Digest` header will accept the request, while a server that does not
support the `Content-MD5` header will fail the request.  Therefore, a
successful upload with `Content-MD5` can only happen if the data is
not corrupt.

The following example shows a file being uploaded using the
`Content-MD5` request header to ensure the file has not be corrupted.
Note that this request is authenticated (`-E /tmp/x509up_u1000`)

```console-user
curl -D- -L -T /bin/bash -H "Content-MD5: $(md5sum /bin/bash | cut -d' ' -f1 | xxd -r -p | base64)" -E /tmp/x509up_u1000 https://dcache.example.org/Users/paul/file-content-md5
|HTTP/1.1 100 Continue
|
|HTTP/1.1 201 Created
|Date: Mon, 23 Sep 2019 11:19:59 GMT
|Server: dCache/6.0.0-SNAPSHOT
|Accept-Ranges: bytes
|ETag: "00004F27D12A7220433DA294D01E8F5785C3_1574536970"
|Transfer-Encoding: chunked
```

The header value contains the MD5 checksum value using BASE64
encoding, rather than the more common hexadecimal encoding.  The
command `md5sum /bin/bash | cut -d' ' -f1 | xxd -r -p | base64`
calculates the MD5 checksum of the file `/bin/bash` in this BASE64
encoding.

Therefore, the value `"Content-MD5: $(md5sum /bin/bash | cut -d' ' -f1
| xxd -r -p | base64)"` is the `Content-MD5` header along with the
correct value for the target file.

If the upload is corrupted then dCache replies with a `400` status
code.  The status line provides additional information.

In the following example, the checksum is calculated for an unrelated
file (`/bin/echo`).  Including this other file's checksum when
uploading the file `/bin/bash` is used to simulate data corruption.

```console-user
✔ 13:19:59 dCache [master ✚1] $ curl -D- -L -T /bin/bash -H "Content-MD5: $(md5sum /bin/echo | cut -d' ' -f1 | xxd -r -p | base64)" -E /tmp/x509up_u1000 https://dcache.example.org/Users/paul/file-content-md5
|HTTP/1.1 100 Continue
|
|HTTP/1.1 400 Checksum mismatch (expected=[2:29f4bf55fe826e5b167340f91aeb0f49], actual=[1:af543afc, 2:ac56f4b8fac5739ccdb45777d313becf])
|Date: Mon, 23 Sep 2019 11:20:56 GMT
|Server: dCache/6.0.0-SNAPSHOT
|Transfer-Encoding: chunked
```

The status message includes both the expected checksums and the actual
checksums.  The `1:` indicates an ADLER32 checksum value, while the
`2:` prefix indicates an MD5 checksum value.

## Directory operations

## Requesting macaroons

A client may request dCache issue a macaroon by making a specific
request to the WebDAV door.  This section describes this process; the
earlier section describes how macaroons may be used.


## Third-party transfers

Third-party transfers are requests to dCache that it transfers a file
with another HTTP server.  This differs from normal client
interactions as it does not require the client first downloads the
file and subsequently uploads the file.
