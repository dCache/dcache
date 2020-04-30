Chapter 5. WebDAV
=================

**Table of Contents**

+ [Authentication](#authentication)
+ [File operations](#file-operations)
    + [Redirection](#redirection)
    + [Checksums](#checksums)
+ [Directory operations](#directory-operations)
+ [Requesting macaroons](#requesting-macaroons)
    + [Inspecting a macaroon](#inspecting-a-macaroon)
    + [Adding extra caveats](#adding-extra-caveats)
    + [Understanding dCache caveats](#understanding-dcache-caveats)
    + [Requesting a macaroon with caveats](#requesting-a-macaroon-with-caveats)
+ [Third-party transfers](#third-party-transfers)
    + [Authenticating the third-party request](#authenticating-the-third-party-request)
    + [Authorising the third-party request](#authorising-the-third-party-request)
    + [HTTP response](#http-response)
    + [Data integrity](#data-integrity)
    + [Custom transfer headers](#custom-transfer-headers)
    + [Authorising the data transfer](#authorising-the-data-transfer)
    + [Complete example](#complete-example)

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
|&lt;?xml version="1.0" encoding="utf-8"?>
|&lt;d:multistatus xmlns:cal="urn:ietf:params:xml:ns:caldav" xmlns:cs="http://calendarserver.org/ns/" xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:ns1="http://www.dcache.org/2013/webdav" xmlns:d="DAV:">
|  &lt;d:response>
|    &lt;d:href>/public/file-with-MD5&lt;/d:href>
|    &lt;d:propstat>
|      &lt;d:prop>
|        &lt;ns1:Checksums>md5=rFb0uPrFc5zNtFd30xO+zw==,adler32=af543afc&lt;/ns1:Checksums>
|      &lt;/d:prop>
|      &lt;d:status>HTTP/1.1 200 OK&lt;/d:status>
|    &lt;/d:propstat>
|  &lt;/d:response>
|&lt;/d:multistatus>
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

A GET request that targets a directory returns an web page that
describes that directory.  This provides a very simple read-only web
interface for accessing files in dCache, through which you can view
the contents of a directory and view or download files stored in
dCache.

If the URI contains query values (e.g.,
`https://dcache.example.org/?foo=bar`) then those values are included
in the directory web-page's navigation and file download links.  For
example, if the directory request was authorised using the bearer
token `TOKEN` embedded within the URL (`?authz=TOKEN`) then the user
may navigate dCache's directory structure and request file downloads
that are also authorised from this bearer token.  This is particularly
useful when used with macaroons, as it provides an interactive view of
dCache powered by macaroons.

## Requesting macaroons

A client may request dCache issue a macaroon by making a specific
request to the WebDAV door.  This section describes this process; the
earlier section describes how macaroons may be used.

To request a macaroon, the client makes a POST request, with the
`Content-Type` of this request set to `application/macaroon-request`.
Note that this is only possible if the request is authenticated.

The following example shows the simplest request to obtain a macaroon.

```console-user
curl -E /tmp/x509up_u1000 -X POST -H 'Content-Type: application/macaroon-request' https://dcache.example.org/
|{
|    "macaroon": "MDAxY2[...]l8K",
|    "uri": {
|        "targetWithMacaroon": "https://dcache.example.org/?authz=MDAxY2[...]l8K",
|        "baseWithMacaroon": "https://dcache.example.org/?authz=MDAxY2[...]l8K",
|        "target": "https://dcache.example.org/",
|        "base": "https://dcache.example.org/"
|    }
|}
```

> **IMPORTANT**
>
> In this example, the full macaroon would be a 284-character strings.
> To improve readability, this macaroon is replaced by the much
> shorter string `MDAxY2[...]l8K`.  This convention is followed for
> all macaroons in this chapter.

The macaroon request must be authenticated.  In the above example, the
request is authenticated using X.509-based authentication (the option
`-E /tmp/x509up_u1000`).  Macaroon requests may be authenticated with
any supported webdav authentication scheme, with the exception of
SciTokens with path restrictions.

The response is a JSON object that includes various related values.
The first is the actual macaroon, which is the corresponding value of
the `macaroon` JSON object key.

The macaroon may be used when making requests, as described in the
[macaroon authentication section](#macaroons).

The `uri` JSON object value is another JSON object, providing useful
URIs related to this query.  These items may all be derived from the
macaroon value, so are included as a helpful short-cut.  The `base`
value is the URI of the root path, containing the scheme, hostname,
port number (if non-standard).  The `target` resolves the POST
request's path against the `base` URI; if the POST request targeted
the root directory then `base` and `target` URIs have the same value.

The `uri` JSON object value is another JSON object, providing useful
URIs related to this query.  These items may all be derived from the
macaroon value, so are included as a helpful short-cut.

<dt>
<dt><tt>base</tt></dt>

<dd>the URI of the WebDAV server's root path.  This URI contains the
scheme, hostname, port number (if non-standard).  The other URIs may
be derived from this URI.</dd>

<dt><tt>target</tt></dt>

<dd>the POST request's path resolved against the <tt>base</tt> URI.
If the POST request targets the server's root directory then the
<tt>base</tt> and <tt>target</tt> URIs will have the same value.</dd>

<dt><tt>baseWithMacaroon</tt></dt>

<dd>the <tt>base</tt> URI with the macaroon embedded within the URI.
This is achieved adding the macaroon as the value to the `authz`
query-part.  The corresponding URL may be copied into a web-browser to
allow browsing of dCache using the macaroon.</dd>

<dt><tt>targetWithMacaroon</tt></dt>

<dd>the <tt>target</tt> URI with the macaroon embedded within the URI.
This is achieved adding the macaroon as the value to the
<tt>authz</tt> query-part.  If the target is a specific file then this
URL may be used to make requests that target that file that are
authorised using the macaroon (e.g., to download a specific file).  If
the target is a directory then the URL may be copied into a
web-browser to allow browsing of dCache using the macaroon.</dd>

</dt>

### Inspecting a macaroon

A macaroon appears as an opaque string, but actually contains
information on what a user is allowed to do in the form of various
caveats.

While it is not essential to understand the caveats of a macaroon,
showing a macaroon's caveats should make it easier to understand how
to request more restrictive macaroons.

This section will use the Python macaroon library `pymacaroons`.  This
is available pre-packaged; e.g., `apt-get install python-pymacaroons`
or `pip install pymacaroons`.

The following example shows how to list the caveats contained within a
macaroon:

```python
from pymacaroons import Macaroon
import sys

for line in sys.stdin:
    m = Macaroon.deserialize(line)    
    print(m.inspect())
```

Here is a typical response:

```console-user
echo MDAxY2[...]l8K | python inspect-macaroon.py
|location Optional.empty
|identifier aktmMDje
|cid iid:GaVltWFP
|cid id:2002;2002,0;paul
|cid before:2019-09-25T08:12:11.080Z
|cid home:/Users/paul
|signature 0a9dcf9ede9d747fdbf365a88c4de7a65a60a709e9054f3e6f5533b06716365f
```

In this example, the macaroon has four caveats, each identified by the
`cid` prefix.  These four caveats values are `iid:GaVltWFP`,
`id:2002;2002,0;paul`, `before:2019-09-25T08:12:11.080Z` and
`home:/Users/paul`.

### Adding extra caveats

One benefit of macaroons is that it is possible to add additional
caveats to macaroon independent of dCache.  This allows some powerful
work-flows where an external agent requests a powerful macaroon and
generates more restricted macaroons "on demand".

The portal use-case provides an example of such a workflow.  A
web-portal should allow its users to download specific files if that
user is authorised to view that data, where these users are unknown to
dCache.  The portal requests a macaroon that is authorised to download
any file in dCache.  When a user requests downloading a file she is
authorised to read, the portal autonomously generate a macaroon that
is authorised to download that single file and redirects the user's
request to the URL with the embedded macaroon.  The result is an
architecture that allows users to download data at an almost arbitrary
throughput.

The python library above may be used to add caveats to an existing
macaroon.  The macaroon is de-serialised, the additional caveats are
added and the resulting macaroon is serialised.

### Understanding dCache caveats

Whenever dCache issues a macaroon there are always some caveats.  When
asking dCache for a macaroon, the request may ask that additional
caveats be included.  Although extra caveats may be added to a
macaroon directly, it is somewhat safer to have dCache add the caveats
as this avoids dCache returning an unnecessarily powerful macaroon.

In general dCache caveats values have the format `KEY:VALUE`; e.g.,
the caveat `before:2019-09-25T08:12:11.080Z` has a key of `before` and
value `2019-09-25T08:12:11.080Z`.

The following table lists the available caveat keys along with the
value format and what it means.

<dt>
<dt><tt>iid</tt></dt>

<dd>The value is a BASE64 value.  This caveat is added automatically
by dCache and is the first caveat.  This is the Issue ID: a random
number that (with high likelihood) uniquely identifies the request
that issued this macaroon.  A macaroon can have at most one
<tt>iid</tt> caveat: adding an additional <tt>iid</tt> caveat renders
the macaroon invalid.</dd>

<dt><tt>id</tt></dt>

<dd>The value is a semi-colon-separated list of items: the uid, a
comma-separated list containing the primary gid and any other gids,
and the username.  This caveat identifies the user the requested the
macaroon.  This caveat is used to make authorisation decisions and to
identify the ownership of created items (e.g., uploaded files, created
directories).  This macaroon is added by dCache automatically.  A
macaroon has exactly one <tt>id</tt> caveat: adding an additional
<tt>id</tt> caveat renders the macaroon invalid.</dd>

<dt><tt>before</tt></dt>

<dd> The value is an ISO 8601 instant.  This caveat limits when the
macaroon is valid.  The macaroon is only valid before the specified
instant.  It is valid to have multiple <tt>before</tt> caveats.  The
macaroon only valid when all <tt>before</tt> caveats are valid.  </dd>

<dt><tt>path</tt></dt>

<dd>The value is an absolute path within dCache.  This caveat limits
which part of the namespace is visible to users.  Ancestor directories
are accessible as are all children of path.  All other path items are
not accessible.  In addition, non-accessible items are excluded from
directory listing.  Multiple path caveats are allowed and have
cumulative effect, where a subsequent path caveats is resolved
relative to previous path caveats; e.g., a macaroon with two path
caveats `/foo` and `/bar` behaves the same as a macaroon with a single
path caveat `/foo/bar`.</dd>

<dt><tt>activity</tt></dt>

<dd>The value is a comma-separated list of enumerated values.  An
activity caveat limits what operations are allowed.  The following
activities are defined: <tt>LIST</tt> obtain directory lists,
<tt>UPLOAD</tt> create new files, <tt>DOWNLOAD</tt> obtain files'
data, <tt>DELETE</tt> delete a file or overwrite an existing file,
<tt>MANAGE</tt> rename and move files, <tt>READ_METADATA</tt> obtain
file metadata, <tt>UPDATE_METADATA</tt> modify file metadata.  The
<tt>READ_METADATA</tt> is implied if any other activity is specified.
For example, the caveat <tt>activity:LIST,DOWNLOAD</tt> restricts the
macaroon to read-only operations.  Multiple activity caveats are
supported.  A request must satisfy all activity caveats to be
accepted.</dd>

<dt><tt>home</tt></dt>

<dd> The value is an absolute path in dCache.  This caveat is added
automatically by dCache.</dd>

<dt><tt>ip</tt></dt>

<dd> The value is a comma-separated list of IPv4 or IPv6 addresses or
subnets in CIDR format.  If specified, client requests are only
accepted if they come from one of the caveat's listed addresses or
from within one of the listed subnets.  Multiple caveats are accepted.
A request is only accepted if it satisfies all <tt>ip</tt>
caveats.</dd>

<dt><tt>root</tt></dt>

<dd> The value is an absolute path within dCache.  All paths are first
resolved against the root path.  This makes the root path a prefix for
all requests.  Multiple caveats are allowed, with the effective root
being the combination of the <tt>root</tt> caveats; e.g., a macaroon
with two root caveats `/foo` and `/bar` is equivalent to a macaroon
with the single <tt>root</tt> caveat `/foo/bar`.</dd>

</dt>

### Requesting a macaroon with caveats

The POST request may include a JSON object providing information about
the desired macaroon.  The `caveats` key may be supplied.  The value
is a JSON list of JSON strings.  Each string is a caveat to be
included in the macaroon.

For example, the following JSON requests the generated macaroon
contain the caveat `activity:LIST,DOWNLOAD`, which limits the macaroon
to read-only operations.

```json
{
  "caveats": [
    "activity:LIST,DOWNLOAD"
  ]
}
```

The corresponding curl command is:

```console-user
curl -E /tmp/x509up_u1000 -X POST -H 'Content-Type: application/macaroon-request' -d '{"caveats": ["activity:LIST,DOWNLOAD"]}' https://dcache.example.org/
|{
|    "macaroon": "MDAxY2[...]XmCg",
|    "uri": {
|        "targetWithMacaroon": "https://dcache.example.org/?authz=MDAxY2[...]XmCg",
|        "baseWithMacaroon": "https://dcache.example.org/?authz=MDAxY2[...]XmCg",
|        "target": "https://dcache.example.org/",
|        "base": "https://dcache.example.org/"
|    }
|}
```

Often, it is useful to generate a macaroon that expires some fixed
period in the future; for example, to generate a macaroon that expires
in five minutes.

One way to achieve this is to calculate the instant five minutes in
the future, convert this into ISO 8601 format and include the caveat
in the macaroon request as one of the desired caveats.  For example,
if the current time is 12:00:00 CEST on Tuesday 24th September 2019
then the caveat would be `before:2019-09-24T10:05:00Z`.

To request a read-only macaroon that is valid for five minutes, The
request JSON object would look like:

```json
{
  "caveats": [
    "activity:LIST,DOWNLOAD",
    "before:2019-09-24T10:05:00Z"
  ]
}
```

The corresponding curl command is:

```console-user
curl -E /tmp/x509up_u1000 -X POST -H 'Content-Type: application/macaroon-request' -d '{"caveats": ["activity:LIST,DOWNLOAD", "before:2019-09-24T10:05:00Z"]}' https://dcache.example.org/
|{
|    "macaroon": "MDAxY2[...]yRgK",
|    "uri": {
|        "targetWithMacaroon": "https://dcache.example.org/?authz=MDAxY2[...]yRgK",
|        "baseWithMacaroon": "https://dcache.example.org/?authz=MDAxY2[...]yRgK",
|        "target": "https://dcache.example.org/",
|        "base": "https://dcache.example.org/"
|    }
|}
```

As a short-cut, you can request a macaroon with a specific validity,
such as five minutes.  dCache will calculate the corresponding period
and add the corresponding `before` caveat.  This is added as the
`validity` key in the request JSON object.  The value is an ISO 8601
value describing the validity period; for example, five minutes is
expressed as `PT5M` in ISO 8601.

The following object requesting a read-only macaroon that is only
valid for the next five minutes:

```json
{
  "caveats": [
    "activity:LIST,DOWNLOAD"
  ],
  "validity": "PT5M"
}
```

Here is the corresponding curl command:

```console-user
curl -E /tmp/x509up_u1000 -X POST -H 'Content-Type: application/macaroon-request' -d '{"caveats": ["activity:LIST,DOWNLOAD"], "validity": "PT5M"}' https://dcache.example.org/
|{
|    "macaroon": "MDAxY2[...]5bCg",
|    "uri": {
|        "targetWithMacaroon": "https://dcache.example.org/?authz=MDAxY2[...]5bCg",
|        "baseWithMacaroon": "https://dcache.example.org/?authz=MDAxY2[...]5bCg",
|        "target": "https://dcache.example.org/",
|        "base": "https://dcache.example.org/"
|    }
|}
```

The final short-cut is for providing a `path` caveat.  Supposing you
wish to allow a user to download a specific file, or files within a
specific directory.  This may be achieved by including the `path`
caveat in the macaroon request object.

For example, to allow users to download the specific file
`/users/paul/data-2019/top-secret.dat`, the following JSON object may
be supplied to the macaroon request:

```json
{
  "caveats": [
    "activity:LIST,DOWNLOAD",
    "path:/users/paul/data-2019/top-secret.dat"
  ]
}
```

The corresponding curl request would look like:

```console-user
curl -E /tmp/x509up_u1000 -X POST -H 'Content-Type: application/macaroon-request' -d '{"caveats": ["activity:LIST,DOWNLOAD", "path:/users/paul/data-2019/top-secret.dat"]}' https://dcache.example.org/
|{
|    "macaroon": "MDAxY2[...]tmIK",
|    "uri": {
|        "targetWithMacaroon": "https://dcache.example.org/?authz=MDAxY2[...]tmIK",
|        "baseWithMacaroon": "https://dcache.example.org/?authz=MDAxY2[...]tmIK",
|        "target": "https://dcache.example.org/",
|        "base": "https://dcache.example.org/"
|    }
|}
```

As a convenient short-cut, the desired path may be included as the
path of the macaroon request URL.

For example, the same request may be achieved by sending the POST
request to
`https://dcache.example.org/users/paul/data-2019/top-secret.dat`
instead of `https://dcache.example.org/`.

```console-user
curl -s -E /tmp/x509up_u1000 -X POST -H 'Content-Type: application/macaroon-request' -d '{"caveats": ["activity:LIST,DOWNLOAD"]}' https://dcache.example.org/users/paul/data-2019/top-secret.dat
|{
|    "macaroon": "MDAzY2[...]JeQo",
|    "uri": {
|        "targetWithMacaroon": "https://dcache.example.org/users/paul/data-2019/top-secret.dat?authz=MDAzY2[...]JeQo",
|        "baseWithMacaroon": "https://dcache.example.org/?authz=MDAzY2[...]JeQo",
|        "target": "https://dcache.example.org/users/paul/data-2019/top-secret.dat",
|        "base": "https://dcache.example.org/"
|    }
|}
```

Note that the `target` and `targetWithMacaroon` URLs in the response
JSON object have changed.  In particular, the `targetWithMacaroon` URL
may be used directly to download the desired file.


## Third-party transfers

Third-party transfers are requests to dCache, asking that it transfers
a file with another HTTP server.  This differs from normal HTTP
interactions as a third-party transfer involves data transferred
directly between dCache and some other HTTP server (which might or
might not be running dCache).

Third-party transfers are useful when transferring data as it uses the
network between the source and destination storage systems.  This is
often well provisioned, with significant available bandwidth.  As an
example, a laptop connected via a coffee shop's free wifi can easily
orchestrate the transfer of petabytes of data using third-party
transfers.

Third-party transfer requests are distinguished into two groups: *pull
requests* where dCache is downloading data from the remote server, and
*push requests* where dCache is uploading data to the remote server.

To initiate a third-party transfer, the client issues a `COPY` request
with the remote URL as either the `Source` request header (for a pull
request) or the `Destination` request header (for a push request).  In
either case, the header value is a URL, which describes how data is
transferred, the name of the remote server, the port number (if
non-standard) and the path of the file.

The COPY request may optionally include other headers that affect the
transfer.  These other headers are described in subsequent sections.

### Authenticating the third-party request

All authentication schemes that dCache supports for direct WebDAV
operations are also supported for third-party transfers.

### Authorising the third-party request

> **IMPORTANT**
>

> This section discusses how dCache handles third-party COPY requests:
> requests to initiate a third-party copy.  There is a separate issue
> where dCache must somehow authorise the data-bearing transfer.  This
> is discussed in a [subsequent
> section](#authorising-the-data-transfer).

Third-party transfers are only allowed for authenticated users:
anonymous third-party COPY requests are always rejected.

In general, third-party transfer authorisation may be understood by
considering what were to happen if the client were to relay the data.
In principal, a client can achieve the same result as a third-party
transfer by downloading the data from the source and uploading that
data to the destination.

A third-party transfer request that pulls data from the remote server
is authorised in a similar manner that user attempting to upload the
data itself.  To initiate a third-party pull request, the user must be
authorised to write into the target directory.  If the target file
already exists then the user must be authorised to overwrite that
existing data.

A third-party transfer request that pushes data to the remote server
is authorised in a similar manner to the client downloading the data:
it requires that the client is able to read the source file.

If the user is not authorised to make a specific third-party transfer
request then dCache returns immediate with an error status code.

In the following example, the client is attempting to initiate a pull
request without authenticating:

```console-user
curl -D- -X COPY -H 'Source: http://www.dcache.org/images/dcache-banner.png' https://dcache.example.org/test.png
|HTTP/1.1 401 Permission denied
|Date: Wed, 25 Sep 2019 08:30:04 GMT
|Server: dCache/6.0.0-SNAPSHOT
|WWW-Authenticate: Basic realm=""
|Content-Length: 0
|
```

### HTTP response

If the third-party transfer request is authorised and it passes some
basic checks (e.g., exactly one of the `Source` and `Destination`
request headers are present) then dCache will respond immediately with
a 202 status code.  This indicates that work has started to process
the request, but dCache will continue working on this request in the
background.

A transfer may take some time to complete.  While working on the
request, dCache will return periodic reports (called performance
markers) describing the current status, as part of the HTTP response.
The HTTP response is sent using chunked encoding.  This allows the
client to receive these reports in a timely fashion, to get feedback
as the transfer is processed.

Each performance marker has a strict format.  Each report contains
multiple lines: the first line is `Perf Marker`, followed by multiple
metadata lines, ending with the line containing only `End`.  Each
metadata line represents a key-value pair, printed as the key,
followed by a colon-space (`: `), followed by the current value.

There are two phases to any transfer: the pre-transfer phase and the
transfer phase.

#### Pre-transfer phase

During the pre-transfer phase, dCache is readying itself for the
transfer.

For pull-requests, this involves creating the namespace entry,
deciding which pool will accept the new data.  There are also
potential internal failures, which trigger internal retries.

For push-requests, the pool that will deliver the file's contents is
selected.  Depending on dCache configuration, it is possible that the
selected pool does not currently contain the file's data.  In this
case, dCache will initiate an internal copy of the file's data, which
may take some time.

During the pre-transfer phase, dCache returns progress markers that
look like this:

```
Perf Marker
    Timestamp: 1569399772
    State: 3
    State description: querying created file metadata
    Stripe Index: 0
    Total Stripe Count: 1
End
```

These metadata items have the following meaning:

| Key               | Value     | Meaning
| ----------------- | --------- | -------
| Timestamp         | UNIX time | When the transfer was accepted
| State             | Integer   | A machine-readable description of the current status
| State description | String    | A human-readable description of the current status
| Stripe Index      | `0`       | Included for compatibility with other software
| Total Strip Count | `1`       | Included for compatibility with other software

#### Transfer phase

Once all the preparation steps of the pre-transfer phase has
completed, the pool will attempt to make an HTTP transfer.  During
this transfer phase, the transfer is in state `10` (described as
`transfer has started`).

While the transfer is underway, some additional metadata is included
in the performance markers:

Here is a typical performance marker:

```
Perf Marker
    Timestamp: 1569403183
    State: 10
    State description: transfer has started
    Stripe Index: 0
    Stripe Start Time: 1569403178
    Stripe Last Transferred: 1569403183
    Stripe Transfer Time: 4
    Stripe Bytes Transferred: 503928392
    Stripe Status: RUNNING
    Total Stripe Count: 1
End
```

The fields `Timestamp`, `State`, `State description`, `Stripe Index`
and `Total Stripe Count` are still present and have the same meaning
as for the pre-transfer phase progress markers.

The additional metadata items have the following meaning:

| Key                      | Value       | Meaning
| ------------------------ | ----------- | -------
| Stripe Start Time        | UNIX time   | When the transfer was started
| Stripe Last Transferred  | UNIX time   | When data was last send or received
| Stripe Transfer Time     | Seconds     | How long the transfer has been running
| Stripe Bytes Transferred | Bytes       | How many bytes have been transferred
| Stripe Status            | enumeration | Current status of the transfer

A large discrepancy between `Timestamp` and `Stripe Last Transferred`
indicates that the remote server has stopped accepting data (for push
requests) or stopped sending data (for pull requests).

The client can establish the average transfer bandwidth between two
performance markers by comparing the two `Stripe Bytes Transferred`
values.  Advanced clients may use this to detect stalled transfers.

The `Stripe Status` value is one of `NEW`, `QUEUED`, `RUNNING`,
`DONE`, `CANCELED`.  As the transfer only remains in state `NEW`,
`DONE` and `CANCELED` for a very short period, you should only see
transfers in state `QUEUED` or in state `RUNNING`.  `QUEUED` indicates
the transfer is queued on the pool, while `RUNNING` indicates the
transfer is now being processed.

#### Final result

The final line of the transfer describes whether or not the transfer
was successful.  If the transfer was successful then the final line is
`success: Created`.  If the transfer was unsuccessful then the final
line starts `failure: ` followed by a description of why the transfer
failed.

For example, the final line `failure: rejected GET: 404 Not Found`
indicates a pull request attempted to copy a file that does not exist.
The final line `failure: rejected GET: 401 Unauthorized` indicates
dCache was not authorised to read the remote file.

#### Response examples

The following provides a complete example of the response when making a
successful pull request.  It includes both the HTTP response headers
and the HTTP response body.  The body contains two progress markers
(one in the pre-transfer phase, the other transfer phase) and the
final line indicating the transfer was successful.

```
HTTP/1.1 202 Accepted
Date: Wed, 25 Sep 2019 09:43:22 GMT
Server: dCache/5.2.0-SNAPSHOT
Content-Type: text/perf-marker-stream
Transfer-Encoding: chunked

Perf Marker
    Timestamp: 1569404602
    State: 3
    State description: querying created file metadata
    Stripe Index: 0
    Total Stripe Count: 1
End
Perf Marker
    Timestamp: 1569404607
    State: 10
    State description: transfer has started
    Stripe Index: 0
    Stripe Start Time: 1569404602
    Stripe Last Transferred: 1569404607
    Stripe Transfer Time: 4
    Stripe Bytes Transferred: 531501280
    Stripe Status: RUNNING
    Total Stripe Count: 1
End
success: Created
```

### Data integrity

dCache takes data integrity very seriously.  This includes
transferring data with remote servers.  Under normal circumstances,
dCache will only consider a third-party transfer successful if the
data-bearing HTTP request (either a `GET` for pull, or `PUT` for push)
indicates a success and the integrity of the new copy is verified.

There are two complementary ways to verify the new copy has not become
corrupted: checking the file size matches and the file checksum
matches.

For pull requests, the file's size is normally included in the
response headers of the `GET` request; however, for push requests the
response headers to the `PUT` request usually do not contain the
file's size.  Therefore, a subsequent `HEAD` request is often needed
after a successful `PUT` request.

The checksum of the remote file is obtained using RFC 3230
`Want-Digest` headers.  For pull requests, this header is included in
the `GET` request; however, for the `PUT` request the `Want-Digest`
header is included in the subsequent `HEAD` request.

Although dCache supports RFC 3230, the standard is not widely
supported by other HTTP servers.  Therefore, when transferring data
with a non-dCache remote server, it is likely that the server does not
support RFC 3230.  By default, dCache will fail the transfer if the
remote server does not support RFC 3230; however, if it is desirable
to accept transfers without checksum verification then the
`RequireChecksumVerification` COPY request header may be set to
`false`.  When setting this flag to `false`, dCache will still attempt
to verify the remote file's checksum and the transfer will fail if
that remote checksum indicates data corruption.


### Custom transfer headers

HTTP requests may contain different request headers.  When dCache is
processing a third-party transfer, it makes one or more requests with
the remote server.  When making these requests, dCache will include a
standard set of transfer headers.  However, you can modify these
transfer headers.

The general rule is that any header in the third-party (COPY) request
that starts with a `TransferHeader` prefix is used when dCache is
making the request without this prefix.  For example, if the COPY
request contains the header `TransferHeaderClientContext: foo` then
dCache will include the header `ClientContext: foo` when making
requests to the remote server.

Perhaps the main use for this feature is to include authorisation
information for the data bearing request.  For example, to include the
basic authentication (`Authorization: Basic
cGF1bDpUb29NYW55U2VjcmV0YQ==`) in the data-bearing request, the COPY
request should include the `TransferHeaderAuthorization` header
(`TransferHeaderAuthorization: Basic cGF1bDpUb29NYW55U2VjcmV0YQ==`).

### Authorising the data transfer

Often a server will require some form of authentication before
accepting an upload (PUT) request.  Similarly, some data is not public
and requires authentication before a downloaded (GET) request is
accepted.

As a direct consequence, when processing a third-party transfer,
dCache may need to authenticate or provide some kind of authorisation
in order to obtain the data (for pull requests) or upload data (for
push requests).

If needed, this authorisation must come from the client.  Delegation
is the general term for handing over credentials that allow dCache to
operate on behalf of the user.

There are several ways a client can delegate a credential to dCache.
The `Credential` third-party COPY request header controls where dCache
will fetch the credential.  A value of `none` indicates dCache should
not initiate any delegation, `gridsite` indicates dCache should use a
delegated X.509 credential, and `oidc` indicates dCache should use
OIDC delegation.

The default value for `Credential` depends on how the client
authenticated for the third-party COPY request.  If OIDC was used then
`oidc` is the default; if X.509 was used then the default is
`gridsite`, otherwise the default is `none`.

#### Direct header delegation

If `Credential` third-party COPY request header has value `none` then
either the request does not require any credential (e.g., downloading
public data) or the credential is supplied through direct header
delegation.

Direct header delegation is where the client supplies the credential
directly to dCache, as a transfer header.  As described above, this is
achieved by specifying the corresponding `TransferHeader` header in
the COPY request; for example, most requests are authorised by
specifying the `Authorization` request header value.  To supply a
suitable `Authorization` value, specify the
`TransferHeaderAuthorization` header in the COPY request.

##### Basic

Basic (username + password) authentication may be used to authorise
the transfer.  For example, to include the basic authentication
(`Authorization: Basic cGF1bDpUb29NYW55U2VjcmV0YQ==`) in the
data-bearing request, the COPY request should include the
`TransferHeaderAuthorization` header (`TransferHeaderAuthorization:
Basic cGF1bDpUb29NYW55U2VjcmV0YQ==`).

Basic authentication is NOT recommended as it requires the user to
send their username and password to dCache.

##### Bearer token

An alternative to basic authentication is to use some kind of a bearer
token to authorise the transfer; for example, if the data bearing
transfer may be authorised with the `Authorization: Bearer TOKEN`
header, then the third-party COPY request should include the
`TransferHeaderAuthorization: Bearer TOKEN` request header.

As a specific example, a macaroon may be used to authorise the
transfer.  The client requests a macaroon from the third-party server
that targets the specific file.  Once obtained, this macaroon may be
passed into the COPY request via the `TransferHeaderAuthorization:
Bearer MACAROON` (with `MACAROON` replaced with the actual macaroon).

#### X.509 delegation

dCache supports the GridSite delegation protocol.  This allows clients
to delegate their X.509 credential to dCache so dCache can operation
on their behalf.

If GridSite delegation is selected, dCache will check if the user has
already delegated a credential that will still be valid in two
minutes.  If so, it will use that credential and the transfer will
proceed directly.

If dCache has no credential for this user (or the credential has
already expired or will expire soon) then dCache will request the user
delegates.  This is done by responding to the COPY request with a
redirection status code (with a target URL in the `Location` response
header) and a `X-Delegate-To` response header.  The header value is a
space-separated list of GridSite delegation URLs.

The client is expected to delegate its X.509 credential to one of the
listed delegation URLs and re-issue the POST request against the URL
in the `Location` response header.


#### OpenID-Connect delegation

An OpenID-Connect access token may include a claim that means it
targets a specific server, or the access token could expire if the
transfer is queued within dCache.

To counter these two problems, the oidc delegation process works by
requesting a fresh access (and refresh) token from the OP that issued
the OpenID-Connect access token.

This delegation process is not uniformly supported.

### Complete example

In the following example, the client instructs dCache to create a new
file `/Users/paul/test-1`, taking the file's data from
`http://www.dcache.org/images/dcache-banner.png`.  Note that the COPY
request is authorised using an X.509 credential (`-E
/tmp/x509up_u1000`).

The source file (`http://www.dcache.org/images/dcache-banner.png`) is
public: dCache does not require any special permission to obtain this
file's data.  Therefore this third-party request requires no
delegation.

As the third-party COPY request is authenticated with X.509, the
`Credential: none` request header is needed to avoid triggering
gridsite delegation.

Finally, as the server supplying the file's data is a standard Apache
web server, it does not support RFC 3230.  Therefore, the client must
tell dCache not to fail the transfer if it cannot obtain a checksum to
verify the file's integrity.  The `RequireChecksumVerification: false`
request header is used to convey this.

```console-user
curl -D- -E /tmp/x509up_u1000 -X COPY -H 'Credential: none' -H 'RequireChecksumVerification: false' -H 'Source: http://www.dcache.org/images/dcache-banner.png' https://dcache.example.org/Users/paul/test-1
|HTTP/1.1 202 Accepted
|Date: Wed, 25 Sep 2019 08:22:26 GMT
|Server: dCache/6.0.0-SNAPSHOT
|Content-Type: text/perf-marker-stream
|Transfer-Encoding: chunked
|
|Perf Marker
|    Timestamp: 1569399772
|    State: 3
|    State description: querying created file metadata
|    Stripe Index: 0
|    Total Stripe Count: 1
|End
|success: Created
```

