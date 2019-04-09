Chapter 2. Frontend
===================

The frontend is an HTTP endpoint that provides a REST API.  REST is a
design principal, rather than a specific protocol, and the REST API
that frontend provides is non-standard.  This allows you to take
advantage of some dCache advance features that are not available
through other protocols.

In this chapter, we will assume that dCache is running a frontend
service on `dcache.example.org` on port `3880` with TLS encryption
enabled.  Therefore, all the example URLs will start
`https://dcache.example.org:3880/`

## Swagger

[Swagger](https://swagger.io/ "Swagger homepage") is a standard way of
describing a REST API using JSON.  In addition to providing online
documentation of the dCache API, it may also be used to build clients
in almost any language.

Each dCache frontend provides a swagger description of its API at the
path `/api/v1/swagger.json` (e.g.,
`https://dcache.example.org:3880/api/v1/swagger.json`).

In addition, dCache frontend comes bundled with the Swagger UI
application.  This application is a web page that uses your browser's
JavaScript support to download dCache's Swagger JSON description and
build a client for trying out the different dCache API calls.  You can
try the Swagger UI by pointing your browser at `/api/v1/`
(e.g,. `https://dcache.example.org:3880/api/v1/`).

You can find out more about Swagger UI at [Swagger UI home
page](https://swagger.io/tools/swagger-ui/).

## REST overview

All REST API calls start `/api/v1/`.  The next path element groups
together related API calls; for example `namespace`
(`/api/v1/namespace`) contains all API calls that operate on dCache's
namespace, `events` (`/api/v1/events`) contains the Server-Sent Events
support with its management interface, and `/user` (`/api/v1/user`)
contains information about user identities.

A number of API calls are intended for administrative operations and
require special privileges.  These API calls are not documented here,
but in a separate admin-focused book.

There seven groups of API calls that a user may wish to use: identity,
namespace, qos, space reservations, active transfers, events and
doors.  The following sections describe each of these.

## identity

The identity API calls are about someone's identity within dCache.
There is currently one API call: a GET request which allows you to
discover information about the user making the request.

If no credentials are presented then the user is the ANONYMOUS user:

```console
paul@sprocket:~$ curl https://dcache.example.org:3880/api/v1/user
{
  "status" : "ANONYMOUS"
}
paul@sprocket:~$ 
```

If the user authenticates, then information is returned:

```console
paul@sprocket:~$ curl -u paul https://dcache.example.org:3880/api/v1/user
Enter host password for user 'paul':
{
  "status" : "AUTHENTICATED",
  "uid" : 2002,
  "gids" : [ 2002, 0 ],
  "username" : "paul",
  "homeDirectory" : "/Users/paul",
  "rootDirectory" : "/"
}
paul@sprocket:~$ 
```

## Namespace

With the namespace part of the API, you can discover information about
a specific file or directory, list the contents of a directory, delete
and rename files, and modify a file's QoS.

### Discovering metadata and listing directory contents

To discover information about a path in dCache, make a GET request to
a URL formed by appending the dCache path to `/api/v1/namespace`.

The following example shows information about the root directory:

```console
paul@sprocket:~$ curl https://example.org:3880/api/v1/namespace/
{
  "fileMimeType" : "application/vnd.dcache.folder",
  "fileType" : "DIR",
  "pnfsId" : "000000000000000000000000000000000000",
  "nlink" : 11,
  "mtime" : 1554697387559,
  "creationTime" : 1554696069369,
  "size" : 512
}
paul@sprocket:~$ 
```

Here is a query to discover information about the `/upload` directory.

```console
paul@sprocket:~$ curl https://dcache.example.org:3880/api/v1/namespace/upload
{
  "fileMimeType" : "application/vnd.dcache.folder",
  "fileType" : "DIR",
  "pnfsId" : "00003405A2416C8D4317AA3833352F967A9A",
  "nlink" : 14,
  "mtime" : 1554726185595,
  "creationTime" : 1554697387559,
  "size" : 512
}
paul@sprocket:~$ 
```

If the path does not exist, then dCache returns an error:

```console
paul@sprocket:~$ curl -D- https://dcache.example.org:3880/api/v1/namespace/no-such-item
HTTP/1.1 404 Not Found
Date: Mon, 08 Apr 2019 21:55:48 GMT
Server: dCache/5.0.5
Content-Type: application/json
Access-Control-Allow-Origin: *
Access-Control-Allow-Methods: GET, POST, DELETE, PUT
Access-Control-Allow-Headers: Content-Type, Authorization, Suppress-WWW-Authenticate
Content-Length: 51

{"errors":[{"message":"Not Found","status":"404"}]}
paul@sprocket:~$ 
```

The HTTP request returns a 404 status code, with a JSON entity
containing the error.

To list the contents of a directory, include the query argument
`children=true`; for example, to list the root directory:

```console
paul@sprocket:~$ curl https://dcache.example.org:3880/api/v1/namespace/?children=true
{
  "fileMimeType" : "application/vnd.dcache.folder",
  "children" : [ {
    "fileName" : "lost+found",
    "fileMimeType" : "application/vnd.dcache.folder",
    "fileType" : "DIR",
    "pnfsId" : "000000000000000000000000000000000001",
    "nlink" : 2,
    "mtime" : 1554696070327,
    "creationTime" : 1554696070327,
    "size" : 512
  }, {
    "fileName" : "Users",
    "fileMimeType" : "application/vnd.dcache.folder",
    "fileType" : "DIR",
    "pnfsId" : "00007EF0F064738E420099E7BDA672500DC2",
    "nlink" : 30,
    "mtime" : 1554696093632,
    "creationTime" : 1554696089487,
    "size" : 512
  }, {
    "fileName" : "VOs",
    "fileMimeType" : "application/vnd.dcache.folder",
    "fileType" : "DIR",
    "pnfsId" : "0000F0C3D9A2EA9F4681970BF3D414A311ED",
    "nlink" : 15,
    "mtime" : 1554696092837,
    "creationTime" : 1554696089424,
    "size" : 512
  }, {
    "fileName" : "upload",
    "fileMimeType" : "application/vnd.dcache.folder",
    "fileType" : "DIR",
    "pnfsId" : "00003405A2416C8D4317AA3833352F967A9A",
    "nlink" : 14,
    "mtime" : 1554726185595,
    "creationTime" : 1554697387559,
    "size" : 512
  } ],
  "fileType" : "DIR",
  "pnfsId" : "000000000000000000000000000000000000",
  "nlink" : 11,
  "mtime" : 1554697387559,
  "creationTime" : 1554696069369,
  "size" : 512
}
paul@sprocket:~$ 
```

The response includes metadata about each of the children.  If the
target is not a directory then adding `children=true` has no effect on
the output.

### Deleting files and directories

A file or directory may be deleted by using the DELETE HTTP verb on
the corresponding path.  If the target is a directory then it must be
empty for the delete to work.

The following example shows deleting a file:

```console
paul@sprocket:~$ curl -u paul -D- -X DELETE https://dcache.example.org:3880/api/v1/namespace/Users/paul/test-1
Enter host password for user 'paul':
HTTP/1.1 200 OK
Date: Mon, 08 Apr 2019 21:59:47 GMT
Server: dCache/5.0.5
Content-Type: application/json
Access-Control-Allow-Origin: *
Access-Control-Allow-Methods: GET, POST, DELETE, PUT
Access-Control-Allow-Headers: Content-Type, Authorization, Suppress-WWW-Authenticate
Content-Length: 20

{"status":"success"}
paul@sprocket:~$ 
```

### Creating directories

A new directory may be created using a POST request to the containing
directory, with a JSON object contain the key `action` with value
`mkdir` and the `name` key containing the new directory's name.

```console
{"status":"success"}paul@sprocket:~$ ^C
paul@sprocket:~$ curl -u paul -X POST -H 'Content-Type: application/json' \
        -d '{"action":"mkdir", "name":"new-dir"}' \
        https://dcache.example.org:3880/api/v1/namespace/Users/paul
Enter host password for user 'paul':
{"status":"success"}
paul@sprocket:~$ 
```

### Moving and renaming

To rename a file or directory, make a POST request to the source file
or directory containing a JSON object with the `action` key with `mv`,
and the `destination` key with the path to the destination.  If the
destination path is relative then it is resolved agianst the request's
path parameter.

The following example renames `test-1` to `test-2`.

```console
paul@sprocket:~$ curl -u paul -X POST -H 'Content-Type: application/json' \
        -d '{"action":"mv", "destination":"test-2"}' \
        https://dcache.example.org:3880/api/v1/namespace/Users/paul/test-1
Enter host password for user 'paul':
{"status":"success"}
paul@sprocket:~$ 
```

### Modifying QoS

A file or directory has a corresponding QoS.  To modify this assigned
QoS, make a POST request with `action` of `qos` and `target` with the
target QoS.

The following modifies the file `/Users/paul/test-1` to have QoS
`tape`.

```console
paul@sprocket:~$ curl -u paul -X POST -H 'Content-Type: application/json' \
        -d '{"action":"qos", "target":"tape"}' \
        https://dcache.example.org:3880/api/v1/namespace/Users/paul/test-1
Enter host password for user 'paul':
{"status":"success"}
paul@sprocket:~$ 
```

More information about discovering QoS values may be found in the QoS
section.

## QoS

Discovering information about the available QoS options.

## Space reservations

The ability to manage space reservations.

## Active transfers

Discovering information about on-going transfers.

## Events

Support for storage events, where information is sent from dCache to
the client using the W3C standard: Server-Sent Events (SSE).

## Doors

Information on alternative network protocols.