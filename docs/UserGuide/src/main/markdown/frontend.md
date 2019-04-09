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

## QoS Management

The QoS management portion of the REST API (`/api/v1/qos-management`)
is about working with the different QoS options.  Discovering the
current QoS of a file or directory or modifying that value is done
within the namespace (`/api/v1/namespace`) portion of the REST API.

The `/api/v1/qos-management/qos` part of the REST API deals with the
different QoS options.

Files and directories in dCache have an associated QoS class.  The QoS
class for a file describes how that file is handled by dCache; for
example, what performance a user may reasonably expect when reading
from that file.  The QoS class for a directory describes what QoS
class a file will receive when it is written into that directory.
Currently, all directory QoS classes have an equivalent file QoS class
with the same name.

To see the available options for files, make a GET request on
`/api/v1/qos-management/qos/file`; e.g.,

```console
paul@sprocket:~$ curl -s -u paul https://dcache.example.org:3880/api/v1/qos-management/qos/file | jq .
Enter host password for user 'paul':
{
  "name": [
    "disk",
    "tape",
    "disk+tape",
    "volatile"
  ],
  "message": "successful",
  "status": "200"
}
paul@sprocket:~$
```

In a similar way, the available QoS options for directories may be
found with a GET query to `/api/v1/qos-management/qos/directory`:

```console
paul@sprocket:~$ curl -s -u paul https://dcache.example.org:3880/api/v1/qos-management/qos/directory | jq .
Enter host password for user 'paul':
{
  "name": [
    "disk",
    "tape",
    "disk+tape",
    "volatile"
  ],
  "message": "successful",
  "status": "200"
}
paul@sprocket:~$
```

In both cases, the returned JSON lists the labels for the various QoS
classes.

Detailed information about a specific QoS label is available by making
a GET request against the path appended with the QoS label.  For
example, a GET request to `/api/v1/qos-management/qos/file/disk`
provides information about the `disk` QoS class:

```console
paul@sprocket:~$  curl -s -u paul https://dcache.example.org:3880/api/v1/qos-management/qos/file/disk | jq .
Enter host password for user 'paul':
{
  "status": "200",
  "message": "successful",
  "backendCapability": {
    "name": "disk",
    "transition": [
      "tape",
      "disk+tape"
    ],
    "metadata": {
      "cdmi_data_redundancy_provided": "1",
      "cdmi_geographic_placement_provided": [
        "DE"
      ],
      "cdmi_latency_provided": "100"
    }
  }
}
paul@sprocket:~$
```

The returned JSON provides two groups of information.

The `transition` list shows all allowed transitions if a file
currently has the QoS class `disk`.  In this example, the transition
from QoS class `disk` to QoS class `volatile` is not allowed.

The `metadata` object contains information about the `disk` QoS class.
The three values in this example (`cdmi_data_redundancy_provided`,
`cdmi_geographic_placement_provided` and `cdmi_latency_provided`) come
from the [CDMI specification](https://www.snia.org/cdmi "Cloud Data
Management Interface (CDMI)"), see section 16.5.

## Space reservations

Space reservations are a promise to store a given amount of data.
They may be used when uploading a reasonable sized dataset, to avoid
running out of storage space midway through the upload.

The `/api/v1/space` is used when interacting with dCache's space
reservation support.  A GET request on the `tokens` resource provides
access to the list of available tokens.  By default, this lists all
space reservations; e.g.,

```console
paul@sprocket:~$ curl -s https://dcache.example.org:3880/api/v1/space/tokens | jq .
[
  {
    "id": 2,
    "voGroup": "/atlas",
    "retentionPolicy": "REPLICA",
    "accessLatency": "ONLINE",
    "linkGroupId": 2,
    "sizeInBytes": 268435456000,
    "creationTime": 1554782633504,
    "description": "ATLASSCRATCHDISK",
    "state": "RESERVED"
  },
  {
    "id": 3,
    "voGroup": "/atlas",
    "retentionPolicy": "REPLICA",
    "accessLatency": "ONLINE",
    "linkGroupId": 2,
    "sizeInBytes": 107374182400,
    "creationTime": 1554782633548,
    "description": "ATLASDATADISK",
    "state": "RESERVED"
  }
]
paul@sprocket:~$
```

Query parameters in the URL limit the reservations that are returned;
for example, `accessLatency=ONLINE` limits the response to those
reservations with online access-latency and `voGroup=/atlas` limits
the response to those reservations with `/atlas` ownership.

The following limits are allowed:

| Name | Select only reservations... |
| ---- | ------ |
|  id  | with this id. |
| voGroup | with this VO group. |
| voRole | with this VO role. |
| accessLatency | with this Access Latency. |
| retentionPolicy | with this Retention Policy. |
| groupId | created from a linkgroup with this id. |
| state | with this current state. |
| minSize | with a capacity (in bytes) larger than this capacity  |
| minFreeSpace | with a free capacity (in bytes) larger than this capacuty |

Multiple filters may be combined in a single query, with the effects
being accumulative: each (potentially) reducing the number of
reservations listed.

Here is an example illustrating those space reservations that satisfy
two filters: the VO group is `/atlas` and the reserved capacity is at
least 200 GB.

```console
paul@sprocket:~$ curl -s 'https://dcache.example.org:3880/api/v1/space/tokens?voGroup=/atlas&minSize=200000000000' | jq .
[
  {
    "id": 2,
    "voGroup": "/atlas",
    "retentionPolicy": "REPLICA",
    "accessLatency": "ONLINE",
    "linkGroupId": 2,
    "sizeInBytes": 268435456000,
    "creationTime": 1554782633504,
    "description": "ATLASSCRATCHDISK",
    "state": "RESERVED"
  }
]
paul@sprocket:~$
```

## Active transfers

The `transfers` resource (`/api/v1/transfers`) provides information
about ongoing transfers.  It does this by generating a snapshot of the
active transfers every minute.  Without any additional options, a GET
request returns information from the latest snapshot; e.g.,

```console
paul@sprocket:~$ curl -s -u paul https://dcache.example.org:3880/api/v1/transfers | jq .
Enter host password for user 'paul':
{
  "items": [],
  "currentOffset": 0,
  "nextOffset": -1,
  "currentToken": "e4c5759b-f9b3-4165-9bd5-074b8e022596",
  "timeOfCreation": 1554815277797
}
paul@sprocket:~$
```

In this example, there are no active transfers (`items` is empty).
The `currentOffset` and `nextOffset` indicate that all available data
is shown.  The `currentToken` is a unique reference for this snapshot
that may be used later.  Finally, the `timeOfCreation` gives the Unix
time (in milliseconds) when this snapshot was created.

If there are on-going transfers when the snapshot is created then the
output is different.


```console
paul@sprocket:~$ curl -s -u paul https://dcache.example.org:3880/api/v1/transfers | jq .
Enter host password for user 'paul':
{
  "items": [
    {
      "cellName": "webdav-secure-grid",
      "domainName": "dCacheDomain",
      "serialId": 1554815749796000,
      "protocol": "HTTP-1.1",
      "process": "",
      "pnfsId": "0000AC8078894C74493B8F1BE11DC2672D9C",
      "path": "/Users/paul/test-1",
      "pool": "pool2",
      "replyHost": "2001:638:700:20d6:0:0:1:3a",
      "sessionStatus": "Mover PoolName=pool2 PoolAddress=pool2@pools/641: Waiting for completion",
      "waitingSince": 1554815749796,
      "moverStatus": "RUNNING",
      "transferTime": 8021,
      "bytesTransferred": 16384,
      "moverId": 641,
      "moverSubmit": 1554815749839,
      "moverStart": 1554815749839,
      "subject": {
        "principals": [
          {
            "primaryGroup": false,
            "gid": 0,
            "name": "0"
          },
          {
            "clientChain": [
              "2001:638:700:20d6:0:0:1:3a"
            ],
            "address": "2001:638:700:20d6:0:0:1:3a",
            "name": "2001:638:700:20d6::1:3a"
          },
          {
            "name": "paul.millar@desy.de"
          },
          {
            "primaryGroup": true,
            "gid": 1001,
            "name": "1001"
          },
          {
            "name": "/C=DE/O=GermanGrid/OU=DESY/CN=Alexander Paul Millar"
          },
          {
            "uid": 2002,
            "name": "2002"
          },
          {
            "primaryGroup": false,
            "gid": 2002,
            "name": "2002"
          },
          {
            "primaryGroup": false,
            "name": "dteam"
          },
          {
            "fqan": {
              "group": "/dteam",
              "capability": "",
              "role": ""
            },
            "primaryGroup": true,
            "name": "/dteam"
          },
          {
            "name": "paul"
          },
          {
            "loA": "IGTF_AP_CLASSIC",
            "name": "IGTF-AP:Classic"
          }
        ],
        "readOnly": false,
        "publicCredentials": [],
        "privateCredentials": []
      },
      "userInfo": {
        "username": "paul",
        "uid": "2002",
        "gid": "1001",
        "primaryFqan": {
          "group": "/dteam",
          "capability": "",
          "role": ""
        },
        "primaryVOMSGroup": "/dteam"
      },
      "valid": true,
      "uid": "2002",
      "gid": "1001",
      "transferRate": 2,
      "vomsGroup": "/dteam",
      "timeWaiting": "0+00:00:09"
    }
  ],
  "currentOffset": 0,
  "nextOffset": -1,
  "currentToken": "fbfd7d39-9959-4135-bf9a-5f65355346f5",
  "timeOfCreation": 1554815757860
}
paul@sprocket:~$
```

The format describing a transfer is not yet fixed and may be subject
to change.

### Filtering

The list of active transfers may be limited by specifying different
query parameters.

The following filters are supported:

| Name | Select only transfers... |
| ---- | --- |
| state | in state |
| door | initiated with this door |
| domain | inititated in this specific domain |
| prot | using the named protocol |
| uid | initiated by this user |
| gid | initiated by members of this group |
| vomsgroup | initiated by a member of this voms group |
| path | transfers involving this path |
| pnfsid | transfers involving this PNFS-ID |
| client | transfers involving this client |

An an example, the query `/api/v1/transfers?uid=1000&prot=HTTP-1.1`
would list all current HTTP transfers involving the user with uid
1000.

### Pagination

On an active dCache, there may be many concurrent transfers: more than
may be returned in one JSON response.  To support this, a query can
target a specific snapshot (rather than the latest snapshot) selecting
different subsets of all concurrent transfers.

The `token` query parameter may be used to select a specific snapshot.
The value is the `currentToken` value.  In the above example, the
currentToken value is `fbfd7d39-9959-4135-bf9a-5f65355346f5`, so
repeated queries that target this snapshot have
`token=fbfd7d39-9959-4135-bf9a-5f65355346f5` as a query parameter.

The `offset` and `limit` query parameters select a subset of values.
If not specified then they default to 0 and 2147483647 respectively.

Therefore, a GET request to
`/api/v1/transfers?token=fbfd7d39-9959-4135-bf9a-5f65355346f5&offset=0&limit=10`
returns the first ten transfers, a GET request to
`/api/v1/transfers?token=fbfd7d39-9959-4135-bf9a-5f65355346f5&offset=10&limit=10`
returns the next ten transfers, and so on.

### Sorting

The output is sorted.  The priority of different fields is controlled
by the `sort` query parameter, which takes a comma-separate list of
field names.  The default value is `door,waiting`.

## Events

Support for storage events, where information is sent from dCache to
the client using the W3C standard: Server-Sent Events (SSE).

## Doors

Information on alternative network protocols.