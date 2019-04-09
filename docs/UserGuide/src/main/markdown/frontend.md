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

## Storage Events

Storage events is a mechanism where dCache can let you know when
something of interest has happened.  To do this, dCache uses a W3C
standard protocol: Server-Sent Events (SSE).

SSE is a protocol that allows an HTTP client to receive events with a
minimal delay.  It is widely supported, with libraries existing in all
major languages in addition built-in support in all major web
browsers.

All REST activity to do with storage events target `events` resources
(`/api/v1/events`) with different resources below this having more
specific roles.

### Management overview

Although the SSE protocol is a standard, there is no standard
management interface to allow you to control which events you are
interested in receiving.  Therefore, dCache has a proprietary
interface that allows you to discover what possibilities exist,
discover the current configuration and modify that configuration.

The SSE protocol targets a specific endpoint.  In dCache, this
endpoint is called the channel.  It is expected that each client has
its own channel: channels are not shared.  A client will create its
own channel and then configures this channel to receive all events the
client is interested in.

Although it is not forbidden, a client could create multiple channels.
However, this is both unnecessary and discouraged as each user is
allowed only a limited number of channels.

Storage events are grouped together into broadly similar types, called
event types; for example, all events that simulate the Linux
filesystem notification system "inotify" have the `inotify` event
type.  Events that are to do with SSE support itself (or other
low-level aspects) have the `SYSTEM` event type.

When a client creates a channel, it initially receives only `SYSTEM`
events, which are generally not of interest.  To start receiving
interesting events, the client must create subscriptions.  A
subscription is just how a client describes which events of a specific
event type are of interest.

A subscription has two parts: the name of the event type and a
selector.  The selector is a JSON object that describes which events
(of all possible events) are of interest.  The exact format of a
selector depends on the event type.

A channel can have multiple subscriptions.  Each subscription is
independent: they could come from the same event type, or from
different event types.  A client can add and remove subscriptions as
needed.

### Top-level information

A GET request on the `/api/v1/events` resource provide information
that is independent of any channel and any event type.

```console
paul@sprocket:~$ curl -s -u paul https://dcache.example.org:3880/api/v1/events | jq .
Enter host password for user 'paul':
{
  "channels": {
    "lifetimeWhenDisconnected": {
      "maximum": 86400,
      "minimum": 1,
      "default": 300
    },
    "maximumPerUser": 128
  }
}
paul@sprocket:~$
```

The `channels` object describes information about channels in general,
rather than about a specific channel.  Channels are automatically
deleted if they are not used for long enough.  The time before this
happens may be adjusted by the client.  The `lifetimeWhenDisconnected`
describes this policy, with the values in seconds.

In the above example, new channels are garbage collected automatically
after five minutes.  An individual channel may be configured to be
garbage collected on a different schedule, as quickly as after one
second, or as long as after a day.

### Understanding event types

The `eventTypes` resource (`/api/v1/events/eventTypes`) describes
information about different events types, independent of any
subscriptions.

A GET request against this resource provides a list of available event
types:

```console
paul@sprocket:~$ curl -s -u paul https://dcache.example.org:3880/api/v1/events/eventTypes | jq .
Enter host password for user 'paul':
[
  "inotify",
  "metronome"
]
paul@sprocket:~$
```

In the above example, two event types are shown: `inotify` and
`metronome`.  The `SYSTEM` is not shown since a channel is always
subscribed to this event type and cannot control the delivery of those
events.

To learn more about an event type, the event type name is appended to
the path.  For example, the resource about the `metronome` event type
is `events/eventTypes/metronome`
(`/api/v1/events/eventTypes/metronome`).

A GET request on this resource provides generic information about this
event type:

```console
paul@sprocket:~$ curl -s -u paul https://dcache.example.org:3880/api/v1/events/eventTypes/metronome | jq .
Enter host password for user 'paul':
{
  "description": "a configurable stream of messages"
}
paul@sprocket:~$
```

There are two further useful documents about an event type: one that
describes selectors and one that describes the events of this event
type.

#### Selectors

The `selector` resource (`/api/v1/events/eventTypes/<name>/selector`)
provides a JSON Schema description of the selector.  When creating a
subscription a selector is used to describe which events are of
interest.

A GET request on this resource returns a JSON Schema.  When
subscribing to this event type, the selector must satisfy this JSON
Schema.  In addition to describing the structure, it also describes
the semantics of each of the arguments, including any default values
that are used if not specified.

Here is the selector for the metronome event type:

```console
paul@sprocket:~$ curl -s -u paul https://dcache.example.org:3880/api/v1/events/eventTypes/metronome/selector | jq .
Enter host password for user 'paul':
{
  "$id": "http://dcache.org/frontend/events/metronomeSelectors#",
  "$schema": "http://json-schema.org/draft-06/schema#",
  "type": "object",
  "properties": {
    "frequency": {
      "title": "The trigger frequency",
      "description": "How often events are fired, in Hz.",
      "type": "number",
      "minimum": 0.0033333333333333335,
      "maximum": 1000000
    },
    "delay": {
      "title": "The delay between successive triggers",
      "description": "The time between two triggers, in seconds.",
      "type": "number",
      "minimum": 1e-06,
      "maximum": 300
    },
    "message": {
      "title": "The event payload",
      "description": "The data sent with each event.  A ${username} is replaced by the user's username and ${count} is replaced by the message number.",
      "minLength": 1,
      "type": "string",
      "default": "tick"
    },
    "count": {
      "title": "The number of events",
      "description": "The number of events to generate before cancelling the subscription.  If not specified then the events are supplied until the subscription is explicitly cancelled by the client.",
      "type": "number"
    }
  },
  "oneOf": [
    {
      "required": [
        "frequency"
      ]
    },
    {
      "required": [
        "delay"
      ]
    }
  ],
  "additionalProperties": false
}
paul@sprocket:~$
```

When subscribing for `metronome` events, either the `frequency` or
`delay` argument must be provided.  The `count` and `message` argument
may be provided, but are not required.

Selectors for `inotify` are more complicated, but a JSON Schema that
describes them is available by querying the `inotify/selector`
resource.

#### Events

The `event` resource (`/api/v1/events/eventTypes/<name>/event`)
provides a JSON Schema description of events this event type will
generate.

The `metronome/event` resource
(`/api/v1/events/eventTypes/metronome/event`) provides the schema for
the metronome event type:

```console
paul@sprocket:~$ curl -s -u paul https://dcache.example.org:3880/api/v1/events/eventTypes/metronome/event | jq .
Enter host password for user 'paul':
{
  "$id": "http://dcache.org/frontend/events/metronomeEvents#",
  "$schema": "http://json-schema.org/draft-06/schema#",
  "type": "string"
}
paul@sprocket:~$
```

This JSON Schema describes a String which (in this case) corresponds
to the `message` field in the metronome selector.

Events for `inotify` are more complicated, but a JSON Schema that
describes them is available by querying the `inotify/event` resource.

### Channel lifecycle

All channel operations happen within the `events/channels`
(`/api/v1/events/channels`) resource.  A GET request against this
resource returns a list of channels.  This is initially empty:

```console
paul@sprocket:~$ curl -s -u paul https://dcache.example.org:3880/api/v1/events/channels | jq .
Enter host password for user 'paul':
[]
paul@sprocket:~$
```

In order to receive any events, a client must connect to a channel.
This requires that a client first creates a channel. This is done by
making a POST request to the `channels` resource:

```console
paul@sprocket:~$ curl -D- -u paul -X POST https://dcache.example.org:3880/api/v1/events/channels
Enter host password for user 'paul':
HTTP/1.1 201 Created
Date: Tue, 09 Apr 2019 20:50:07 GMT
Server: dCache/5.1.0-SNAPSHOT
Location: https://dcache.example.org:3880/api/v1/events/channels/pf_B1dEed98IVKqc9BNa-w
Access-Control-Allow-Origin: *
Access-Control-Allow-Methods: GET, POST, DELETE, PUT
Access-Control-Allow-Headers: Content-Type, Authorization, Suppress-WWW-Authenticate
Content-Length: 0

paul@sprocket:~$
```

The `Location` response header contains the channel endpoint.  In the
above example, the new channel is
`https://dcache.example.org:3880/api/v1/events/channels/pf_B1dEed98IVKqc9BNa-w`.

A subsequent GET request on `events/channels` will show this channel:

```console
paul@sprocket:~$ curl -s -u paul https://dcache.example.org:3880/api/v1/events/channels | jq .
Enter host password for user 'paul':
[
  "https://dcache.example.org:3880/api/v1/events/channels/pf_B1dEed98IVKqc9BNa-w"
]
paul@sprocket:~$
```

Information about a channel may be obtained by a GET request against a
channel endpoint.  It is important to specify that the result should
be JSON by specifying the `Accept` HTTP request header.  This is to
avoid the request being processed as an SSE request.

```console
paul@sprocket:~$ curl -s -u paul -H 'Accept: application/json' \
    https://dcache.example.org:3880/api/v1/events/channels/pf_B1dEed98IVKqc9BNa-w | jq .
Enter host password for user 'paul':
{
  "timeout": 300
}
paul@sprocket:~$
```

This shows the timeout: the amount of time a client is disconnected
from the channel after which the channel is automatically removed.

This value may be modified using a PATCH request.  In the following
example, the timeout is extended to one hour:

```console
paul@sprocket:~$ curl -s -u paul -X PATCH -H 'Content-Type: application/json' -d '{"timeout" : 3600}'
        https://dcache.example.org:3880/api/v1/events/channels/pf_B1dEed98IVKqc9BNa-w | jq .
Enter host password for user 'paul':
paul@sprocket:~$
```

After this request is successfully processed, the channel metadata
shows the updated timeout value:

```console
paul@sprocket:~$ curl -s -u paul -H 'Accept: application/json'
        https://dcache.example.org:3880/api/v1/events/channels/pf_B1dEed98IVKqc9BNa-w | jq .
Enter host password for user 'paul':
{
  "timeout": 3600
}
paul@sprocket:~$
```

Once a client is finished receiving events, it can remove a channel
using a DELETE request:

```console
paul@sprocket:~$ curl -u paul -X DELETE https://dcache.example.org:3880/api/v1/events/channels/pf_B1dEed98IVKqc9BNa-w
Enter host password for user 'paul':
paul@sprocket:~$
```

Subsequent attempts to use this channel will return a 404 status code
and it will not appear in the channel list.

```console
paul@sprocket:~$ curl -D- -u paul -H 'Accept: application/json'
        https://dcache.example.org:3880/api/v1/events/channels/pf_B1dEed98IVKqc9BNa-w
Enter host password for user 'paul':
HTTP/1.1 404 Not Found
Date: Tue, 09 Apr 2019 21:11:57 GMT
Server: dCache/5.1.0-SNAPSHOT
Content-Type: application/json
Access-Control-Allow-Origin: *
Access-Control-Allow-Methods: GET, POST, DELETE, PUT
Access-Control-Allow-Headers: Content-Type, Authorization, Suppress-WWW-Authenticate
Content-Length: 51

{"errors":[{"message":"Not Found","status":"404"}]}
paul@sprocket:~$
```

A client is not required to remove the channel it created, because
dCache will automatically delete any left over channel once they have
been idle for too long.  However, it is recommended clients explicitly
delete a channel if it is no longer needed.  This is because each
dCache user is only allowed a limit number of concurrent channels.

### Subscriptions

A channel subscribes to events.  A channels subscriptions are handled
with the `events/channels/<id>/subscriptions` endpoint
(`/api/v1/events/channels/<id>/subscriptions`).

A GET request returns the current list of subscriptions.  This is
initially empty:

```console
paul@sprocket:~$ curl -u paul -s https://dcache.example.org:3880/api/v1/events/channels/pf_B1dEed98IVKqc9BNa-w/subscriptions | jq .
Enter host password for user 'paul':
[]
paul@sprocket:~$
```

To subscribe to events, a client issues a POST request to the resource
below subscriptions with the event type name.  The POST request entity
is the selector for this subscription.

For example, to create a subscription to the `metronome` event type,
the client posts to `events/channels/<id>/subscriptions/metronome`
resource with a JSON object that satisfies the metronome selector JSON
Schema (`events/eventTypes/metronome/selector`)

The following is a simple example that creates a subscription to
`metronome` with the simple selector `{"delay": 2}`:

```console
paul@sprocket:~$ curl -D- -u paul -X POST -H 'Content-Type: application/json' -d '{"delay":2}'
        https://dcache.example.org:3880/api/v1/events/channels/pf_B1dEed98IVKqc9BNa-w/subscriptions/metronome
Enter host password for user 'paul':
HTTP/1.1 201 Created
Date: Tue, 09 Apr 2019 21:29:30 GMT
Server: dCache/5.1.0-SNAPSHOT
Location: https://dcache.example.org:3880/api/v1/events/channels/pf_B1dEed98IVKqc9BNa-w/subscriptions/metronome/53db4a4a-d04b-47ec-acee-b475772586ed
Access-Control-Allow-Origin: *
Access-Control-Allow-Methods: GET, POST, DELETE, PUT
Access-Control-Allow-Headers: Content-Type, Authorization, Suppress-WWW-Authenticate
Content-Length: 0

paul@sprocket:~$
```

The `Location` response header contains a resource that represents
this subscription.  This new subscription is also now included in the
channel's subscription list:

```console
paul@sprocket:~$ curl -s -u paul
        https://dcache.example.org:3880/api/v1/events/channels/pf_B1dEed98IVKqc9BNa-w/subscriptions | jq .
Enter host password for user 'paul':
[
  "https://dcache.example.org:3880/api/v1/events/channels/pf_B1dEed98IVKqc9BNa-w/subscriptions/metronome/53db4a4a-d04b-47ec-acee-b475772586ed"
]
paul@sprocket:~$
```

A GET request on the subscription returns the selector used to
generate the subscription:

```console
paul@sprocket:~$ curl -s -u paul https://dcache.example.org:3880/api/v1/events/channels/pf_B1dEed98IVKqc9BNa-w/subscriptions/metronome/53db4a4a-d04b-47ec-acee-b475772586ed | jq .
Enter host password for user 'paul':
{
  "delay": 2
}
paul@sprocket:~$
```

Once a subscription is no longer needed, it may be remove by issuing a
DELETE request against the subscription resource.

```console
paul@sprocket:~$ curl -u paul -X DELETE https://dcache.example.org:3880/api/v1/events/channels/pf_B1dEed98IVKqc9BNa-w/subscriptions/metronome/53db4a4a-d04b-47ec-acee-b475772586ed
Enter host password for user 'paul':
paul@sprocket:~$
```

The channel will stop receiving events for this subscription and the
subscription is no longer listed:

```console
paul@sprocket:~$ curl -s -u paul
        https://dcache.example.org:3880/api/v1/events/channels/pf_B1dEed98IVKqc9BNa-w/subscriptions | jq .
Enter host password for user 'paul':
[]
paul@sprocket:~$
```

### Receiving events: SSE

The SSE protocol is sufficiently simple that it is possible to see
events using curl.  In this section, we explore enough to demonstrate
receiving events.  In real environments, you would use an SSE client
library to receive events.

First, to receive SSE events, the client makes a GET request to the
channel, specifying it will accept the MIME type `text/event-stream`

```console
paul@sprocket:~$ curl -u paul -H 'Accept: text/event-stream' https://dcache.example.org:3880/api/v1/events/channels/pf_B1dEed98IVKqc9BNa-w
Enter host password for user 'paul':
^C
paul@sprocket:~$
```

The server does not return straight away, but blocks.  Any event will
be delivered as a series of lines describing the event type, the
subscription and the event itself.

You must interrupt curl (typically, by typing Control+C) in order to
stop it from waiting for an event.

Add example showing subscribing to events, and deleting an event.

Add inotify examples

## Doors

Information on alternative network protocols.