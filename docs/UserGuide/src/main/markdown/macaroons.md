Chapter 1. Understanding macaroons
==================================

Macaroons are a standard way of handling delegated authorisation that
brings several benefits.  Delegated authorisation is where the person
who is allowed to do something (for example, read a specific file in
dCache) allows someone else to do that operation.  There are lots of
use-cases where delegated authorisation is useful, some of which are
described below.

Macaroons are bearer tokens.  This means that the client presents the
macaroon along with the request. Unlike with X.509 and Kerberos
authentication, there is no back-and-forth communication between the
client and dCache when accepting a macaroon.  This makes using a
macaroon very easy, but also increases the risks if a macaroon being
stolen.

## Macaroons and caveats

A macaroon is a string of characters without any spaces.  The
following is an example of a macaroon, where the single long line has
been split into multiple lines to make the macaroon more readable.

```
MDAxY2xvY2F0aW9uIE9wdGlvbmFsLmVtcHR5CjAwMThpZGVudGlmaWVyIGh \
sQ0kremlRCjAwMTVjaWQgaWlkOnBGTTA1MnJTCjAwMjFjaWQgaWQ6MjAwMj \
sxMDAxLDIwMDIsMDtwYXVsCjAwMjhjaWQgYmVmb3JlOjIwMTktMDQtMTdUM \
Dk6NTE6MjIuODQwWgowMDE5Y2lkIGhvbWU6L1VzZXJzL3BhdWwKMDAyZnNp \
Z25hdHVyZSCT6Lea6oBIEpiF2KOsZ1FQvLeoXve_a3q38TZTBWhM1Qo
```

A macaroon contains a location and an identifier.  Neither of these
values is protected from being modified; however, dCache ignores the
location information and modifying the identifier simply prevents the
macaroon from working.

Other than this, a macaroons contains a list of caveats.

Caveats either carry context information or limit how the macaroon may
be used.  Caveats are strings and, in dCache, caveats have the form
`<key>:<value>`. where `<key>` describes what kind of limitation this
caveat represends and `<value>` is information about the restriction.
For example, the caveat `before:2019-04-17T09:51:22.840Z` limits when
that macaroon may be used.  Once this time has elapsed, the macaroon
will be rejected.

Macaroon caveats have a property that makes them very useful.

Given a macaroon (like the one above), it is easy to create a new
macaroon that includes the existing caveats and some additional
caveats.  If done correctly, that new macaroon is valid and will be
accepted by dCache.

The opposite does not work: given a macaroon, it is "impossible"
(cryptographically hard) to create a new macaroon with any of the
given macaroon's caveat removed.

In simple terms, you can add caveats but cannot remove them.

## Caveats supported by dCache

dCache supports the following caveats: root, home, path, before, ip,
id, iid and activity.

Some of these caveats are included in the macaroon automatically.
Creating a new macaroon where such caveat is mentioned a second time
results in an invalid macaroon.

The other caveats may be included multiple times, each caveat
modifying what may be done with the macaroon.

### activity
### before
### home
### id
### iid
### ip
### path
### root

## Requesting a macaroon

To request a macaroon, make a POST request to the WebDAV door with the
Content-Type request header set to `application/macaroon-request`.
The request must be authenticated, but any authentication mechanism
should work: X.509 client certificate, Kerberos, username+password,
OpenID-Connect and SciToken.

The following example shows a simple macaroon request where X.509
client authentication is used:

```console-user
curl -E /tmp/x509up_u1000 -X POST \
|    -H 'Content-Type: application/macaroon-request' \
|    https://dcache.example.org/
|{
|    "macaroon": "MDA[...]Qo",
|    "uri": {
|        "targetWithMacaroon": "https://dcache.example.org/?authz=MDA[...]Qo",
|        "baseWithMacaroon": "https://dcache.example.org/?authz=MDA[..]o",
|        "target": "https://dcache.example.org/",
|        "base": "https://dcache.example.org/"
|    }
|}
```

The response to this request is a JSON Object.  The `macaroon`
property value is the macaroon.  The four `uri` properties
(`targetWithMacaroon`, `baseWithMacaroon`, `target` and `base`) are
for convenience only.

This returns the most powerful macaroon: a macaroon with the least
number of caveats.

Once you have received the macaroon, you can create a new macaroon
that has additional caveats to make the new macaroon less powerful.

A convenient way of obtaining such a macaroon is to request dCache
includes the extra caveats in the macaroon.  This has the advantage
that the macaroon only leaves dCache with the additional caveats,
providing a slight security benefit.

Send a JSON object with the POST request to request dCache issues a
macaroon that has additional caveats.  The general form for these
caveat-requesting JSON is:

```json
{
    "caveats": [
        "caveat-1",
	"caveat-2",
	"...",
	"caveat-n"
    ]
}
```

For example, to request a single additional caveat
`activity:DOWNLOAD,LIST`, the JSON object is:

```json
{
    "caveats": [
        "activity:DOWNLOAD,LIST"
    ]
}
```

As a curl command, this request looks like:

```console-user
curl -E /tmp/x509up_u1000 -X POST \
|        -d '{"caveats": ["activity:DOWNLOAD,LIST"]}' \
|        -H 'Content-Type: application/macaroon-request' \
|        https://prometheus.desy.de/
|{
|    "macaroon": "MDA[...]bgK",
|    "uri": {
|        "targetWithMacaroon": "https://prometheus.desy.de/?authz=MDA[...]bgK",
|        "baseWithMacaroon": "https://prometheus.desy.de/?authz=MDA[...]bgK",
|        "target": "https://prometheus.desy.de/",
|        "base": "https://prometheus.desy.de/"
|    }
|}
```

### Requesting path caveats

The `path` caveat may be specified by specifying a path in the POST
request.

The following operation requests a macaroon with the `path:/data/2019`
caveat:

```console-user
curl -E /tmp/x509up_u1000 -X POST \
|        -d '{"caveats": ["activity:DOWNLOAD,LIST", "path:/data/2019]}' \
|        -H 'Content-Type: application/macaroon-request' \
|        https://prometheus.desy.de/
```

The following operation requests a macaroon with the same
`path:/data/2019` caveat, but specified in the request URL:

```console-user
curl -E /tmp/x509up_u1000 -X POST \
|        -d '{"caveats": ["activity:DOWNLOAD,LIST"]}' \
|        -H 'Content-Type: application/macaroon-request' \
|        https://prometheus.desy.de/data/2019
```

### Requesting a macaroon with limited validity

Including the `before` caveat in a macaroon limits for how long a
macaroon may be used.  Once the time has elapsed, the macaroon is
useless.

The value is an instant in time, which is often not really what is
desired.  Instead, it is often desired to have a macaroon that is
valid for a fixed duration (e.g., the next five minutes).  This then
requires calculating the expiry time in order to build the `before`
caveat.  An additional problem is that the clocks on the client and
server might not agree exactly.

Both these problems are resolved by the `validity` property in the
request JSON object.  The value is an ISO 8601 duration; for example
the value `PT5M` represents five minutes.

For example, the following JSON object requests a macaroon that allows
read-only access to dCache for one hour.

```json
{
    "caveats": [
        "activity:DOWNLOAD,LIST"
    ],
    "validity": "PT1H"
}
```

## Adding caveats

Anyone with a macaroon can create a new macaroon with additional
caveats.  As each successive caveat either leaves the set of allowed
operations the same or reduces it, the new macaroon either has the
same authorisation, or weaker authorisation.

### Adding caveats autonomously

Various libraries exist for handling macaroons.  These libraries may
be used to create a new macaroon with more caveats.

### Adding caveats with dCache

Another way to create a more restricted macaroon is to use the
macaroon to request a new macaroon.  Simply use the macaroon to
authenticate when issuing the HTTP POST request.  All the current
macaroon's caveats are copied into the new macaroons and any requested
caveats are added subsequently.

## Using a macaroon

dCache supports macaroon based requests with the WebDAV door and the
frontend door.

For both doors, there are two ways of using a macaroon: in the
Authorization request header and in the URL.

### In the Authorization header

Include the macaroon prefixed by the word `Bearer` as the
`Authorization` HTTP request header: `Authorization: Bearer
MDA[...]bgK`.

The following example shows a curl request authorised by including the
macaroon in the `Authorization` request header.  The macaroon is
stored in the variable `MACAROON` to make this curl command (and any
subsequent ones) easier to read.

```console-user
MACAROON=MDA[...]bgK
curl -H "Authorization: Bearer $MACAROON" \
|        https://dcache.example.org/
```

### In the URL

In some cases, you cannot control the HTTP requests, so cannot include
the macaroon in the Authorization request header.  This is perhaps
most common for web-browsers, but can occur with frameworks.

To support these clients, dCache accepts the macaroon in the URL, as
an `authz` query parameter.

In the following example, curl sends the macaroon as part of the URL.

```console-user
MACAROON=MDA[...]bgK
curl https://dcache.example.org/?authz=$MACAROON
```

## Example macaroon use-cases

This section describes some scenarios where macaroons may prove
useful.  This is not an exhaustive list, but meant more to whet your
appetite.

### Portal use-case
### Direct sharing
### Third-party transfer
### Enfore catalogue permissions