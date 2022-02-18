Chapter 1. Understanding macaroons
==================================

Macaroons are a certain type of bearer token that was developed by
Google's Research group.  A bearer token is something a client
presents to a server, typically when making a request (the client says
"I want to delete this file, and this bearer token says I'm allowed to
do that").  Unlike X.509 and Kerberos authentication, there is no
"back and forth" communication between the client and the server: the
client simply presents the bearer.  Perhaps the most common example of
a bearer token are web browser cookies.  These are sent by the web
browser (the client) to the web server when making requests.

Macaroon's main goal is to support sharing.  It should be easy for
someone to obtain a macaroon and give it to someone else.  For
example, Alice (who has a valid dCache account) can request a macaroon
that allows Bob to read a file in dCache even though Bob is unknown to
that dCache instance.  The shared file could be private (only Alice
can read it), but Alice can still safely share it with Bob without
granting anyone else access.

This concept of delegating authorisation supports quite a broad range
of use-cases.  Person-to-person sharing is only one example.  Some
other use-cases are discussed below, but this chapter cannot be
exhaustive.  You may find other use-cases where using macaroons makes
sense.

Bearer tokens suffer from the problem that they are relatively easy to
"steal".  Here, "steal" really means that an unauthorised person
obtains a copy of the token, so they can start using it.  There are
[numerous
examples](https://en.wikipedia.org/wiki/HTTP_cookie#Cookie_theft_and_session_hijacking)
of exploits that work by an attacker somehow "stealing" web-browser
cookies.

Macaroons, as bearer tokens, suffer from the same problem.  Similar
precautions need to be taken (e.g., only send macaroons over an
encrypted/TLS network connections).  Macaroons also offer some
protection over simple cookies by allowing the token to be locked
down.  This may limit the damage should the token be stolen and could
even prevent its misuse altogether.

To be useful, a macaroon has a carefully constructed structure.  This
structure is described in a research paper published by the team at
Google research.  Although the macaroon's format has not been adopted
as a standardised, various mutually compatible libraries exist that
support it.  In this sense, macaroons have become a de facto standard.

This chapter will provide a introduction to macaroons.  It will not
describe how macaroons are encoded, but it will provide a high-level
overview so that the key concepts may be understood.  For example,
caveats are described but without detailing the cryptographic
algorithms that underpinning their behaviour.  The Google Research
[paper on macaroons](https://research.google/pubs/pub41892/) provides
an excellent description of the structure, if you want to know more.

This chapter will also describe how macaroons work within dCache.  For
the most part, this is describing the authorisation framework that
macaroons provide and how this is mapped to caveats.  Unfortunately,
there is no standard way of encoding authorisation within macaroons,
so this is necessarily dCache-specific.  However, similar semantics
have been adopted by other storage systems.

## Basic structure of a macaroon

A macaroon is a string of characters without any spaces.  The
following is an example of a macaroon, where the single long line has
been split into multiple lines to make the macaroon more readable in
this document.

```
MDAxY2xvY2F0aW9uIE9wdGlvbmFsLmVtcHR5CjAwMThpZGVudGlmaWVyIGh \
sQ0kremlRCjAwMTVjaWQgaWlkOnBGTTA1MnJTCjAwMjFjaWQgaWQ6MjAwMj \
sxMDAxLDIwMDIsMDtwYXVsCjAwMjhjaWQgYmVmb3JlOjIwMTktMDQtMTdUM \
Dk6NTE6MjIuODQwWgowMDE5Y2lkIGhvbWU6L1VzZXJzL3BhdWwKMDAyZnNp \
Z25hdHVyZSCT6Lea6oBIEpiF2KOsZ1FQvLeoXve_a3q38TZTBWhM1Qo
```

Although it may not be obvious by simply looking at it, a macaroon
encodes several pieces of information.

An important part of how macaroons work is that the important
information is cryptographically protected.  Someone (who understands
the macaroon structure) could modify some of these characters in an
attempt to gain unauthorised access; however, any such change will be
detected and the modified macaroon will be rejected.

A macaroon does contains two fields (the location and the identifier)
that are not cryptographically protected.  These are meant to provide
hints.  dCache simply ignores the location information.  Modifying the
identifier simply prevents the macaroon from working.

The main part of a macaroon are the caveats.  This is a list of
textual information that is cryptographically protected.  Any attempt
to create a new macaroon by modifying a caveat in an existing macaroon
will be detected and rejected by dCache; for example, Alice obtains a
macaroon that allows someone to view the file `shared project.dat` and
she shares that macaroon with Bob.  If Bob creates a new macaroon in
which the file's name (`shared project.dat`) has been changed to `top
secret.dat` then dCache will detect this change and reject the
macaroon.

Caveats have an interesting trick.  A macaroon contains a list of
caveats that have a special property.  It's impossible for someone to
create a new macaroon by copying fewer caveats from a valid macaroon:
dCache will detect this and reject the macaroon.  However, anyone can
create a new macaroon by taking the caveats from a valid macaroon and
appending additional caveats at the end.

In simple terms, you can "add" caveats but cannot "remove" them.

This is easier to expain with some concrete examples, so we'll come
back to this once the authorisation model and the corresponding
caveats have been explained.

## The format of a caveat

The macaroon paper from dCache describes macaroons in terms of a list
of textual caveats.  It provides no details on what text these caveats
should contain.  This makes some sense, since the text will likely be
domain-specific: the caveats for a storage system could be rather
different from the caveats for, say, a IAAS system or some other kind
of service.

In dCache, we have adopted a general structure for caveats.  They all
have the form `KEY:VALUE`.  In the example caveat
`before:2022-02-02T09:51:22.840Z`, the key is `before` and the value
is `2022-02-02T09:51:22.840Z`.  The key describes what kind of caveat
this is and the value provides the details for this specific
restriction.  Using this example, the key (`before`) indicates that
this caveat limits the macaroons validity, while the value part
(`2022-02-02T09:51:22.840Z`) describes when the macaroon becomes
invalid.

Some caveats carry contextual information.  Such information does not
limit the macaroon's use, but encodes "useful" information.  As an
example, dCache will generate a unique identifier for each macaroon.
This is encoded as a caveat, which means the value is
cryptographically protected, as noone can remove or modify a caveat
without dCache rejecting the macaroon.

## Authorisation model for macaroons.

All requests for which the client presents a macaroon are evaluated as
the user that requested the macaroon.  If Alice requests a macaroon
and shares it with Bob then Bob can only access files that Alice would
be able to access.

### Identity

dCache does this by including a contextual caveat that describes the
uid, username and gids of Alice when she requested the macaroon.  This
has a couple of important consequences:

  * If Alice creates a macaroon for sharing a file and (subsequently)
    the dCache namespace permissions change so that Alice can no
    longer read that file then the macaroon will also no longer have
    access.

  * If the macaroon allows Bob to create new content (upload files,
    create directories, etc) then those new files and directories will
    be owned by Alice.

    As with regular uploads, if a directory has the setgid bit then
    the group-ownership will come from the parent directory.

### Activities

dCache defines seven activities that describe what someone using the
macaroon is allowed to do.  These are `READ_METADATA`,
`UPDATE_METADATA`, `LIST`, `DOWNLOAD`, `MANAGE`, `UPLOAD` and
`DELETE`,

<dl>
  <dt>READ_METADATA</dt>

  <dd>
    <p>Any requests that discover metadata about a file or directory.
    Examples include querying a file's size, POSIX permissions,
    ownership, any ACLs, any checksum values that dCache knows, read
    extended attributes, etc.</p>
  </dd>

  <dt>UPDATE_METADATA</dt>

  <dd>
    <p>Any request that attempts to update the metadata about the
    file: modifying the POSIX permissions, updating ACLs, update
    extended attributes.</p>
  </dd>

  <dt>LIST</dt>

  <dd>
    <p>Any request through which a user might discover the existence
    of files or directories.  This is mostly through some kind of
    directory listing.  This may be machine readable (e.g., WebDAV's
    PROPFIND method) or human readable (e.g., WebDAV's HTML-rendering
    of a directory).</p>
  </dd>

  <dt>DOWNLOAD</dt>

  <dd>
    <p>Any request that exposes data from a file.</p>
  </dd>

  <dt>MANAGE</dt>

  <dd>
    <p>Any request that adjusts the namespace: by renaming files,
    moving files, creating directories, etc.  If, by renaming a file,
    the request overwrites some existing data then the request has
    both MANAGE and DELETE activities.</p>
  </dd>

  <dt>UPLOAD</dt>

  <dd>
    <p>Any request that creates new data within dCache.  If the upload
    overwrites some existing data then the request has both UPLOAD and
    DELETE activities.</p>
  </dd>

  <dt>DELETE</dt>

  <dd>
    <p>Any request that makes data permanently inaccessible for all
    users.  Making data inaccessible for some users is a modification
    to the authorisation (see UPDATE_METADATA activity). Making data
    inaccessible from one path but accessible from other is namespace
    management (see MANAGE activity).</p>
  </dd>
</dl>

The following describes how HTTP requests are mapped to different
activities.  Similar mappings exist for other protocols that support
macaroons.

<dl>
  <dt>HEAD</dt>

  <dd>The READ_METADATA activity.</dd>

  <dt>GET</dt>

  <dd>The DOWNLOAD activity.</dd>

  <dt>PUT</dt>

  <dd>The UPLOAD activity if file does not already exist, UPLOAD and
  DELETE activity if target file already exists.</dd>

  <dt>DELETE</dt>

  <dd>The DELETE activity.</dd>

  <dt>PROPFIND</dt>

  <dd>The READ_METADATA activity if the target is a file; both the
  LIST and READ_METADATA activities if the target is a directory.</dd>

  <dt>PROPPATCH</dt>

  <dd>The UPDATE_METADATA activity.</dd>

  <dt>COPY (internal)</dt>

  <dd>Both the UPLOAD and DOWNLOAD activities.</dd>

  <dt>COPY (HTTP-TPC)</dt>

  <dd>The UPLOAD activity for pull requests.  The DOWNLOAD activity
  for push requests.</dd>
</dl>

### Paths

Macaroons have two paths: a root path and a visibility path.  Either
is optional and the filesystem root (i.e., `/`) is assumed if they are
not specified.

#### The root path

The root path describes a path within dCache namespace under which all
requests are resolved.  A client attempting to read the file
`/latest.dat` with a macaroon containing a root
`/Users/paul/shared-with-Bob` will attempt to download the file
`/Users/paul/shared-with-Bob/latest.dat`.

This operates in a similar way to the common chroot(8) Unix command.

Care is taken so that it is not possible to "escape" from the root
"jail"; for example, requesting a file `/../latest.dat` using the
above example macaroon will resolve to the same file.

#### The visibility path

(For historic reasons, this path is encoded as `path` caveat.)

The visibility path describes which part of the namespace (a subtree)
the user can see.  Parent directories show only a single directory.
Attempts to access files or directories outside of this subtree will
fail.

As an example, if a macaroon has a visibility path of
`/Users/alice/shared-with-Bob` then when Bob makes a directory listing
on the root path he will see a single item: the directory `Users`.
When making a directory listing on `/Users` he sees only the `alice`
directory.  Likewise, when making a directory listing on
`/Users/alice` he sees only the `shared-with-Bob` directory.

Bob is not allowed to list the directory `/Users/paul`, since this is
not a parent directory of the visibility path
(`/Users/alice/shared-with-Bob`).  This is true even if that directory
exists and it is visible to Alice.

#### Comparing root and visibility paths

Root paths have the advantage of hiding unrelated information.  For
example, if Alice wishes to share the directory
`/Users/alice/shared-with-Bob` there may be no particular benefit in
Bob knowing where this directory is located within dCache's namespace.
Indeed, including this information may make it harder for Bob to find
the desired file.

Visibility paths have the advantage of providing a common path for a
file.  A common URL may be shared between dCache users authenticating
directly with dCache, and users who are accessing data via a macaroon.
This may be useful if users with dCache accounts wish to use the same
URL when authenticating directly and when accessing data via a
macaroon.

The choice of using root path or visibility paths depends on which of
these features is more useful.

#### Combining root and visibility paths

The root and visibility paths are optional.  A macaroon may also have
both paths defined.

If both the root and visibility paths are defined then the root path
takes priority and the visibility path is processed relative to the
root path.

TODO double-check this.

### Validity

Macaroons always have a time limit: they are no longer useful after a
specific time.

If, when requesting a macaroon, the client does not indicate for how
long the macaroon should be valid then dCache will add a default
validity.  If the client indicates for how long the macaroon should
stay valid then dCache will include this value provided it isn't too
long.

### IP address

A macaroon may also be limited by specifying from which IP addresses
or subnets it may be used.  Both IPv4 and IPv6 addresses and subnets
may be specified.

dCache will reject requests made from a client if that client has an
IP address outside of the set of valid IP addresses.

## Caveats supported by dCache

Recall how dCache supports caveats with the structure
`KEY:VALUE`. dCache will reject any macaroon that contains a caveats
that is not of the form `KEY:VALUE`.

The allowed key values are: `root`, `home`, `path`, `before`, `ip`,
`id`, `iid` and `activity`.  If a macaroon contains any caveats with a
different key then dCache will reject that macaroon.

Some of these caveat keys must appear only once.  A macaroon that
contains multiple caveats with that key is invalid and will be
rejected by dCache.

Other caveat keys may appear multiple times in a macaroon.  This is to
allow clients to create "weaker" (more restricted) versions of a
macaroon.

More formally, if a macaroon does not allow a request R then, in
general, it is not possible to create a new macaroon by adding a
caveat that allows R.  Caveats only ever reduce the set of allowed
operations.

The details on how multiple caveats with the same key combine are
described below.

### activity

The `activity` key describes the set of activities that are allowed
with this macaroon.  The value is a comma-separated list of activity
names.  The order of the activities does not matter.  The semantics of
these activities is described above.

For example, the caveat `activity:LIST,DOWNLOAD,MANAGE` allows the
client to make LIST requests, DOWNLOAD requests and MANAGE requests.

The `READ_METADATA` activity is needed for all the other
activities. To save space, it is assumed if any other activity is
specified.  This means that the caveat `activity:LIST` and
`activity:LIST,READ_METADATA` are equivalent.

Multiple `activity` caveats combine by taking the intersection of the
allowed activities.

For example, a macaroon with the two `activity` caveats

```
activity:LIST,MANAGE,DOWNLOAD
activity:LIST,UPLOAD,DOWNLOAD
```

is equivalent to the single `activity` caveat:

```
activity:LIST,DOWNLOAD
```

### before

The `before` key describes when a macaroon is no longer valid.  The
value is a timestamp following ISO 8601 and must be in UTC ("Zulu")
time zone.  The timezone information must be provided.

Once this time has occured, the macaroon is rejected.

### home

The `home` caveat describes the home directory of the user.  This
caveat is optional and assumed to be the filesystem root (`/`) if
omitted.

### id

The identity of the user, encoding the user's uid, gids and username.

A macaroon must have exactly one caveat with this key to be valid.
This caveat is added automatically by dCache.

### iid

A caveat with `iid` key is the "issuer" ID.  It provides a unique ID
for this macaroon.

A macaroon must have exactly one caveat with this key to be valid.
This caveat is added automatically by dCache.

### ip

The IP addresses or subnets from which a client may use the macaroon.
The value is a comma-separated list of addresses (IPv4 or IPv6) or
subnets following CIDR representation.

If a macaroon contains multiple caveats with the `ip` key then this is
equivalent to taking the intersection of these sets.  In other words,
a client request is accepted only if its IP address is valid according
to all caveats containing the `ip` key.

### path

A caveat with the `path` key describes the visibility path for this
macaroon.  See above for a full description of the semantics of this
path.

This caveat is optional.  If omitted then the visitibility path is
assumed to be the filesystem root (`/`).

A macaroon may contain multiple `path` caveats.  These combine by
considering subsequent caveats as relative paths and resolving them
against the current (effective) visibility path.

For example, a macaroon with the two `path` caveats:

```
path:/Users/alice
path:shared-with-Bob
```

is equivalent to a macaroon with the single `path` caveat:

```
path:/Users/alice/shared-with-Bob
```

Visibility path caveats are always considered relative to the their
previous caveats even if they start with a slash.  For example a
macaroon with the two `path` caveats

```
path:/Users/alice
path:/shared-with-Bob
```

is also equivalent to a macaroon with
`path:/Users/alice/shared-with-Bob`.

Care should be taken when combining `root` and `path` caveats as
including a `root` caveat will modify the effective visibility path.

### root

A caveat with the `root` key describes the root path for this
macaroon.  See above for a full description of the semantics of this
path.

This caveat is optional.  If omitted then the root path is assumed to
be the filesystem root (`/`).

A macaroon may contain multiple `root` caveats.  These combine by
considering subsequent caveats as relative paths and resolving them
against the current (effective) root path.

For example the macaroon with the two `root` caveats:

```
root:/Users/alice
root:shared-with-Bob
```

is equivalent to the macaroon with the single `root` caveat:

```
root:/Users/alice/shared-with-Bob
```

Specifying a root caveat after a visibility path caveat will alter the
affective visibility path.

For example a macaron with the two `path` and `root` caveats:

```
path:/Users/alice/shared-with-Bob
root:/Users/alice
```

is equivalent to a macaroon with the caveats:

```
root:/Users/alice
path:/shared-with-Bob
```

If the macaroon contains root and visibility paths that are
incompatible then the macaroon is invalid.

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

See [Macaroon Hands-On](macaroons_handson.md) for a few examples on
using macaroons with `curl` on the command line.

### Portal use-case
### Direct sharing
### Third-party transfer
### Enfore catalogue permissions
