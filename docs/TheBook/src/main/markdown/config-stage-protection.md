CHAPTER 20.  STAGE PROTECTION
=============================

Access to tape is expensive. To avoid inefficient use of tape resources, or "stage mayhem" by random, chaotic  user
activity a mechanism exists in dCache called "stage protection" that allows to control access to data on tape based
on user identity (DN), VO group membership and VO role (defined in FQAN),
storage group and protocol. Attempts to stage data that does not satisfy the criteria of
stage permission configuration will result in permission denied errors.

-----
[TOC bullet hierarchy]
-----

## Configuration of stage protection

The stage protection rules are captured in a stage configuration file which
is pointed to by the variable :

```ini
dcache.authz.staging = <path>/StageConfiguration.conf
```

The stage protection policy enforcement point (PEP) can be
`PoolManager` or `doors`. Configurable using the variable
`dcache.authz.staging.pep`.

If set to `PoolManager` like so:

```ini
dcache.authz.staging.pep = PoolManager
```

then the stage protection applies to all transfers in the system and the stage protection
configuration file naturally has to be available on the host running `PoolManager`. If
set for `doors` like:

```ini
dcache.authz.staging.pep = doors
```

then the stage protection applies only to transfers performed by doors on hosts that have
`dcache.authz.staging` defined and the file present. The host running `PinManager` also has to have
`dcache.authz.staging` defined and the file present.

The default settings are `dcache.authz.staging = ` (not set) and
`dcache.authz.staging.pep = doors`.

## Defining stage protection

Stage protection can be set up as a white or a black list.  Blacklisting is achieved by
using `!` in front of an expression.

Each line of the list may contain up to four regular expressions
enclosed in double quotes. The regular expressions match the DN, FQAN,
the Storage Group and protocol specified in the following format:

    "<DN>" ["<FQAN>" ["<StorageGroup>" ["<protocol>"]] ]

Lines starting with a hash symbol `#` are discarded as comments. The regular expression syntax follows the syntax defined for
the [Java Pattern class](http://docs.oracle.com/javase/6/docs/api/java/util/regex/Pattern.html).


Some examples of the White List Records:

   ".*" "/atlas/Role=production"
   "/C=DE/O=DESY/CN=Kermit the frog"
   "/C=DE/O=DESY/CN=Beaker" "/desy"
   "/O=GermanGrid/.*" "/desy/Role=.*"

This example authorizes a number of different groups of users:

-   Any user with the FQAN  `atlas/Role=production`.
-   The user with the DN `/C=DE/O=DESY/CN=Kermit the frog`, and any VOMS groups he belongs to.
-   The user with the DN `/C=DE/O=DESY/CN=Beaker` but only if he is also identified as a member of VO desy (FQAN `/desy`)
-   Any user with DN and FQAN that match `/O=GermanGrid/.\*` and `/desy/Role=.\*` respectively.

If a storage group is specified, three parameters must be provided. The regular expression `".*"` may be used to authorize any DN or any FQAN. Consider the following example:

Example:

    ".*" "/atlas/Role=production" "h1:raw@osm"
    "/C=DE/O=DESY/CN=Scooter" ".*" "sql:chimera@osm"

In the example above:

-   Any user with
    FQAN
    `/atlas/Role=production`
    is allowed to stage files located in the storage group
    `h1:raw@osm`.
-   The user
    `/C=DE/O=DESY/CN=Scooter`, irrespective of which VOMS groups he belongs to, is allowed to stage files located in the storage group
    `sql:chimera@osm`.

If a protocol is specified, all four parameters must be provided; for
example:

    ".*" "/atlas/Role=production" "h1:raw@osm" "Htt.*"
    "/C=DE/O=DESY/CN=Scooter" ".*" "sql:chimera@osm"  "GFtp.*"

In the example above:

-   Any user with
    FQAN
    `/atlas/Role=production`
    is allowed to stage files located in the storage group
    `h1:raw@osm` using HTTP protocol.
-   The user
    `/C=DE/O=DESY/CN=Scooter`, any VOMS groups he belongs to,
    is allowed to stage files located in the storage group
    `sql:chimera@osm` using GFTP protocol.


The exact protocol names are `DCap-3.0`, `GFtp-1.0`, `GFtp-2.0`, `Http-1.1`, `NFS4-4.1` and ` Xrootd-2.7`.
The version suffix is subject to change, therefore `.*` comes in handy.

Non-authenticated protocols like plain `DCap` protocol or `NFS` protocol (if so setup)
carry null for DN and FQAN. A `"""` expression will match nulls.

In order to allow all users using the `dCap` protocol to stage data for any storage
group the list, the configuration would look like this:

   ""  "" ".*" "DCap.*"

NB: Once stage protection configuration exists, the PEP will process it for match and if
no match is found, staging will be denied. Therefore an empty stage configuration file will
effectively deny staging for all.

As was mentioned above, black lists can be formed my adding `!<expr>` in front of matching
regular expression translating into "staging is allowed if not matching `<expr>`".

In this example:
```
".+"
"" "" "!nova.*"
"" "" "nova.*"          "!NFS4.*"
```
-   Any authenticated user (non empty DN) can stage any files using any protocol (not a black list per se but is used here from a real life setup).
-   All non-authenticated users can stage files not belonging to storage groups matching
`nova*` using any protocol.
-   All non-authenticated users cannot stage files belonging to storage groups matching
`nova*` using the `NFS` protocol.

NB: A root user is special. All authorization checks are by-passed for root user. Therefore,
in the example above, a root user will still be able to stage `nova` data using the `NFS` protocol.

The `!` notation is a convenience feature, the same setup can be expressed by using proper
Java regular expressions for negation:

   ".+"
   "" "" "^(?:(?!nova).)*$"
   "" "" "nova.*"          "^(?:(?!NFS4).)*$"

The stage protection configuration file can be edited on the running system at any time and
the policies will take effect once the file is saved to disk.
