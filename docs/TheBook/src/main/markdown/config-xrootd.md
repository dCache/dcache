Chapter 10. dCache as xroot-Server
===================================

This chapter explains how to configure dCache in order to access it
via the `xroot` protocol, allowing `xroot`-Clients like ROOT’s
TXNetfile and xrdcp to do file operations against a dCache instance in
a transparent manner. dCache implements version 2.1.6 of `xroot`
protocol.

-----
[TOC bullet hierarchy]
-----

## Setting up

To allow file transfers in and out of dCache using xroot, a new
`xrootd door` must be started. This door acts then as the entry point
to all xroot requests. Compared to the native xrootd
server-implementation (produced by SLAC), the `xrootd door`
corresponds to the `redirector node`.

To enable the `xrootd door`, you have to change the layout file
corresponding to your dCache-instance. Enable the xrootd-service
within the domain that you want to run it by adding the following line

```ini
[<domainName>/xrootd]
```

Example:

You can just add the following lines to the layout file:

```ini
[xrootd-${host.name}Domain]
[xrootd-${host.name}Domain/xrootd]
```

After a restart of the domain running the DOOR-XROOTD, done e.g. by executing

```console-root
dcache restart xrootd-babelfishDomain
|Stopping xrootd-babelfishDomain (pid=30246) 0 1 2 3 4 5 6 7 done
|Starting xrootd-babelfishDomain done
```

the xrootd door should be running. A few minutes later it should appear at the web monitoring interface under "Cell Services" (see [the section called “The Web Interface for Monitoring dCache”).](intouch.md#the-web-interface-for-monitoring-dcache)

### Parameters

The default port the `xrootd door` is listening on is 1094. This can be changed two ways:

1.  *Per door*: Edit your instance’s layout file, for example
`/etc/dcache/layouts/example.conf` and add the desired port for the
xrootd door in a separate line (a restart of the domain(s) running the
xrootd door is required):

```ini
[xrootd-${host.name}Domain]
[xrootd-${host.name}Domain/xrootd]
xrootd.net.port = 1095
```

2.  *Globally:* Edit `/etc/dcache/dcache.conf` and add the variable
`xrootd.net.port` with the desired value (a restart of the domain(s)
running the `xroot door` is required):

```ini
xrootd.net.port=1095
```

For controlling the `TCP`-portrange within which `xrootd`-movers will
start listening in the <pool>Domain, you can add the properties
`dcache.net.lan.port.min` and dcache.net.lan.port.max to
`/etc/dcache/dcache.conf` and adapt them according to your
preferences. The default values can be viewed in
`/usr/share/dcache/defaults/dcache.properties`.

```ini
dcache.net.lan.port.min=30100
dcache.net.lan.port.max=30200
```

## QUICK TESTS

The subsequent paragraphs describe a quick guide on how to test
`xroot` using the **xrdcp** and **ROOT** clients.

### Copying files with xrdcp

A simple way to get files in and out of dCache via `xroot` is the
command xrdcp. It is included in every xrootd and ROOT distribution.

To transfer a single file in and out of dCache, just issue

```console-user
xrdcp /bin/sh root://<xrootd-door.example.org>/pnfs/<example.org>/data/xrd_test
xrdcp root://<xrootd-door.example.org>/pnfs/<example.org>/data/xrd_test /dev/null
```

### Accessing files from within ROOT

This simple ROOT example shows how to write a randomly filled histogram to a file in dCache:

    root [0] TH1F h("testhisto", "test", 100, -4, 4);
    root [1] h->FillRandom("gaus", 10000);
    root [2] TFile *f = new TXNetFile("root://<door_hostname>//pnfs/<example.org>/data/test.root","new");
    061024 12:03:52 001 Xrd: Create: (C) 2004 SLAC INFN XrdClient 0.3
    root [3] h->Write();
    root [4] f->Write();
    root [5] f->Close();
    root [6] 061101 15:57:42 14991 Xrd: XrdClientSock::RecvRaw: Error reading from socket: Success
    061101 15:57:42 14991 Xrd: XrdClientMessage::ReadRaw: Error reading header (8 bytes)

Closing remote `xroot` files that live in dCache produces this
warning, but has absolutely no effect on subsequent ROOT commands. It
happens because dCache closes all `TCP` connections after finishing a
file transfer, while the SLAC xroot client expects to keep them open
for later reuse.

To read it back into ROOT from dCache:

    root [7] TFile *reopen = TXNetFile ("root://<door_hostname>//pnfs/<example.org>/data/test.root","read");
    root [8] reopen->ls();
    TXNetFile**             //pnfs/<example.org>/data/test.root
    TXNetFile*             //pnfs/<example.org>/data/test.root
    KEY: TH1F     testhisto;1     test


## Pool memory requirements

In general, each `xroot` connection to the pool will require approximately 8 MiB
of Java direct memory.  This is a consequence of several factors.  First, the
default `XRD_CPCHUNKSIZE` is 8 MiB, and the xrootd client requires the server
to read off the entire frame + body of a message on the connection, which
dCache currently holds in memory as a single request.  Second, our Netty implementations
of both the xroot framework and the mover channel use the default preference for Java NIO
[= "new I/O" or "non-blocking I/O"] which avoids buffer-to-buffer copying from user to kernel
space and back, so the direct memory requirements are greater.

This would mean that to sustain 1000 concurrent connections, you would need
a minimum of 8 GiB of direct memory, e.g.:

```
[${host.name}-5Domain]
dcache.java.memory.heap=...
dcache.java.memory.direct=8192m
```
If these are all write requests, the requirement is actually pushed up to around 12 GiB.

There are several possible approaches to mitigating the allocation of this
much memory on each pool. The first would be to lower the ``XRD_CPCHUNKSIZE``
so that the client is sending smaller frames.  This would allow more concurrent
sharing of direct memory.  Obviously, this is not uniformly enforceable
on the connecting clients, so in essence is not a real solution.

The second possibility is to try to lower the corresponding dCache max frame size.
By default, this is also 8 MiB (to match the xrootd native default).

Going from 8 MiB to 128 KiB, for instance, by doing

```
pool.mover.xrootd.frame-size=131072
```

will also cut down individual connection consumption; this, however, is mostly
useful for reads, since writes are currently implemented to read off the
entire xroot frame (and thus the entire chunk sent by the client).

For reads, the following comparison should serve to illustrate
what the lower buffer sizes can accomplish:

```
70 clients/connections
8M frame/buffer size

PEAK DIRECT MEMORY USAGE = 720 MiB
```

vs.

```
70 clients/connections
128K frame/buffer size

PEAK DIRECT MEMORY USAGE = 16 MiB
```

So the savings here is pretty significant.

As mentioned above, however, writes profit less from manipulation of the frame size.
Writing 100mb files in parallel, with 1 GiB of direct memory allocated to the JVM, for instance:

```
8 MiB:    out of memory at 55 concurrent transfers
```

vs.

```
128 KiB:  out of memory at 82 concurrent transfers
```

In either case, it does not appear that individual bandwidth is greatly affected:

```
       8M           128K

read:  111.1MB/s vs 111.1MB/s
write: 70.42MB/s vs 69.93MB/s
```

High concurrent transfers, however, may have a somewhat more pronounced affect.

The third and final approach to handling connection concurrency is
to limit the number of active movers on the pool by creating protocol-specific
I/O queues.

As an example, the following would configure an xroot-specific queue limited to 1000 movers
(be sure to do `save` to write these to the setup file):

```
\s <pools> mover queue create XRootD -order=LIFO
\s <pools> mover set max active -queue=XRootD 1000
\s <pools> jtm set timeout -queue=XRootD -lastAccess=14400 -total=432000
\s <pools> save
```

One would also need to add the following corresponding property to the dcache configuration
on the door(s):

```
xrootd.mover.queue=XRootD
```

It is suggested that the first approach to protecting pools from out-of-memory errors be
some combination of increased allocation and throttling via I/O queues; decreasing
the `pool.mover.xrootd.frame-size` should be reserved as a last resort.

### Controlling the buffer size for writes

Altering `pool.mover.xrootd.frame-size` does not affect the way writes are handled;
frame size dictates the size of outgoing frames on read requests to the
server or the incoming read responses for requests by the third-party client.

As stated above, the native xrootd client env variable, XRD_CPCHUNKSIZE,
determines the size of the frame transmitted by the client over the wire;
up until 8.2, this also determines the size of the write chunk held in
memory and subsequently written to disk. The native client default (8 MiB)
can put considerable pressure on direct memory if there are many concurrent
write connections to a given pool.  This can also lead, under extreme conditions
(or choice of an even larger chunk size) to an out-of-memory error.

We have thus added support for placing a boundary on the buffer size
which holds the incoming data; if the chunk size exceeds this maximum,
the write is broken up into segments and processed serially, with the
buffer released after each segment has been written to disk.  This offers
better defense against memory bursts.

The buffer size is controlled by the following property:

```
pool.mover.xrootd.write-buffer-size = 0 KiB
```

The default is 0, which means no limit (INF), thus preserving previous behavior.

With XRD_CPCHUNKSIZE=8 MiB and a max 512 KiB write buffer, for example, 50 concurrent
1 GiB file writes to the same pool cause JVM (i.e, Netty) direct memory usage to peak at
256 MiB; changing XRD_CPCHUNKSIZE to 64 MiB does not push direct memory consumption beyond
256 MiB, though it does reach that level immediately rather than gradually as in the first case.


## XROOT security

### Read-Write access (legacy)

Legacy default for dCache xroot is restricted to read-only, because plain
xroot was originally completely unauthenticated.

To enable read-write access, add the following line to
`${dCacheHome}/etc/dcache.conf`

```ini
xrootd.authz.anonymous-operations=FULL
```

and restart any domain(s) running a `xroot door`.

Please note that due to the unauthenticated nature of this access
mode, files can be written and read to/from any subdirectory in the
pnfs namespace (including the automatic creation of parent
directories). If there is no user information at the time of request,
new files/subdirectories generated through `xroot` will inherit
UID/GID from its parent directory. The user used for this can be
configured via the `xrootd.authz.user` property.

#### Permitting read/write access on selected directories

To overcome the security issue of uncontrolled `xroot` read and write
access mentioned in the previous section, it is possible to restrict
read and write access on a per-directory basis (including
subdirectories).

To activate this feature, a colon-seperated list containing the full
paths of authorized directories must be added to
`/etc/dcache/dcache.conf`. You will need to specify the read and write
permissions separately.

```ini
xrootd.authz.read-paths=/pnfs/<example.org>/rpath1:/pnfs/<example.org>/rpath2
xrootd.authz.write-paths=/pnfs/<example.org>/wpath1:/pnfs/<example.org>/wpath2
```

A restart of the `xroot` door is required to make the changes take effect.
As soon as any of the above properties are set, all read or write requests
to directories not matching the allowed path lists will be refused.
Symlinks are however not restricted to these prefixes.

### Strong authentication

The `xroot`-implementation in dCache includes a pluggable
authentication framework. To control which authentication mechanism is
used by `xroot`, add the `xrootd.plugins` option to your dCache
configuration and set it to the desired value.

#### GSI

To enable `GSI` authentication in `xroot`, add the following line to `/etc/dcache/dcache.conf`
(globally) or to the layout for the specific door:

```ini
xrootd.plugins=gplazma:gsi
```

When using `GSI` authentication, depending on your setup, you may or may not want dCache to fail
if the host certificate chain can not be verified against trusted certificate authorities.
Whether dCache performs this check can be controlled by setting the option
`dcache.authn.hostcert.verify` for all of dCache.  For enabling or disabling specifically
for xroot,

```ini
xrootd.gsi.hostcert.verify=true
```

*Authorization* of the user information obtained by strong authentication
is performed by contacting the gPlazma service. Please refer to
[Chapter 10, Authorization in dCache](config-gplazma.md) for instructions about
how to configure gPlazma.

> **SECURITY CONSIDERATION**
>
> In general `GSI` on `xroot` is not secure. It does not provide confidentiality and integrity
> guarantees and hence does not protect against man-in-the-middle attacks.
>
> As of 6.2, this can be mitigated by using GSI in conjunction with TLS.

####  ZTN

Scitokens/JWT bearer tokens (see below) are for authorization; however, the (SLAC) xroot protocol
also defines an authentication equivalent, ZTN, where a token is passed as a credential at
authentication (just after login).

Originally, this was a countermeasure taken to prevent stray clients from accessing the
vanilla server via methods where there was no path (and thus no CGI authz element).  However,
recent changes to the vanilla client and server will allow a ZTN token to be used as
a fallback authorization token as well, without further need to express a base64-encoded token
as part of the path query.

dCache now supports this strategy.  To illustrate, here are two different door configurations.

This one:

```
[xrootd-1095-${host.name}Domain]
dcache.java.memory.heap=2048m
dcache.java.memory.direct=2048m
[xrootd-1095-${host.name}Domain/xrootd]
xrootd.cell.name=xrootd-1095-${host.name}
xrootd.net.port=1095
xrootd.authz.write-paths=/
xrootd.authz.read-paths=/
xrootd.plugins=gplazma:none,authz:scitokens
xrootd.security.tls.mode=STRICT
xrootd.security.tls.require-login=true
xrootd.plugin!scitokens.strict=true
```
indicates that any client will be allowed through with anonymous identity and restrictions at
authentication time, but ultimately will need a token on the path in order to be authorized, with
the subject and claim being converted into dCache user and restrictions at the time of the request
containing the path (usually 'open' on the file).

This configuration,

```
[xrootd-1095-${host.name}Domain]
dcache.java.memory.heap=2048m
dcache.java.memory.direct=2048m
[xrootd-1095-${host.name}Domain/xrootd]
xrootd.cell.name=xrootd-1095-${host.name}
xrootd.net.port=1095
xrootd.authz.write-paths=/
xrootd.authz.read-paths=/
xrootd.plugins=gplazma:ztn,authz:scitokens
xrootd.security.tls.mode=STRICT
xrootd.security.tls.require-login=true
xrootd.plugin!scitokens.strict=false
```

on the other hand, turns on ZTN in the door.  For seamless functioning, this should be coupled with
a loosening of the strict requirement on the CGI/path token.   With this configuration, the client
will need to be provided a ZTN token via an environment variable, e.g.,

```
XDG_RUNTIME_DIR=/run/user/8773
```

it will look for a file named 'bt_\<uid\>' in that directory.   With that token in hand,
authorization will also take place.  A second token can still be passed as the path query CGI
element (``authz=Bearer%20<base64-encoded string>``), and will override the original if present,
but this is treated as optional, not required.

ZTN requires TLS.  One has the option to enforce this up front via the ``STRICT`` setting
(see further below), but a check will be made at authentication time as well, and an authentication
error returned if TLS is not on.

#### Token-based authorization (SciTokens/JWT)

The `xroot` dCache implementation includes a generic mechanism to plug in different authorization
handlers.

As of 6.2, xroot authorization has been integrated with gPlazma SciToken/JWT bearer token support.

Add

```
auth    sufficient	scitoken
```

to the _gplazma.conf_ configuration file in order to enable authorization.

The token for xroot is passed as an 'authz=Bearer%20' query element on paths.

For example,

```
xrdcp -f xroots:///my-xroot-door.example.org:1095///pnfs/fs/usr/scratch/testdata?authz=Bearer%20eyJ0eXAiOiJKV1QiLCJhb... /dev/null
```

dCache will support different tokens during the same client session, as well as
different tokens on source and destination endpoints in a third-party transfer.

To enable scitoken authorization on an xroot door, use "authz:scitokens" to load the plugin.
See above (under ZTN) for an example layout configuration.

Note that the above configuration enforces TLS (STRICT); this is highly recommended
with SciToken authorization as the token hash is not secure unless encrypted.
While it is not strictly required to start TLS at login (since the actual token
is not passed until a request involving a path, in this case, 'open') ––
``xrootd.security.tls.require-session=true``
would have been sufficient –– the extra protection on login of course will not
hurt. (The same applies to GSI: TLS is of course redundant for the handshake, but can further
guarantee data protection and integrity if on thereafter. For GSI-only doors, then, one can also
opt to start the TLS session after login using 'session'.)

The xroot protocol states that the server can specify supporting
different authentication protocols via a list which the client should
try in order (again, see below on multi-protocol doors). Authorization, on the other hand,
takes place after the authentication phase; the current library code assumes that the
authorization module it loads is the only procedure allowed, and there is no provision for passing
a failed authorization on to a successive handler on the pipeline.

We thus make provision here for failing over to "standard" behavior
via ``xrootd.plugin!scitokens.strict``.   If it is ``true``, then
the presence of a scitoken is required.  If ``false``, and the token is missing,
whatever restrictions that are already in force from the login apply.

##### A Note on Pool configuration with Scitokens

JWT/Scitoken authorization also requires TLS to protect the token.

> **SECURITY CONSIDERATION**
>
> The client will pass the token in the open request's path query
> regardless of whether the server supports TLS or has indicated that it should
> be turned on.  While a check for TLS when authenticating/authorizing via token
> on the door is made, and the attempt rejected if TLS is off, on the pool no such check
> occurs, since no further authorization takes place.
>
> In order to protect the bearer token when passed on redirect to the pool, then,
> the client should _always_ require TLS by using 'xroots' as the URL schema.
> By using 'xroots', the client guarantees TLS will be activated by dCache at login or
> the connection will fail.

#### Precedence of security mechanisms

The previously explained methods to restrict access via `xroot` can
also be used together. The precedence applied in that case is as follows:

With strong authentication like GSI, (as well as the `Kerberos` protocols) trust in remote
authorities is required; however, this only affects user *authentication*, while authorization
decisions can be adjusted by local site administrators by adapting the `gPlazma` configuration.
Hence, the permission check executed by an authorization plugin (if one is installed), or
acquired at login via GSI, is given the lowest priority; access control is ultimately determined
by the file catalogue (global namespace).

This may also be true for some token authorization schemas; however, as implemented, the JWT/
scitoken authorization (see below) **can in fact override the local controls via the scope and
group claims** the token carries.

To allow local site’s administrators to override remote security
settings, write access can be further restricted to few directories
(based on the local namespace, the `pnfs`). Setting xroot access to
read-only has the highest priority, overriding all other settings.

### TLS

As of 6.2, dCache supports TLS according to the protocol requirements
specified by the xroot Protocol 5.+.

The xroot protocol allows a negotiation between the client and server as to when to
initiate the TLS handshake.  The server-side options are explained in
the _xrootd.properties_ file.  Currently supported is the ability to
require TLS on all connections to the door and pool, or to make TLS
optional, depending on the client.  For the former, one can also
specify whether to begin TLS before login or after.  The "after" option
is useful in the case of TLS being used with a strong authentication protocol
such as GSI, in which case it would make sense not to protect the login as GSI
already requires a Diffie-Hellman handshake to protect the passing of
credential information; it can also be used if token authorization is coupled with
anonymous authentication (``gplazma:none``).  For ZTN, TLS should begin at login.

For third-party, the dCache embedded client (on the destination server) will
initiate TLS if (a) TLS is available on the destination pool (not turned off),
and (b) the source server supports or requires it.  In the case that the source
does not support TLS, but the triggering client has expressed 'tls.tpc=1'
(requiring TLS on TPC), the connection will fail.

As of 6.2, dCache has not yet implemented the GP file or data channel options;
stay tuned for further developments in those areas.

> **A note on TLS configuration for the pools**
>
>Given that pools may need to service clients that do not support TLS (they
>may, for instance, be using a non-xroot protocol), it is probably not
>practical to make the pools require TLS by setting
>``pool.mover.xrootd.security.tls.mode=STRICT``.

> **Host cert and key**
>
> These are required to be there when the SSHHandlerFactory (which provides
> TLS support) is loaded at startup.  If either pool.mover.xrootd.security.tls.mode
> or xrootd.security.tls.mode is set to either OPTIONAL or STRICT, the host cert
> and key will be required, or the domain will not start.  You can start pools or
> doors without a host cert/key by setting these properties to OFF.

###  Multiple authentication protocol chaining and defaults

As of 8.1, dCache now supports the chaining of authentication plugins/protocols on the door.
This means that a single door can tell the client to try each or any of the protocols indicated.

The defaults have been modified so that dCache can be used out of the box in most cases
without further concern to configure doors and pools.   To summarize, for the door:

```
xrootd.plugins=gplazma:ztn,gplazma:gsi,gplazma:none,authz:scitokens
xrootd.security.tls.mode=OPTIONAL
xrootd.plugin!scitokens.strict=false
```

These defaults guarantee that the client

1. will try ZTN first, because if it has no discoverable cert keys and no proxy,
   the client will fail a GSI request unless `export XrdSecGSICREATEPROXY=0` is set;
2. will then try GSI if it has no token;
3. if it tries ZTN it must turn on/request TLS or that protocol will be rejected (xroots:// must be used)
4. if those fail, it can be logged in anonymously and potentially receive further authorization
   downstream from another token;
5. if ZTN succeeds and there is no authorization token on the path URL, the ZTN token
   will be used as fallback (the scitoken requirement is not strict);
6. third-party-copy will succeed with dCache doors as sources because the third-party
   client can connect using the rendezvous token without further authentication
   (gplazma:none).
7. NOTE:  it is possible to use GSI rather than ZTN and als provide a scitoken/JWT token on the path
   URL; TLS must be activated in this case.  The usage scenario for this would, however, be rare.

Of course, these remain configurable in case of special requirements.

The pool defaults

```
pool.mover.xrootd.tpc-authn-plugins=gsi,unix,ztn
```

have been updated to load the ZTN module, but currently this results in a NOP
because there is no agreed-upon strategy for getting the token to the third-party client;
this may change in the near future.

### Tried-hosts

Xrootd uses the path URL CGI "tried" and "triedrc" as hints to the
redirector/manager not to reselect a data source because of some
error condition or preference.  dCache provides limited support for
this attribute.  In particular, it will honor it in the case that
the indicated cause suggests some error previously encountered
that suggests an IO malfunction on the node.

The property

```
xrootd.enable.tried-hosts
```

is true by default. When it is off, the 'tried' element on the path
is simply ignored.  dCache also ignored the tried hosts when 'triedrc'
is not provided, or when it is not 'enoent' or 'ioerr'.  In the latter
two cases, the xroot door will forward the list of previously tried
hosts to the Pool Manager to ask that they be excluded from selection.

See ``xrootd.properties`` for further information.

### Proxying transfers through the door

Support for internal protected networks
for data transfer to and from pools can be achieved
using the Pool Selection Unit; Xroot generally
relies on this configuration to do the right thing.
For some sites, however, this can become rather
complicated and unwieldy.

With release 8.2, dCache xroot will support (as do FTP and NFS)
proxying transfers through the door.  This should be helpful
in those cases where use of the pool manager configuration for this purpose
is not desirable or feasible.

An xroot door is either proxying or not (currently it cannot
detect the conditions under which a transfer should be proxied;
this may be modified in future releases).  To create a proxying
door, simply set ``xrootd.net.proxy-transfers=true`` (default is ``false``); e.g.:

```
[xrootd-1096-${host.name}Domain]
dcache.java.memory.heap=2048m
dcache.java.memory.direct=2048m
[xrootd-1096-${host.name}Domain/xrootd]
xrootd.cell.name=xrootd-1096-${host.name}
xrootd.net.port=1096
xrootd.authz.write-paths=/
xrootd.authz.read-paths=/
xrootd.net.proxy-transfers=true
```

If the door uses proxying, then when an open request arrives, a proxy instance will be
launched on a new port and the client redirected to it as if it were the pool endpoint.
The proxy serves as both façade and client to the pool by intercepting requests
from the initiating client and passing them on to the pool transfer service, and similarly
relaying responses from the pool back to the client.  The connections between client and proxy
on the one hand and proxy and pool on the other are independently established
(this is necessary to support TLS, should that be requested or required), but after login
is complete, all subsequent requests and replies are passed through the proxy without
further interpretation.

As with pools, one can define the range from which proxy ports are selected:

```
xrootd.net.proxy.port.min=${dcache.net.wan.port.min}
xrootd.net.proxy.port.max=${dcache.net.wan.port.max}

```

One can also control how long the proxy will wait for a response from the pool:

```
xrootd.net.proxy.response-timeout-in-secs=30
```

> CAVEAT:  Since the purpose of proxying is to allow transfers to and from
> pools that are not accessible to the client, the door itself must obviously be able
> to connect to the pool; it thus makes sense that it would ask the Pool Manager to
> select the pool on the basis of the door's address, not the address of the client.
> This, however, has consequences for the use of the pool manager configuration
> to partition pool groups via client addresses.  At present, we have no clear
> solution to this conundrum, so you are advised to be aware that when
> proxying is on, such partitioning may be defeated for transfers that go
> through that specific door.

> BEST PRACTICE:  Memory consumption (Java direct memory, not heap) for a proxied
> door is somewhat higher than normal, since it not only has double the connections
> from the outside (one for the initial request, the second for the redirect to
> the proxy), but must also sustain the passage of data packets through it
> (and on to the pool).  For the default chunk size used for xrootd (8 MiB),
> there seems to be required approximately 28MiB  of direct memory per transfer.
> Hence the dCache default (512MiB) will very likely be insufficient.  This needs
> to be adjusted according to expected traffic, but keep in mind that something
> like 500 _concurrent_ transfers through the proxy door would require
> setting it to at least 16GiB on the door domain, e.g.:

```
[xrootd-1095-${host.name}Domain]
dcache.java.memory.direct=16384m
```

### Other configuration options

The `xrootd-door` has several other configuration properties. You can
configure various timeout parameters, the thread pool sizes on pools,
queue buffer sizes on pools, the xroot root path, the xroot user and
the xroot IO queue. Full descriptions on the effect of those can be
found in `/usr/share/dcache/defaults/xrootd.properties`.

## XROOTD Third-party Transfer

Starting with dCache 4.2, native third-party transfers between dCache
and another xroot server (including another dCache door) are possible.

To enforce third-party copy, one must execute the transfer using

    xrdcp --tpc only <source> <destination>

One can also try third party and fail over to one-hop two-party (through the client) by using

    xrdcp --tpc first <source> <destination>

#### TPC from dCache to another xroot server

Very few changes in the dCache door were needed to accomplish this.
If dCache is merely to serve as file source, then all that is needed
is to update to version 4.2+ on the nodes running the xrootd doors.

#### TPC from another xroot server to dCache, or between dCache instances

As per the protocol, the destination pulls/reads the file from the source and
writes it locally to a selected pool. This is achieved by an embedded third-party
client which runs on the pool. Hence, using dCache as destination means the pools
must also be running dCache 4.2+.

Pools without the additional functionality provided by 4.2+ will not be able
to act as destination in a third-party transfer and a "tpc not supported" error
will be reported if `--tpc only` is specified.

### Unauthenticated TPC using the xrootd-generated rendezvous key.

The xrootd protocol allows the third-party-copy client to read the source file under
the following conditions:

1. The initiating client has successfully opened the source file;
2. The initiating client has been authorized to write the requested path on the destination;
3. The third-party client has successfully connected to the source.

Note that (3) does not strictly require authentication, much less further authorization.  This
is achieved by the generation of an internal opaque key called a 'rendezvous token' which is
handed off by the client to both source and destination, with the destination forwarding
this key to the third-party-client.  When the third-party-client connects, the source server
checks the key against the one it was given by the initiating client, and if they match,
the transfer proceeds.

To allow for unauthenticated TPC, the ``gplazma:none`` plugin must be active on the door.

Unauthenticated TPC (or "rendezvous TPC") is done using the ``--tpc only <source> <destination>``
directive on the client command.

### Authenticated TPC using GSI

The only way currently available to enforce authentication by the TPC client is to use GSI (neither
dCache nor plain xrootd currently support passing the ZTN token to the TPC client, though this
is subject to change in the future).

To enable GSI on the TPC client, on all pools that will receive files through xroot TPC, the
GSI service provider plugin must be loaded by including this in the configuration or layout:

```ini
pool.mover.xrootd.tpc-authn-plugins=gsi
```
(Note that this is already loaded by default.)

There are two modes for using GSI to authenticate the TPC client.

#### GSI using a generated proxy

This method may be useful in the case of communication with pre-4.9 xrootd or pre-5.2 dCache
instances, or when using a pre-4.9 xroot client.

First, enable this alternative by setting ``xrootd.gsi.tpc.delegation-only`` to false.

There are two ways of providing authentication capability to the pools in
this case:

* Generate a proxy from a credential that will be recognized by the source (e.g., a robocert)
  and arrange to have it placed (and periodically refreshed) on each pool that
  may be the recipient of files transfered via xrootd TPC. The proxy path must
  be indicated to dCache by setting this property:

```ini
xrootd.gsi.tpc.proxy.path={path-to-proxy}
```

* If this property is left undefined, dCache will attempt to auto-generate a proxy from
  the `hostcert.pem` / `hostkey.pem` of the node on which the pool is running.
  While this solution means no cron job is necessary to keep the proxy up to date,
  it is also rather clunky in that it requires the hostcert DNs of all the pools
  to be mapped on the source server end.

Once again, this mode requires the ``--tpc only <source> <destination>`` directive.

#### GSI using credential (proxy) delegation

With the 5.2.0 release, full GSI (X509) credential delegation is available in dCache.
This means that the dCache door, when it acts as destination, will ask the client to sign
a delegation request.

If both endpoints support delegation (dCache 5.2+, XrootD 4.9+), nothing further
need be done by way of configuration.  dCache caches the proxy in memory and
discards it when the session is disconnected.

To indicate that you wish delegation, the xroot client requires this directive:

    xrdcp --tpc delegate only <source> <destination>

or

    xrdcp --tpc delegate first <source> <destination>

Like the xrootd server and client, dCache can determine whether the endpoint
with which it is communicating supports delegation, and fail over to the
pre-delegation protocol if not.

> NOTE:  For reading the file in dCache (dCache as TPC source), the third-party server
> needs only a valid certificate issued by a recognized CA; anonymous read access is granted
> to files (even privately owned) on the basis of the rendezvous token submitted with the request.

##### Proxy delegation and host aliasing

A feature of the xrootd client is that it will refuse to delegate a proxy to
a server endpoint if the hostname of the host credential is unverified.

This can occur if hostname aliasing is used but the host certificate was not
issued with the proper SAN extensions.  This is because the xrootd client by
default does not trust the DNS service to resolve the alias.

In the case where dCache is the destination of a third-party transfer and
the client does not delegate a proxy to the door, one may thus see
an error on the pool due to the missing proxy.  It is possible to
configure dCache to attempt to generate a proxy from the pool host certificate
in this case, but one may similarly see an error response from the
source if the host DN is not mapped there.

Short of having the host certificate reissued with a SAN extension for the alias,
DNS lookup can be forced in the client by setting this environment variable:

```
XrdSecGSITRUSTDNS
      0    do not use DNS to aide in certificate hostname validation.
      1    use DNS, if needed, to validate certificate hostnames.
      Default is 0.
```

> WARNING: this is considered to be a security hole.  The recommended
solution is to issue the certificate with SAN extensions.

Please consult the xrootd.org document for further information; this policy
may be subject to change in the future.

https://xrootd.slac.stanford.edu/doc/dev50/sec_config.htm

#### Client timeout control

The Third-party embedded client has a timer which will interrupt and return
an error if the response from the server does not arrive after a given
amount of time.

The default values for this can be controlled by the properties

`pool.mover.xrootd.tpc-server-response-timeout`

and

`pool.mover.xrootd.tpc-server-response-timeout.unit`.

These are set to 2 seconds to match the aggressive behavior of the SLAC
implementation.  However, dCache allows you to control this dynamically
as well, using the admin command:

    \s <xrootd-door> xrootd set server response timeout

This could conceivably be necessary under heavier load.

### Signed hash verification support

The embedded third-party client will honor signed hash verification if the
source server indicates it must be observed.

Starting with dCache 5.0, the dCache door/server also provides the option
to enable signed hash verification.

However, there is a caveat here. Since dCache redirects reads from the door
to a selected pool, and since the subsequent connection to the pool is
unauthenticated (this has always been the case; the connection fails if the
opaque id token dCache gives back to the client is missing), the only way to
get signed hash verification on the destination-to-pool connection is to set
the kXR_secOFrce flag. This means that the pool will then require unix
authentication from the destination and that it will expect unencrypted hashes.

While the usefulness of unencrypted signed hash verification is disputable,
the specification nevertheless provides for it, and this was the only way,
short of encumbering our pool interactions with yet another GSI handshake,
to allow for sigver on the dCache end at all, since the main subsequent
requests (open, read, etc.) are made to the pool, not the door.

dCache 5.0 will provide the following properties to control security level
and force unencrypted signing:

```ini
dcache.xrootd.security.level={0-4}
dcache.xrootd.security.force-signing={true,false}
```

In the case that the latter is set to true, and one anticipates there
will be xroot TPC transfers between two dCache instances or two dCache
doors, one also would need to include the unix service provider plugin
in all the relevant pool configurations:

```ini
pool.mover.xrootd.tpc-authn-plugins=gsi,unix
```

> As of 6.2, TLS is the preferred way of establishing a secured connection.  Signed has
> verification has not been officially deprecated, however, and the choice to use it is
> still available.


<!--  [???]: #intouch-web
  []: http://people.web.psi.ch/feichtinger/doc/authz.pdf
  [1]: #cf-gplazma
