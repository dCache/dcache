dCache as an FTP Server
=======================

Table of Contents

- [Introduction](#introduction)
- [The Control Channel](#the-control-channel)
- [Data transfers](#data-transfers)
- [Configuration examples](#configuration-examples)

This chapter explains how to configure dCache to allow FTP, a common
network protocol that many clients support.

Introduction
============

FTP is a long established protocol that allows clients to transfer
files, and manage files and directories within dCache.  FTP was
originally specified without any encryption, with later standards
adding support for encrypted communication.  FTP differs from many
other protocols by using separate TCP connections for issuing commands
(the control channel) and transferring file data (the data channel).

Various extensions to FTP exist to support additional functionality.
These are typically backwards compatible, allowing the ftp door to
work with clients that support the extension in addition to those that
don't.

The Control channel
===================

The control channel is the TCP connection established by the client
over which the client issues commands and receives replies indicating
whether those commands were successful.

In general, dCache supports four flavours of control channel: `plain`,
`tls` (also known as FTPS), `gsiftp` (also known as GridFTP), and
`kerberos`.  Each FTP door supports exactly one of these flavours.
These flavours differ in how the control channel is handled.  In plain
FTP, the control channel is unencrypted; in many cases, this is
insecure and requires additional protection.  With tls, gsiftp and
Kerberos FTP, the control channel is encrypted, preventing
eavesdropping or interfering with requests.  Authentication with tls
is based on username and password, gsiftp is based on X.509
credentials, while Kerberos FTP uses Kerberos.

Although tls and gsi FTP doors are both X.509 based, they differ in
how the encryption is handled.  Support for tls FTP is more common and
is often referred to as FTPS, FTP(E)S, FTPS-explicit or FTPES. Support
for gsi FTP is limited to grid tools.

Limiting access
---------------

The door may be configured to accept network connections only from
specific clients.  This is perhaps most useful with plain
(unencrypted) FTP, but may be used with all flavours.  The
configuration property `ftp.net.allowed-subnets` is a space-separated
list of either IP addresses of subnets (written in CIDR notation).

Directory listing
-----------------

A client may request a directory listing.  In the original FTP
specification, the format of a directory listing was unspecified.
Different FTP servers could respond in different ways.  Returning the
output of 'ls -l' became a de facto standard, although different
implementations of the 'ls' command also different in their response.

Some clients exist that attempt to parse the directory listing, using
various heuristics to guess in which format the server is replying.

dCache supports two formats: a legacy format and a format that more
closely emulates the output from 'ls -l'.  The `ftp.list-format`
configuration property controls which format is returned.

Subsequent extensions to FTP support directory listing in a precise,
prescribed format.  This extension removes any ambiguity and allows
clients to work with different servers.  dCache supports this
extension.

High-availability
-----------------

It is possible for dCache to run several ftp doors of the same type.
These doors could be accessed through a DNS alias or through an
haproxy server.

If a load-balancer is used then many support sending the client's IP
address when establishing the control channel using the ha-proxy
protocol.  If the door receives such messages then must be configured
to process such message via the `ftp.enable.proxy-protocol`
configuration property.  The door accepts both version 1 and version 2
of the ha-proxy protocol.

Concurrent connections
----------------------

The FTP door will limit the number of concurrent connections.  This is
controlled by the `ftp.limits.clients` configuration property.

Anonymous access
----------------

Anonymous access (also know as anonymous FTP or anonFTP) is a long
established practice where publicly available data is made available
to anyone who wants it.  The client authenticates with a specific
username (typically 'anonymous').  Although there is no specific
password for these accounts, it is common practice that the client
sends the user's email address as the password, as a courtesy.

dCache supports anonymous FTP for the plain and tls FTP doors.  This
is disabled by default, but may be enabled using the
`ftp.enable.anonymous-ftp` configuration property.  When enabled,
users may access dCache as user NOBODY; e.g., world-readable files may
be downloaded and world-readable directories may be listed.

dCache may be configured to expose only part of the namespace by
configuring an anonymous-ftp specific root directory.  This is
controlled by the `ftp.anonymous-ftp.root` property.  When someone
uses the anonymous FTP service then the root directory they see is
whichever path is configured in this property.

The name of the anonymous account is configured with the
`ftp.authn.anonymous.user` property.  If a regular dCache user has the
same username as this property then that dCache user will no longer be
able to log into their account via FTP username and password
authentication.  The default value is "anonymous", which is the widely
accepted account name for anonymous access: many FTP clients will use
this name automatically.

The `ftp.authn.anonymous.require-email-password` configuration
property controls whether to reject anonymous login attempts where the
password is not a valid email address.  Note that, Globus transfer
service currently sends "dummy" as the password, which is not a valid
email address.

If the plain or tls FTP door should be used only for anonymous access
then regular username and password access may be disabled by
configuring the `ftp.enable.username-password` property.  This is
perhaps most useful with plain FTP doors to prevent normal dCache
users from typing in their password unencrypted.


Data transfers
==============

Transferring a file with FTP involves establishing one (or more) TCP
connect over which the data will travel.  These are independent of the
control channel.

The TCP connection over which a file's data travels (the data
channels) are either established by dCache or by the client.
Transfers where the data channel is establish by dCache are called
active transfers; those where the client establishes the data channel
are called passive transfers.

Overwriting existing files
--------------------------

The FTP specification is clear that an upload that targets an existing
file should overwrite that file (provided the user is authorisation to
do so).  However, historically, dCache has preferred to fail such
transfers.

Currently, the `ftp.enable.overwrite` configuration property controls
whether or not dCache allows clients to overwrite existing files when
uploading data.  By default, this is allowed.

Proxies
-------

There are extensions to basic the FTP protocol (called GridFTP) that
allow the ftp door to redirect the client to the pool.  Clients that
support this GridFTP extension will advertise their support, allowing
dCache to establish a transfer directly between the client and the
pool.

If the client does not support GridFTP, or is configured not to
redirect, or if the ftp door is configured not to redirect the client
then the door will create a proxy for data transfers.  The data
channel(s) will be established between the door and the client and an
additional TCP connection is established between the door and the
pool.

There are two configuration options that control whether a proxy is
used for transfers: ftp.proxy.on-passive and ftp.proxy.on-active.
These control whether a proxy is used for passive and active data
channels, respectively.  If set to 'true' then those transfers will
always use a proxy, irrespective of whether the client supports the
GridFTP extension.

A transfer involving a proxy requires more CPU and memory on the node
hosting the ftp door.  It also increases the network traffic that the
door sees, which may become a performance bottleneck.  Therefore, it
is desirable not to proxy transfers, if possible.  If proxying is
necessary then this increased requirements may be managed by spread
the load over multiple ftp doors.

Transfer Modes
--------------

Data transfers may use one of two modes: MODE S and MODE E.

In MODE S, a single TCP connection is established to transfer a file's
data.  The TCP connection is closed once all the file's data is sent.
This is the standard way of delivering data and most commonly
supported.

MODE E is part of the GridFTP extension.  It allows multiple TCP
connection to be used when transferring a file's data.  It also allows
those TCP connections to be reused when sending multiple files.

The configuration property `ftp.limits.streams-per-client` controls
the maximum number of TCP connections that dCache will establish for
active MODE E transfers.  For passive MODE E transfers, the client
establishes the data channels, so dCache cannot control the number of
streams.

For proxied transfers involving MODE E, dCache will try to keep any
established data channels open.  This is an optimisation especially
for smaller files, where the time needed to establish a data channel
is a significant compared to the time needed to transfer the file's
data.

Logging aborted transfers
-------------------------

An aborted transfer is one where the client requested a new file be
uploaded or all data from an existing file be downloaded, but only
part of the file's contents were transferred.  Such aborted transfers
normally indicate a problem.  As the problem may be transitory, it may
not be clear what caused it and therefore it may be hard to recreate
the problem.

To aid in fixing aborted transfers, the door can provide considerable
information about a transfer at the point the transfer is aborted.
This is currently disabled by default, but may be controlled with the
`ftp.enable.log-aborted-transfers` configuration property.


Configuration examples
======================

Hosting an FTP server in a domain is as simple as:

    ..
    [<domainName>/ftp]
    ..

The 'ftp.authn.protocol' configuration property controls which flavour
of FTP server should be started; for example, to start a gsiftp server:

    ..
    [<domainName>/nfs]
    ftp.authn.protocol = gsi
    ..

There are distinct default TCP ports on which the different flavours
of FTP server will listen, so a single host may run multiple FTP doors
without requiring any port configuration:

    ..
    [<domainName>/nfs]
    ftp.authn.protocol = plain
    [<domainName>/nfs]
    ftp.authn.protocol = gsi
    [<domainName>/nfs]
    ftp.authn.protocol = kerberos
    [<domainName>/nfs]
    ftp.authn.protocol = tls
    ..


