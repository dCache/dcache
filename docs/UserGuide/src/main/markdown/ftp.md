Chapter 4. FTP
==============

The File Transfer Protocol (FTP) is a standard network protocol used
for the transfer of computer files between a client and server on a
computer network.  dCache supports varous extensions allow you to list
files with FTP and manage them.

## GridFTP ##

The GridFTP protocol extensions support a number of advanced features.
On of these features is to protect the control channel and allow the client
to negotiate a secure data channel, as described in RFC 2228.

dCache supports GridFTP with a secure control channel, but does *not* support
a secure data channel: all data is transferred unencrypted.

> **IMPORTANT**
> 
> The `globus-url-copy` command supports the options `-dcpriv` (or,
> equivalently, `-data-channel-private`) that the man page describe as:
> > Set data channel protection mode to PRIVATE
>
> There is a bug where a transfer with the `-dcpriv` (or
> `-data-channel-private`) option specified will succeed with dCache, but
> the file's contents is sent unencrypted over the network.
>
> Therefore we recommend that you *NEVER* use the `-dcpriv` or the
> `-data-channel-private` option as it's behaviour is unreliable.
>
> For more details, see the original report
> [GT issue #121](https://github.com/globus/globus-toolkit/issues/121),
> and the status under GTC
> [GCT issue #62](https://github.com/gridcf/gct/issues/62).
