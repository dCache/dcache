Chapter 8. Xroot
=================

**Table of Contents**

+ [Authentication](#authentication)
+ [Third-party transfers](#third-party-transfers)
    + [Protecting third-party data using TLS](#authenticating-the-third-party-request)

[This chapter is under construction.]

## Authentication

Now that the dCache xroot door supports multiple authentication protocols, clients with
authentication credentials of distinct types (such as an x509 proxy or an OIDC token)
no longer need to contact separate doors.

However, if the client (meaning, in most cases, `xrdcp` or `gfal`) is holding more than
one such valid credential at the same time, a complication may arise.

Suppose that, in the client environment, there exists both a VOMS proxy at the standard
location, as well as an OIDC token in the location indicated by the XrootD env var
`BEARER_TOKEN_FILE`.  Suppose, further, that the user's home is defined in the target
dCache installation to be `/home/username`, but that the OIDC token authorizes the
user to write at `/home/organization/username`.

When the client contacts the door, it will get back an ordered list of authentication
protocols to try.  The client will try them in order, and will be logged in with the
first successful validation.  If an attempt fails, it will move on to the next protocol
in the list.  (If the client cannot find an appropriate credential in its environment
for a given protocol, it will skip that protocol and move on to the next.)

With more than one credential, this means that what the client can access actually will
depend on the way the protocols are ordered by the door.  If the door says `ztn`
(i.e., OIDC token), then `gsi`, the client with both valid types available will always
be logged in with the OIDC permissions; vice versa for the case where the door says `gsi` first.
Consequently, if the permissions for the two types of credentials held by the user are mutually
exclusive, the user will never be able to get authorized to use the second set. Specifically,
using our example:  a `ztn`-first door will never permit the user to access `/home/username`,
and a `gsi`-first door will never permit the user to access `/home/organization/username`.

NOTE that this is not the case if the client is given only one type of credential at a time.
In this case, everything works as it should:  the client with an OIDC token will be authorized
on `/home/organization/username`, and the client with an x509 proxy on `/home/username`, by
the same door.

There is a workaround, however, which will allow users to set up their environment with both
credentials and not have to destroy and recreate them in order to get the correct permissions.
In order to guarantee the correct credential is used, set:

```
export XrdSecPROTOCOL=ztn
```

to use the OIDC token, and

```
export XrdSecPROTOCOL=gsi
```

to use the x509 proxy.


## Third-party transfers

### Protecting third-party data using TLS

The dCache implementation of the xroot protocol currently lacks
channel splitting (where control requests are on one connection and the actual data
is sent on a separate one).  This means that if TLS is turned on for a given connection,
the data transfer itself will also be encrypted on it.

It is for this reason that we suggest that TLS on dCache pools should be set to OPTIONAL,
since data encryption may not always be desired (admins should consult further The Book
chapter on xroot for the properties pertinent to TLS activation).  Making the pool
require TLS effectively means that all xroot reads and writes to that pool are encrypted.

For two-party xroot transfers, the way for the client to guarantee encryption of data is
to use the `(x)roots` protocol rather than `(x)root` in the path URL.  This may actually
already be required of the user if the door does not enforce TLS but the user tries to
authenticate using a token.

For third-party xroot transfers, expressing `(x)roots` on the *_source URL_* will also
turn on encryption for the third-party data connection.
