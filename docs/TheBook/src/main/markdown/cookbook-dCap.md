DCAP
=========

DCAP is the native random access I/O protocol for files within dCache.
In addition to the usual data transfer mechanisms, it supports all
necessary file metadata and name space manipulation operations.

-----
[TOC bullet hierarchy]
-----

## Setting up

To allow file transfers in and out of dCache using the DCAP protocol, a new DCAP should be enabled in the layout file corresponding to your dCache-instance.

```ini
[<domainName>/dcap]
```

## Network operation

TThe DCAP protocol, similar to other access protocols offered by dCache, uses a control and data channel separation. Before dCache version 9.0, by default the data channel was initiated by the pool, connecting to an open port in the client library. Starting with dCache 9.0, the DCAP door **always** starts a data mover in `passive mode`, which consequently means that the client connects to the pool to initiate the data channel.

## Authentication in DCAP

The DCAP door supports multiple authentication mechanisms; this is
controlled by the `dcap.authn.protocol` property. The property accepts the following values:

- plain - no authentication and control channel protection
- auth - username+password -based authentication over TLS enabled control connection
- kerberos - kerberos5-based authentication
- gsi - GRID proxy-based user authentication

## Conficuration properties

The list of all available configuration properties controlling DCAP door behavior
can be found in the `share/defaults/dcap.properties` property file.

## Example setup

```
[doorDomain/dcap]
dcap.authn.protocol=plain
dcap.authz.anonymous-operations = FULL

[doorDomain/dcap]
dcap.authn.protocol=auth

[doorDomain/dcap]
dcap.authn.protocol=gsi

```
