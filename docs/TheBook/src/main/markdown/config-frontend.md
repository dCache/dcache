CHAPTER 16. dCache Frontend Service
===================================

-----
[TOC bullet hierarchy]
-----

The Frontend service is a dCache cell responsible for
serving data to clients via HTTP/REST.  The default port on which it
runs is 3880.  The default protocol is https (TLS). These values, along with other settings such as timeouts, proxy configuration, and anonymous
user access, can be configured in the frontend.properties file.

```init
/usr/share/dcache/defaults/frontend.properties
```

Once the service is running, the REST API documentation can be viewed at:

    https://example.org:3880/api/v1

The Swagger documentation provides a list of available REST endpoints with full descriptions of
the methods and their data types.These methods range over namespace access, allowing users
to view files and directories, monitoring data for the dCache system, and
event subscription. Each path also provides example `curl` commands, example responses
and error code descriptions. As well as the ability to test API calls directly from the browser.

## Configuring the Frontend service

The frontend performs some in-memory caching to reduce the load on
backend services when serving monitoring or administrative data.
The cached information consists mainly of text or JSON and
therefore does not require significant memory.
The service can be added to an existing domain or a separate domain can be created for it:

```ini
[frontendDomain]
[frontendDomain/frontend]
```

The service can be run out-of-the-box without changing default property
values.  There are a few properties affecting the admin/monitoring components
which should, however, be noted.

## Properties controlling monitoring data collection

The number of threads which are available to collect data from
the pools is set to 10:

```ini
# Used for processing updates on messages returned from pools
frontend.service.pool-info.update-threads=10
```

This should usually be sufficient, but it is possible that for extremely
large numbers of pools more threads may be necessary.  One could, alternatively,
increase the refresh interval for pool data collection.

When RESTful calls are made for admin or monitoring information, some of them
translate into a direct (blocking) call to a backend service to deliver
the data to the frontend, which then delivers it to the REST client.  These
"pass through" calls usually involve queries concerning a specific file (pnfsid)
to either the namespace or billing.   The remainder of the data, however,
is served from cached data which the frontend has collected from backend
services.  How often this data is collected can be controlled by adjusting timeout
properties (for alarms, billing, cells, pools, transfers, restores, history)
in the configuration files, or directly through the admin interface.

Aside from collecting data directly from the pools, the frontend also relies
on the [history service](config-history.md) for its histogram data.  Without that service, you
will not be able to request time-related statistics for billing, pool queues or
file lifetime.  The plots generated from this data by dCache-View will also
not be available.  Please refer to the documentation under the [dCache History Service](config-history.md) for how to set
this up.

## Last-Modified header in monitoring responses

GET responses for monitoring and admin data include a `Last-Modified` HTTP
header indicating when the cached snapshot was last updated.  This applies
to the following endpoints:

- `/api/v1/alarms`
- `/api/v1/billing`
- `/api/v1/cells`
- `/api/v1/domains`
- `/api/v1/pools`
- `/api/v1/restores`
- `/api/v1/transfers`

The header value reflects the timestamp of the most recent background
collection cycle that refreshed the data.

## Properties controlling monitoring data access

The following property should be noted.

```ini
#  ---- Determines whether operations exposing file information
#       can be viewed by non-admin users.
#
#       When false (default), restores, queue operations on the pools,
#       billing records, and transfers which are not owned by the user
#       and are not anonymous can only be seen by admins.
#
#       Setting this value to true allows all users access
#       to this information.
#
(one-of?true|false)frontend.authz.unlimited-operation-visibility=false
```

This property controls whether non-admin users can view operations involving files owned by other users through either the RESTful api
or in dCache-View. When assigned to false (default), non-admin users can only view their own operations.


## Configuring and using the _admin_ role

The above property has to do with HTTP methods `GET`.  `PUT`, `POST`, `PATCH`
or `DELETE`, however, these are always limited to those who have the _admin role_.
Hence, this role must be defined for the dCache installation.  Please refer to
the documentation under [gPlazma](config-gplazma.md#roles) for how to set
this up.

When issuing a ```curl``` command, one can indicate the role using a '#'
after the username; e.g.,

```console
curl -k -u arossi#admin https://fndcatemp1.fnal.gov:3880/api/v1/restores
Enter host password for user 'arossi#admin':
```

Note that currently, the assertion of the admin role requires a password.
We realize that this extra step is clunky and we are working
on allowing role assertion on the basis of the credential.

For the moment, however, you will need to add a .kpwd module to your
gplazma setup and enable login and passwd entries for the user in question.
Examples of how to do this may be found in the gPlazma section of this
document; see, for instance,
[Enabling Username/Password Access for WebDAV](#enabling-username-password-access-for-webdav).

The same procedure applies when enabling the admin role in dCache-View.
At the upper right hand corner of the dCache-View landing page,
you will see the user icon.  Click on it and select "add another credential"
Type in the username and password, and check the box which says "assert all roles".

See the [dCache-View](https://github.com/dCache/dcache-view/blob/master/README.md) documentation for further information.

##### RESTful API for tape restores

The data retrieved via REST path

```
/api/v1/restores ...
```

corresponds to the admin command

```
\sp rc ls
```

for all available pool managers.   This means that the restores (stages) listed in the
output are those initiated by an actual protocol through a door.  The restore (stage)
initiated by the pool command:


```
\s <pool> rh restore <pnfsid>
```

does not show up in this list because the pool manager knows nothing about it.

In order to get all the restores (stages) on a given pool, the REST path

```
/api/v1/pools/{pool}/nearline/queues?type=stage
```

must be used.

## OpenID Connect for dCache View

dCache View can log users in with the OpenID Connect authorization
code flow.  Configure the client on the frontend cell:

```ini
[dCacheDomain/frontend]
frontend.authn.oidc.issuer = https://op.example.org
frontend.authn.oidc.client-id = <client-id>
frontend.authn.oidc.client-secret = <client-secret>
frontend.static!dcache-view.oidc-provider-name-list = ExampleOP
frontend.static!dcache-view.oidc-client-id-list = <client-id>
```

The frontend then fetches `{issuer}/.well-known/openid-configuration`
and takes `authorization_endpoint` (shown on the View login page) and
`token_endpoint` (used to exchange the authorization code).  You can
still set `frontend.static!dcache-view.oidc-authz-endpoint-list` and
`frontend.authn.oidc.token-url` explicitly; those values override
discovery.

The redirect URL sent with the authorization request is never
configured: dCache View derives it itself, from the browser's own
origin plus `dcache-view.endpoints.webapi` (i.e.
`{origin}/api/v1/auth/callback`), which always matches the callback
the frontend serves.

For more than one provider, give space-separated issuer URLs in
`frontend.static!dcache-view.oidc-issuer-list` in the same order as
the name and client-id lists.  The code-flow token exchange still uses
a single token endpoint (`frontend.authn.oidc.issuer` /
`frontend.authn.oidc.token-url`).

##### RESTful API for QoS transitions

The RESTful commands now communicate with the [QoS Engine](config-qos-engine.md)
