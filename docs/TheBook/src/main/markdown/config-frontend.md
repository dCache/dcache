CHAPTER 17. dCache Frontend Service
===================================

-----
[TOC bullet hierarchy]
-----

The Frontend service is the dCache service (cell) responsible for
serving data to clients via HTTP/REST.  The default port on which it
runs is 3880.  The default protocol is https (TLS).  As usual, these,
as well as other values for timeouts, enabling proxies, and anonymous
user access, can be configured; see
`/usr/share/dcache/defaults/frontend.properties` for details.

The API delivered by the frontend is easily consulted once the service
is running.   It is provided by a Swagger page found at

    https://example.org:3880/api/v1

As can be seen there, these methods range over namespace access, allowing users
to view files and directories, monitoring data for the dCache system, and
event subscription.   The Swagger documentation provides full descriptions of
the methods and their data types, and can be used to test the calls to the
service.  Each path also provides example `curl` commands, example responses
and error code descriptions.

## Configuring the Frontend service

Some in-memory caching is done by this service in order to optimize the
delivery of monitoring/administrative data, but the memory requirements
are not excessive (it is just text/JSON).  Add the service to an existing
domain or create a separate one for it:

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
on the history service for its histogram data.  Without that service, you
will not be able to request time-related statistics for billing, pool queues or
file lifetime.  The plots generated from this data by dCache-View will also
not be available.  Please refer to the section on
the [dCache History Service](config-history.md) for configuration
(which is very simple).

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

If you wish all authenticated (non-anonymous) users to be be able to see
the full range of file-related information through either the RESTful api
or in dCache-View, this property should be set to _true_.

## Configuring and using the _admin_ role

The above property has to do with `HTTP GET`.  `HTTP PUT`, `POST`, `PATCH`
or `DELETE`, however, are always limited to those who have the _admin role_.
Hence, this role must be defined for the dCache installation.  Please see
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
Type in the user name and password, and check the box which says "assert all roles".

See the [dCache-View] documentation for further information.

##### A Note on the RESTful API for tape restores

The data retrieved via the REST path

```
/api/v1/restores ...
```

corresponds to the admin command

```
\sp rc ls
```

for all available pool managers.   This means that the restores listed in the
output are those initiated by an actual protocol through a door.  The restore
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

##### A Note on the RESTful resource for QoS transitions

As of dCache 6.1, we have begun to integrate management of QoS with
services such as PnfsManager and Resilience.  The Frontend resource
has now been modified to send messages requesting ACCESS LATENCY
and RETENTION POLICY changes to PnfsManager, which then broadcasts
the resulting change, to which Resilience will respond.

This is only a partial, first-step toward a full-blown QoS infrastructure,
but at least allows Resilience to manage such transitions for resilient
files.  For other files, the current mechanism remains (QoS state maintained
by the Frontend resource).
