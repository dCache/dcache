CHAPTER 17. dCache Frontend Service
=====================================

### Frontend RESTful Interfaces

The Frontend service is the dCache service (cell) responsible for serving
data to clients via HTTP/REST.  The default port on which it runs is 3880.
The default protocol is https (TLS).  As usual, these, as well as other
values for timeouts, enabling proxies, and anonymous user access, can be 
configured; see **/usr/share/dcache/defaults/frontend.properties** for
details.

The API delivered by the frontend is easily consulted once the service
is running.   It is provided by a Swagger page found at

    https://example.org:3880/api/v1
    
As can be seen there, these methods range over namespace access allowing users
to view files and directories, monitoring data for the dCache system, and
event subscription.   The Swagger documentation provides full descriptions of
the methods and their data types, and can be used to test the calls to the
service.  Each path also provides example `curl` commands, example responses
and error code descriptions.

In general, `HTTP GET` is available to all users.  `HTTP PUT`, `POST`, `PATCH` 
or `DELETE`, however, are limited to those who have the admin role.  For 
more on roles, see the documentation under [gPlazma](config-gplazma.md#roles).

#### The History Service

As the RESTful admin API relies in many instances on the collection of monitoring 
or state information from various backend services (pools, doors, etc.), these
activities have been separated out from the Frontend into another service,
namely, the History service.  

In order for the Frontend to be able to deliver that data through the REST 
interfaces, then, that service must be running.  Please see the relevant
section of this documentation for further 
information [History Service](config-history.md).
