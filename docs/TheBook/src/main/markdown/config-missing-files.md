THE MISSING FILES SERVICE
=========================

Introduction
============

When a user requests to read a file that doesn't exist, 
the door will respond to the user with the appropriate error message.

There are two use-cases where something more sophisticated is
desirable.  The first is that dCache should notify one or more
external services that it was requested to read a file that doesn't
exist.  The second is that dCache will populate the missing file from
some external source, allowing the read request to succeed.  There
may be other examples where it is desirable for dCache to react to
failed read attempts.

The `MissingFiles` service is a dCache component that perform these tasks.  
It exists to allow dCache to react in a coordinated fashion to a user's request to
read a file that doesn't exist.  This central service instructs the
door to either fail the request or retry (which makes sense only if
the file has been fetched from some external source). 
Currently only the `WebDAV` door provides this supports interaction with 
`MissingFile` service. The interaction is enabled by setting :
```
   dcache.enable.missing-files = true
```
The `MissingFiles` service is pluggable.  It takes a list of plugins
and instantiates them.  These plugins are used to determine how
dCache should react when a user attempts to read a missing file.
Each plugin is asked in turn what to do until a plugin replies with a
terminating answer or the list of plugins is exhausted.  A plugin
replies saying to fail the request, to retry the request or to ask
the next plugin in the chain.  If the last plugin defers the request
then then the MissingFiles service will instruct the door to fail the
request.

Setting up
==========

To instantiate `MissignFiles` service the following line needs to be added to your 
layout file.  
```
   ..
   [<domainName>/missing-files]
   ..
```

Additionally the service must be enabled:
```
   dcache.enable.missing-files = true
```
The behavior of `MissignFiles` service is determined by plugins that need 
to be specified as comma separated names:
```
   missing-files.plugins =
``` 
Currently no plugins are available and `MissingFiles` service simply  instructs the door 
to fail the request. 

