# System Test

A test setup for development

## Requirements

- java runtime 11 or newer
- maven 3.1.1 or newer

## Configuration

The System test provides a basic, but rich configuration to explore dCache functionality.
> Note that the setup provides an all-in-one configuration and doesn't accept connections from any other Domains. There is no core domain configured by default.

> Also note that the embedded database only allows connections from at most one process. Cells that require databse access consequently need to be run in the same domain.

On initialization the following directories are created:

| directory    | storage group          | pool binding                   |
| -------------| ---------------------- | -------------------------------|
| `/`          | **test@default**       | non resilient write pools      |
| `/tape`      | **test:tape@osm**      | tape attached pools            |
| `/disk`      | **test:disk@osm**      | non resilient write pools      |
| `/resilient` | **test:resilient@osm** | resilient pools                |
| `/reserved`  | **test:reserved@osm**  | space manager controlled pools |
| `/public`    | **test:public@osm**    | non resilient write pools      |
| `/private`   | **test:private@osm**   | non resilient write            | 


### Pools

- tape connected pools
  - pool_write
  - pool_read
  - pool_sm

- resilient pools
  - pool_res1
  - pool_res2
  - pool_res3

- space manager connected pools
  - pool_sm

### Pool Manager setup

The pool manager is configured to split **write** and **read** pools for non resilient pools, e.g. on write file will land on a one pool, and will be copied to read pools before user can access them.

All data written into resilient storage group (/resilient directory) will be stored with two replicas.


See: [poolmanager.conf](src/main/skel/var/config/poolmanager.conf)

## Debugging

The system-test deployment stared with remote debugging enabled on port `2299`.
> Note Only connections from the local host are accepted. 
