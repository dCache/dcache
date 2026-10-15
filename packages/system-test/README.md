# System Test

A test setup for development

## Requirements

- java 11 or newer
- maven 3.5.0 or newer

## Configuration

The System test provides a basic, but rich configuration to explore dCache functionality.
> Note that the setup provides an all-in-one configuration and doesn't accept connections from any other Domains. There is no core domain configured by default.

> Also note that the embedded database only allows connections from at most one process. Cells that require database access consequently need to be run in the same domain.

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

The pool manager is configured to split **write** and **read** pools for non resilient pools, e.g. on write a file will land on one pool, and will be copied to read pools before a user can access them.

All data written into the resilient storage group (/resilient directory) will be stored with two replicas.


See: [poolmanager.conf](src/main/skel/var/config/poolmanager.conf)

### S3 doors

Two S3 doors serve the whole namespace: every top-level directory is a
bucket. Port `9000` uses HTTP, port `9001` HTTPS with the system-test host
certificate. Both accept the access key `AKIASYSTEMTEST` with the secret
`system-test-secret`, which gPlazma maps to the user running the system test.

```
rclone --s3-provider Other --s3-endpoint http://localhost:9000 \
       --s3-access-key-id AKIASYSTEMTEST --s3-secret-access-key system-test-secret \
       ls :s3:public
s3cmd --host localhost:9000 --host-bucket localhost:9000 --no-ssl \
      --access_key AKIASYSTEMTEST --secret_key system-test-secret ls s3://public
```

Files in `/tape` that are only on tape are listed with storage class
`GLACIER` and must be restored (`RestoreObject`) before they can be read.

## Debugging

The system-test deployment is started with remote debugging enabled on port `2299`.
> Note Only connections from the local host are accepted. 
