The intent of this guide is to provide you with a minimal working
dCache instance which you can explore. However, please note that there are many ways to configure
dCache and your production instance will be more complex. The optimal choice depends on which hardware you wish to use
and how dCache's users will interact with the system.  There is no one size fits all.

# Minimal dCache Installation Guide

### Minimum System Requirements

- Hardware:
  - Contemporary CPU
  - At least 1 GiB of RAM
  - At least 500 MiB free disk space

- Software:
  - OpenJDK 17
  - Postgres SQL Server 13.0 or later
  - ZooKeeper version 3.5 (embedded)

## Minimal Installation

### Java 
For this installation, we use OpenJDK 17.

```bash
sudo -s

dnf install java-17-openjdk

dnf install java-17-openjdk-devel
```
For a production installation, you will need standalone ZooKeeper version 3.8 or later.  

### PostgreSQL
    
For this installation, we assume the database will run on the same machine as the dCache services that
use it.

Start by installing PostgreSQL version 13.0 or later

```bash
dnf -y install postgresql-server
```

Initialise the database service, note that we **do not start** the database at this point, as we will make some tweaks to the configuration.

```bash
[root@neic-demo-1 centos]#  postgresql-setup --initdb
Initializing database in '/var/lib/pgsql/data'
Initialized, logs are in /var/lib/pgsql/initdb_postgresql.log
```

Make the database passwordless for this minimal installation. Inside the pg_hba.conf file in **/var/lib/pgsql/data/pg_hba.conf** comment out all lines except for 

```bash
vi /var/lib/pgsql/data/pg_hba.conf
```

    # TYPE  DATABASE    USER        IP-ADDRESS        IP-MASK           METHOD
    local   all         all                                             trust
    host    all         all         127.0.0.1/32                        trust
    host    all         all         ::1/128                             trust    

Then, start the database

```bash
systemctl enable --now postgresql 
```

For a production installation, we recommend using the latest version and upgrading your PostgreSQL version as new versions become available.
Remember to tune your PostgreSQL database for optimal performance depending on available hardware.
Without doing this, you will see poor performance as PostgreSQL
typically experiences low performance with its default
configuration.  In general, the database may be deployed on the same node as dCache or
on some dedicated machine with db-specific hardware.

    
#### Creating PostgreSQL user and database

```bash
systemctl reload postgresql
```

dCache will manage the database schema by creating and updating the
database tables, indexes etc. as necessary. However, dCache does not
create the database.

Start by creating a user, we will call it `dcache`
```bash
createuser -U postgres --no-superuser --no-createrole --createdb --no-password dcache
```

Create the database using `dcache` to ensure correct database ownership.  At this stage, we need only one database, `chimera`. This database holds dCache's namespace.

```bash
createdb -U dcache chimera
```

### dCache

### Four main components

All components in dCache are cells. They are independent and can interact with each other by sending messages just like microservices with message queues.

##### Door 
User entry points (WebDav, NFS, FTP, DCAP, XROOT protocols) 
 
##### PoolManager
The heart of a dCache System. When a user performs an action on a file, reading or writing, a transfer request is sent to the dCache system. Poolmanager then decides how to handle this request.

##### PNFSManager
Interface to a file system name space that maps dCache name space operations to file-system operations (Metadata DB, POSIX layer)

##### Pool
Service responsible for storing the contents of files. There must be always at least one pool. They talk all protocols.

In addition to the four main cells you need an access protocol and Zookeper for cell communication.

##### Zookeeper
A distributed directory and coordination service that dCache relies on.


### Installation

dCache's current golden release is 10.2

```bash
rpm -ivh https://www.dcache.org/old/downloads/1.9/repo/10.2/dcache-10.2.19-1.noarch.rpm
```    

```bash
Retrieving https://www.dcache.org/old/downloads/1.9/repo/9.2/dcache-9.2.19-1.noarch.rpm
warning: /var/tmp/rpm-tmp.gzWZPS: Header V4 RSA/SHA256 Signature, key ID 3321de4c: NOKEY
Verifying...                          ################################# [100%]
Preparing...                          ################################# [100%]
Updating / installing...
   1:dcache-9.2.19-1                  ################################# [100%]
```

### Configuration files

There are three important configuration files:

**/usr/share/dcache/defaults**  for default properties files  
**/etc/dcache/dcache.conf**  for main configuration file of dCache  
**/etc/dcache/layouts** for  properties/configuration updates

#### Configuration single process / one domain (simplest installation):

- Shared JVM
- Shared CPU
- Shared log files
- All components run the same dCache version
- A process called DOMAIN?
 
Update `dcache.conf` by appending the following line

```ini
dcache.layout = mylayout
```

Create the `mylayout.conf` file inside the layouts directory and add the following contents

```ini
dcache.enable.space-reservation = false

[dCacheDomain]
 dcache.broker.scheme = none

[dCacheDomain/admin]
admin.paths.history=${host.name}/var/admin/history

[dCacheDomain/zookeeper]

[dCacheDomain/pnfsmanager]
 pnfsmanager.default-retention-policy = REPLICA
 pnfsmanager.default-access-latency = ONLINE

[dCacheDomain/poolmanager]

[dCacheDomain/gplazma]
gplazma.oidc.audience-targets=https://wlcg.cern.ch/jwt/v1/any
gplazma.oidc.provider!wlcg=https://wlcg.cloud.cnaf.infn.it/ -profile=wlcg -prefix=/ -suppress=audience

[dCacheDomain/webdav]
webdav.cell.name=WebDAV-${host.name}
#webdav.authn.basic = true
webdav.authn.protocol=http
webdav.authz.anonymous-operations=READONLY

```

**Notes**

- In this installation, dCache will have only one domain, and it will not be connected to a tape system, hence, the values of retention-policy and access-latency are replica and online respectively. Additionally, `dcache.broker.scheme = none` tells dCache it is running stand-alone instance with no additional domains.
- SpaceManager uses space reservation which is by default enabled, but for this minimal installation we change it to false. 
- Authorization is achieved by oidc plugin inside the gplazma component.
- Our protocol, webdav, will allow share, migration, edit, copy, etc. of files.


We make use of a dCache script to create pool1 in `/srv/dcache/pool-1` and dCache will add this pool to the pool service inside the dCache domain automatically.

```bash
dcache pool create /srv/dcache/pool-1 pool1 dCacheDomain
```
 

```ini
Created a pool in /srv/dcache/pool-1. The pool was added to dCacheDomain
in file:/etc/dcache/layouts/mylayout.conf.
```

Open mylayout.conf to verify pool1 was added.

```ini
[dCacheDomain/pool]
pool.name=pool1
pool.path=/srv/dcache/pool-1
pool.wait-for-files=${pool.path}/data
```
### Start dCache

Using systemd service dCache creates a service for each defined domain in the mylayout.conf file. Before starting the service all dynamic systemd units should be generated.

```bash
systemctl restart dcache.target
```

To inspect all generated units in dcache.target 
```bash
systemctl list-dependencies dcache.target
```

In our simple installation we have one domain hosting several services that look like this

```console-root
systemctl list-dependencies dcache.target
|dcache.target
|● ├─dcache@dCacheDomain.service

```

### Useful commands
Finally, we will leave you with some important commands to get you going.

```bash
systemctl status dcache@*
systemctl restart dcache.target 
systemctl stop dcache.target
journalctl -f -u dcache@dCacheDomain
```

## Next steps

Now that you have learned how to install and start dCache, we encourage you to play around with the admin interface and run your own commands, add or split services into different domains, and work with diverse authentication methods and protocols. For a more in depth dCache installation, please refer to [Chapter 2. Installing dCache](install.md).

Happy dCaching!

