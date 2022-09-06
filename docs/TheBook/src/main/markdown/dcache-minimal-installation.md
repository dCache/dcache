
   
     
### Minimum System Requirements

   #### Hardware:
- Contemporary CPU
- At least 1 GiB of RAM
- At least 500 MiB free disk space
   
 #### Software:
- OpenJDK 11
 > yum install java-11-openjdk
- Postgres SQL Server 9.5 or later

- ZooKeeper version 3.5 (in case of a standalone ZooKeeper installation)

### Installing PostgreSQL

To keep this simple, we are assuming the database will run on the same machine as the dCache services that
use it.

The simplest configuration is to allow password-less access to the database. Hier we assumes this to be the case.

To allow local users to access PostgreSQL without requiring a password, make sure the following three lines
are the only uncommented lines in the file **/var/lib/pgsql/10/data/pg_hba.conf**

      ...
    # TYPE  DATABASE    USER        IP-ADDRESS        IP-MASK           METHOD
    local   all         all                                             trust
    host    all         all         127.0.0.1/32                        trust
    host    all         all         ::1/128                             trust    
   
   
   ### Creating PostgreSQL users and databases    


> createuser -U postgres --no-superuser --no-createrole --createdb --no-password dcache
> 
> createdb -U dcache chimera

And run the command `dcache database update`.



### Installing dCache

All dCache packages are available directly from our website’s dCache releases page, under the Downloads
section.

>     
>    rpm -ivh https://www.dcache.org/old/downloads/1.9/repo/7.2/dcache-7.2.19-1.noarch.rpm 



### Configuration files

In the setup of dCache, there are three main places for configuration files:

-   **/usr/share/dcache/defaults**
-   **/etc/dcache/dcache.conf**
-   **/etc/dcache/layouts**

The folder **/usr/share/dcache/defaults** contains the default settings of the dCache. If one of the default configuration values needs to be changed, copy the default setting of this value from one of the files in **/usr/share/dcache/defaults** to the file **/etc/dcache/dcache.conf**, which initially is empty and update the value.

### Four main components in dCache
-------------

All components are CELLs and they are independent and can interact with each other by sending messages.
For the minimal instalation of dCache the following cells must be configured in **/etc/dcache/dcache.conf** file.


#### DOOR 
 - User entry points (WebDav, NFS, FTP, DCAP, XROOT) 
 
#### PoolManager
- The heart of a dCache System is the poolmanager. When a user performs an action on a file - reading or writing - a transfer request is sent to the dCache system. The poolmanager then decides how to handle this request.

#### PNFSManager
- The namespace provides a single rooted hierarchical file system view of the stored data.
- metadata DB, POSIX layer

#### POOL
 - Data storage nodes, talk all protocols

Billing cell not clear?

#### Zookeeper
 - A distributed directory and coordination service that dCache relies on.




# Grouping CELLs - Single process:

- Shared JVM
- Shared CPU
- Shared Log files
- All components run the same version
- A process called DOMAIN


```ini
dcache.layout = mylayout
```

Now, create the file `/etc/dcache/layouts/mylayout.conf` with the
following contents:

```ini
dcache.enable.space-reservation = false

[dCacheDomain]
 dcache.broker.scheme = none
[dCacheDomain/zookeeper]
[dCacheDomain/pnfsmanager]
 pnfsmanager.default-retention-policy = REPLICA
 pnfsmanager.default-access-latency = ONLINE

[dCacheDomain/poolmanager]
[dCacheDomain/billing]
[dCacheDomain/webdav]
 webdav.authn.basic = true
 

```

> **NOTE**
>
> In this first installation of dCache your dCache will not be connected to a tape sytem. 
> Therefore the values for pnfsmanager.default-retention-policy and pnfsmanager.default-access-latency must be changed in the file **/etc/dcache/dcache.conf**. ????


>
>     pnfsmanager.default-retention-policy=REPLICA
>     pnfsmanager.default-access-latency=ONLINE


Now we can add a new cell: Pool using the following command.

 > dcache pool create /srv/dcache/pool-A poolA dCacheDomain

The `dcache` script provides an easy way to create the pool directory
structure and add the pool service to a domain.  In the following
example, we will create a pool using storage located at
`/srv/dcache/pool-1` and add this pool to the `dCacheDomain` domain.

```console-root
mkdir -p /srv/dcache
dcache pool create /srv/dcache/pool-1 pool1 dCacheDomain
|Created a pool in /srv/dcache/pool-1. The pool was added to dCacheDomain
|in file:/etc/dcache/layouts/mylayout.conf.
```

Now if we open  `/etc/dcache/layouts/mylayout.conf` file, it should be updated to have
an additional `pool` service:

```ini
dcache.enable.space-reservation = false
[dCacheDomain]
 dcache.broker.scheme = none
[dCacheDomain/zookeeper]
[dCacheDomain/pnfsmanager]
 pnfsmanager.default-retention-policy = REPLICA
 pnfsmanager.default-access-latency = ONLINE

[dCacheDomain/poolmanager]
[dCacheDomain/webdav]
 webdav.authn.basic = true

[dCacheDomain/pool]
pool.name=pool1
pool.path=/srv/dcache/pool-1
pool.wait-for-files=${pool.path}/data
```


## Starting dCache

There are two ways to start dCache: 1) using sysV-like daemon, 2) Using systemd service.

##### Using sysV -like daemon

 The the 2nd one is preferred and enforced by default when the hosts operating system supports it. To change this behavior set

dcache.systemd.strict=false

> dcache start

> dcache status


The domain log file (/var/log/dcache/dCacheDomain.log) also contains some details, logged as dCache
starts

Using systemd service dCache uses systemd’s generator functionality to create a service for each defined
domain in the layout file. That’s why, before starting the service all dynamic systemd units should be
generated:

  > systemctl daemon-reload 


To inspect all generated units of dcache.target the systemd list-dependencies command can be used. In
our simple installation with just one domain hosting several services this would look like

> systemctl list-dependencies dcache.target
> systemctl status dcache@* 

```console-root
systemctl list-dependencies dcache.target
|dcache.target
|● ├─dcache@coreDomain.service
|● ├─dcache@NamespaceDomain.service
|● ├─dcache@zookeeperDomain.service
|● ├─dcache@poolDomain.service
|● └─dcache@poolmanagerDomain.service



```

In the next  step, we will split the services into three domains.
There is nothing special about having three domains. At one extreme, dCache will work correctly if everything
runs in a single domain. On the other extreme, you can configure dCache to run each service in a separate
domain. Although the latter deployment is the most flexible, there is some overhead in having many domains,
so the optimal approach is usually somewhere in between these two extremes.
 The difference is that, when a dCache instance
spans multiple domains, there needs to be some mechanism for sending messages between services located
in different domains.

This is done by establishing tunnels between domains. A tunnel is a TCP connection over which all messages
from one domain to the other are sent.
To reduce the number of TCP connections, domains may be configured to be core domains or satellite
domains. Core domains have tunnels to all other core domains. Satellite domains have tunnels to all core
domains.
The simplest deployment has a single core domain and all other domains as satellite domains. This is a
spoke deployment, where messages from a service in any satellite domain is sent directly to the core domain,
but messages between services in different satellite domains are relayed through the core domain.

# Grouping CELLs - In different processes:

 - Independent JVMs
 - Shared CPU
 - Per-process Log file
 - All components run the same version (you can run different versions if needed)


```ini
dcache.enable.space-reservation = false

[corelDomain]
dcache.broker.scheme = core
[corelDomain/zookeeper]
[corelDomain/pnfsmanager]
 pnfsmanager.default-retention-policy = REPLICA
 pnfsmanager.default-access-latency = ONLINE

[corelDomain/poolmanager]

[doorsDomain]
[doors/webdav]
 webdav.authn.basic = true
 
[poolsDomainA]
[poolsDomainA/pool]
pool.name=pool1
pool.path=/srv/dcache/pool-1
pool.wait-for-files=${pool.path}/data
```

[poolsDomainB]
[poolsDomainB/pool]
pool.name=pool1
pool.path=/srv/dcache/pool-1
pool.wait-for-files=${pool.path}/data
```





```ini
dcache.enable.space-reservation = false

[corelDomain]
dcache.broker.scheme = core
[corelDomain/zookeeper]
[corelDomain/pnfsmanager]
 pnfsmanager.default-retention-policy = REPLICA
 pnfsmanager.default-access-latency = ONLINE

[corelDomain/poolmanager]

[doorsDomain]
[doors/webdav]
 webdav.authn.basic = true
 
[poolsDomainA]
[poolsDomainA/pool]
pool.name=pool1
pool.path=/srv/dcache/pool-1
pool.wait-for-files=${pool.path}/data
```

[poolsDomainB]
[poolsDomainB/pool]
pool.name=pool1
pool.path=/srv/dcache/pool-1
pool.wait-for-files=${pool.path}/data
```

```ini
dcache status
|DOMAIN
|centralDomain
|doorsDomain
|poolsDomain
STATUS PID USER
LOG
stopped
dcache /var/log/dcache/centralDomain.log
stopped
dcache /var/log/dcache/doorsDomain.log
stopped
dcache /var/log/dcache/poolsDomain.log

```



# Grouping CELLs - On a different hosts:
- Share-nothing option
- Components can run different, but compatible versions.

```ini

[core-${host.name}]



[core-${host.name}/poolmanager]

[core-${host.name}/pnfsmanager]


[core-${host.name}/nfs]
chimera.db.url=jdbc:postgresql://${chimera.db.host}/${chimera.db.name}?prepareThreshold=3&targetServerType=master&ApplicationName=${nfs.cell.name}

[admin]
[core-${host.name}/admin]
```






    
