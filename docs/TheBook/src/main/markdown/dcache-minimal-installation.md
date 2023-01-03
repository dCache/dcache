
 dCache mission is  to provide a system for storing and retrieving huge amounts of data, distributed among a large number of heterogeneous
server nodes, under a single virtual filesystem tree with a variety of standard access methods.
     
### Minimum System Requirements

   #### Hardware:
- Contemporary CPU
- At least 1 GiB of RAM -???
- At least 500 MiB free disk space -???
   
 #### Software:
- OpenJDK 11
 > yum install java-11-openjdk
 > dnf install java-11-openjdk-devel
 
- Postgres SQL Server 9.5 or later
 > dnf -y install postgresql-server

- ZooKeeper version 3.5 (in case of a standalone ZooKeeper installation)

### Installing PostgreSQL

To keep this simple, we are assuming the database will run on the same machine as the dCache services that
use it.

The simplest configuration is to allow password-less access to the database. Hier we assumes this to be the case.

To allow local users to access PostgreSQL without requiring a password, make sure the following three lines
are the only uncommented lines in the file **/var/lib/pgsql/10/data/pg_hba.conf**

> **NOTE**: the path to **/pg_hba.conf** is different for PostgreSQL 13 and higher versions.
> **/var/lib/pgsql/data/pg_hba.conf**

      ...
    # TYPE  DATABASE    USER        IP-ADDRESS        IP-MASK           METHOD
    local   all         all                                             trust
    host    all         all         127.0.0.1/32                        trust
    host    all         all         ::1/128                             trust    
   
   
   ### Creating PostgreSQL users and databases    
> systemctl enable --now postgresql

> createuser -U postgres --no-superuser --no-createrole --createdb --no-password dcache
> 
> createdb -U dcache chimera

And run the command `dcache database update`.



### Installing dCache

All dCache packages are available directly from our website’s dCache releases page, under the Downloads
section.

>     
>   rpm -ivh https://www.dcache.org/old/downloads/1.9/repo/8.2/dcache-8.2.9-1.noarch.rpm



### Configuring dCache users

The dCache RPM comes with a default gPlazma configuration file /etc/dcache/gplazma.conf; however,
that configuration is intended for users with X.509 credentials. X.509 credentials require a certificate au-
thority, 
which require considerable effort to set up. For this tutorial the certificates have been installed on our vm.

> vi etc/dcache/gplazma.conf
                              
 ```ini
auth    optional    x509
auth    optional    voms
auth    sufficient  htpasswd
map     optional    vorolemap
map     optional    gridmap
map     requisite   authzdb
session requisite   roles
session requisite   authzdb
                              
```

The first column is the phases of the authentication process. Each login attempt follows four phases: **auth**,
**map**, account-? and **session**. **auth** verifies user’s identity. **map** converts this identity to some dCache user.
account checks if the user is allowed to use dCache right now. Finally, **session** adds some additional infor-
mation.

This configuration tells gPlazma to use the htpasswd plugin to check any passwords, the multimap plugin to
convert usernames into uid and gid values, the banfile plugin to check if the user is allowed to use dCache,
and finally use the authzdb plugin to add various session information.
The sufficient and requisite labels describe how to handle errors. For more details on this, see the
gplazma chapter.

This ability to split login steps between different plugins may make the process seem complicated; however,
it is also very powerful and allows dCache to work with many different authentication schemes.

The sufficient and requisite labels describe how to handle errors. For more details on this, see the
gplazma chapter.
This ability to split login steps between different plugins may make the process seem complicated; however,
it is also very powerful and allows dCache to work with many different authentication schemes.


For the next step, we need to create the configuration for these four plugins. We will create two users: a
regular user (”tester”) and an admin user (”admin”).
The htpasswd plugin uses the Apache HTTPD server’s file format to record username and passwords. This
file may be maintained by the htpasswd command.
Let us create a new password file (/etc/dcache/htpasswd) and add these two users (”tester” and ”admin”)
with passwords TooManySecrets and dickerelch respectively:

> touch /etc/dcache/htpasswd
> htpasswd -bm /etc/dcache/htpasswd tester tester12
> 
> Adding password for user tester
> 
> htpasswd -bm /etc/dcache/htpasswd admin dickerelch
> 
> Adding password for user admin


Next, we need to tell dCache which uid and gids these users should be assigned. To do this, create the file
/etc/dcache/multi-mapfile with the following content:
> username:tester uid:1000 gid:1000,true
> username:admin uid:0 gid:0,true

Now we need to add the ** /etc/grid-security/certificates** folder.


> mkdir -p /etc/grid-security/certificates 


### Four main components in dCache
-------------

All components in dCache are CELLs and they are independent and can interact with each other by sending messages.
Such architecture today knows as Microservices with message queue.
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

#### Zookeeper
 - A distributed directory and coordination service that dCache relies on.


### Configuration files

In the setup of dCache, there are three main places for configuration files:

-   **/usr/share/dcache/defaults**
-   **/etc/dcache/dcache.conf**
-   **/etc/dcache/layouts**

The folder **/usr/share/dcache/defaults** contains the default settings of the dCache. If one of the default configuration values needs to be changed, copy the default setting of this value from one of the files in **/usr/share/dcache/defaults** to the file **/etc/dcache/dcache.conf**, which initially is empty and update the value.



### Configurations for Minimal set  - Single process:

- Shared JVM
- Shared CPU
- Shared Log files
- All components run the same version
- A process called DOMAIN


For this tutorial we will create a mylayout.conf where the confugurations would be stored.

First we update the file /etc/dcache/dcache.conf, appending the following line:


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
[dCacheDomain/webdav]
 webdav.authn.basic = true
 

```

**Note**

In this first installation of dCache your dCache will not be connected to a tape sytem. 
Therefore the values for `pnfsmanager.default-retention-policy` and `pnfsmanager.default-access-latency` must be changed in the file **/etc/dcache/dcache.conf**. ????


>     pnfsmanager.default-retention-policy=REPLICA
>     pnfsmanager.default-access-latency=ONLINE


> `dcache.broker.scheme = none`
>  tells the domain that it is running stand-alone, and should not attempt to contact other domains. We will cover these in the next section, where we > will have to set configuration for different domains.

>  webdav.authn.basic = true




Now we can add a new cell: Pool which is a service responsible for storing the contents of files and there must be always at least one pool.


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

No we will use the following command:

 > dcache pool create /srv/dcache/pool-1 pool1 dCacheDomain


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


So we have added a new cell pool to the dCacheDomain.




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

```console-root
systemctl list-dependencies dcache.target
|dcache.target
|● ├─dcache@coreDomain.service
|● ├─dcache@NamespaceDomain.service --???
|● ├─dcache@zookeeperDomain.service
|● ├─dcache@poolDomain.service
|● └─dcache@poolmanagerDomain.service

```

> systemctl status dcache@* 



So now you can upload a file:

> curl -u admin:admin -L -T /bin/bash http://localhost:2880/home/tester/test-file



# Grouping CELLs - core Domain and pools as satellite:


dCache will work correctly if everything runs in a single domain. dCache could be configured  to run each service in a separate
domain. 
When a dCache instance spans multiple domains, there needs to be some mechanism for sending messages between services located
in different domains.

The domains communicate with each other via TCP using connections that are
established at start-up. The topology is controlled by the location manager service. When configured, all
domains connect with a core domain, which routes all messages to the appropriate domains. This forms a
star topology.
To reduce the number of TCP connections, domains may be configured to be core domains or satellite
domains.

The simplest deployment has a single core domain and all other domains as satellite domains, mostly POOL CELLS.
In the following example we will add a new Pool domains as satellite  domains. 
 
  > dcache pool create /srv/dcache/pool-A poolA poolsDomainA

  > dcache pool create /srv/dcache/pool-B poolB poolsDomainB

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

[corelDomain/webdav]
 webdav.authn.basic = true
 
[poolsDomainA]
[poolsDomainA/pool]
pool.name=poolA
pool.path=/srv/dcache/pool-A
pool.wait-for-files=${pool.path}/data

[poolsDomainB]
[poolsDomainB/pool]
pool.name=poolB
pool.path=/srv/dcache/pool-B
pool.wait-for-files=${pool.path}/data
```

**NOTE**
> [corelDomain]
> dcache.broker.scheme = core
> indicates that coreDomain is a core domain and if the satilite poolA will need to connect to coreDomain to send a  a mesage to sattilite poolB.



Now in /var/log/dcache/ there will be created a log file for each domain

```ini
dcache status
|DOMAIN
|coreDomain
|poolBDomain
|poolADomain
STATUS PID USER
LOG
stopped
dcache /var/log/dcache/centralDomain.log
stopped
dcache /var/log/dcache/poolsDomainA.log
stopped
dcache /var/log/dcache/poolsDomainB.log

```



# Grouping CELLs - On a different hosts:
- Share-nothing option
- Components can run different, but compatible versions.
- Better throughput



1. less /etc/dcache/layouts/dcache-head-test01.conf

```ini

dcache.enable.space-reservation = false

[${host.name}_coreDomain]

dcache.broker.scheme = core


[${host.name}_coreDomain/zookeeper]
[${host.name}_coreDomain/poolmanager]
[${host.name}_coreDomain/pnfsmanager]

 pnfsmanager.default-retention-policy = REPLICA
 pnfsmanager.default-access-latency = ONLINE
 
 [${host.name}_coreDomain/webdav]
  webdav.authn.basic = true


[poolsDomainA]
[poolsDomainA/pool]
pool.name=poolA
pool.path=/srv/dcache/pool-A
pool.wait-for-files=${pool.path}/data

```

2. less /etc/dcache/layouts/dcache-head-test02.conf



```ini


[poolsDomainA]
[poolsDomainA/pool]
pool.name=poolA
pool.path=/srv/dcache/pool-A
pool.wait-for-files=${pool.path}/data

```


2. less /etc/dcache/layouts/dcache-head-test03.conf



```ini


[poolsDomainB]
[poolsDomainB/pool]
pool.name=poolB
pool.path=/srv/dcache/pool-B
pool.wait-for-files=${pool.path}/data

```
> let us upload a file

> curl -u admin:admin -v -L -T /bin/bash http://localhost:2880/home/tester/test-file


3. real instalation example

```ini
[${host.name}_gplazmaDomain]
[${host.name}_gplazmaDomain/gplazma]
gplazma.ldap.group-member=uniqueMember
gplazma.ldap.organization=ou=RGY,o=DESY,c=DE
gplazma.ldap.tree.groups=group
gplazma.ldap.url=ldap://it-ldap-slave.desy.de
gplazma.ldap.userfilter=(uid=%s)
gplazma.roles.admin-gid=5339

[${host.name}_messageDomain]
dcache.broker.scheme=core
dcache.java.memory.heap=4096m

[${host.name}_namespaceDomain]
[${host.name}_namespaceDomain/pnfsmanager]
chimera.db.url=jdbc:postgresql://${chimera.db.host}/${chimera.db.name}?prepareThreshold=3&targetServerType=master&ApplicationName=${pnfsmanager.cell.name}
pnfsmanager.db.connections.max=16


[${host.name}_coreDomain]
dcache.broker.scheme=core
dcache.java.memory.heap=4096m

[${host.name}_coreDomain/poolmanager]


[${host.name}_coreDomain/pnfsmanager]
chimera.db.url=jdbc:postgresql://${chimera.db.host}/${chimera.db.name}?prepareThreshold=3&targetServerType=master&ApplicationName=${pnfsmanager.cell.name}
pnfsmanager.db.connections.max=16


```



```ini

[core-${host.name}]



[core-${host.name}/poolmanager]

[core-${host.name}/pnfsmanager]


[core-${host.name}/nfs]
chimera.db.url=jdbc:postgresql://${chimera.db.host}/${chimera.db.name}?prepareThreshold=3&targetServerType=master&ApplicationName=${nfs.cell.name}

[admin]
[core-${host.name}/admin]
```






    
