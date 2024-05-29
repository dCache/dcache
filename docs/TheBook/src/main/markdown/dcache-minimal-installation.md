


The dCache mission is to provide a system for storing and retrieving huge amounts of data, distributed among a large number of heterogeneous
server nodes, under a single virtual filesystem tree with a variety of standard access methods.

# Minimal dCache Installation Guide

By doing this step-by-step tutorial, you have the opportunity to learn more
about how dCache works and explore some of the details of dCache
configuration and administration without being overwhelmed.  As far as
possible, you can simply copy the commands and end up with a working
dCache.  We've included some example commands that perform some basic
tests, so you can have some confidence that everything is OK so far,
before moving on to the next step.
Please note that, although this tutorial will provide you with a
working dCache instance, your production instance should be more complex.  There are many ways to configure
dCache.  The optimal choice depends on which hardware you wish to use
and how dCache's users will interact with the system.  So, there is no a single recipe that will provide the optimal solution in
all cases.
     
### Minimum System Requirements

For a minimal test installation:

- Hardware:
  - Contemporary CPU
  - At least 1 GiB of RAM
  - At least 500 MiB free disk space

- Software:
  - OpenJDK 11
  - Postgres SQL Server 9.5 or later
  - ZooKeeper version 3.5 (in case of a standalone ZooKeeper installation)

For high performance production scenarios, the hardware requirements greatly
differ, which makes it impossible to provide such parameters here. However, if
you wish to setup a dCache-based storage system, just let us know and we will
help you with your system specifications. Just contact us: <support@dcache.org>.
   
#### Software:

- OpenJDK 11 (java 11 , and java 17 for dCache staring from version 10.1)
  
 > yum install java-11-openjdk
 > 
 > dnf install java-11-openjdk-devel


- ZooKeeper version 3.7 (in case of a standalone ZooKeeper installation)

### Installing PostgreSQL



Please remember that, wherever you choose to deploy the database, it
must be tuned for optimal performance for the available hardware.
Without doing this, you will see poor performance as PostgreSQL
typically experiences very poor performance with its default
configuration.  In general, the database may be deployed on the same node as dCache or
on some dedicated machine with db-specific hardware.

To keep this simple, we are assuming that the database will run on the same machine as the dCache services that
use it.

Then, install the server package for PostgreSQL. The minimal supported PostgreSQL version
is 9.5. In general, we recommend using the latest version and upgrading your PostgreSQL version as
new versions becomes available.


First we install PostgreSQL:

- Postgres SQL Server 9.5 or later
  
 > dnf -y install postgresql-server

With the database packages installed, we can initialise the database
the service.  Note that we do not start the database at this point,
as we will make some tweaks to the configuration.

> postgresql-setup --initdb

 ...
[root@neic-demo-1 centos]#  postgresql-setup --initdb
 * Initializing database in '/var/lib/pgsql/data'
 * Initialized, logs are in /var/lib/pgsql/initdb_postgresql.log

...

The simplest configuration is to allow password-less access to the database. 

 The default setting  looks like this **/var/lib/pgsql/data/pg_hba.conf**:

> grep -v -E "^#|^$" /var/lib/pgsql/data/pg_hba.conf

 ```ini
local   all             all                                     peer
host    all             all             127.0.0.1/32            ident
host    all             all             ::1/128                 ident
local   replication     all                                     peer
host    replication     all             127.0.0.1/32            ident
host    replication     all             ::1/128                 ident
 ```

To allow local users to access PostgreSQL without requiring a password, make sure the following three lines
are the only uncommented lines in the file **/var/lib/pgsql/data/pg_hba.conf**

>vi /var/lib/pgsql/data/pg_hba.conf
>

   ```ini
    # TYPE  DATABASE    USER        IP-ADDRESS        IP-MASK           METHOD
    local   all         all                                             trust
    host    all         all         127.0.0.1/32                        trust
    host    all         all         ::1/128                             trust    
   ```


   > **NOTE**: the path to **/pg_hba.conf** is different for PostgreSQL 13 and higher versions.
   > It is **/var/lib/pgsql/data/pg_hba.conf**



Once this is done, we can configure the system to automatically start
PostgreSQL on startup, and then manually start the database:

> systemctl enable --now postgresql 

   
   
   ### Creating PostgreSQL users and databases 
   
> systemctl reload postgresql

dCache will manage the database schema, creating and updating the
database tables, indexes etc as necessary.  However, dCache does not
create the databases.  That is a manual process, typically done only
once.

 Let us creat a single database user dcache:

> createuser -U postgres --no-superuser --no-createrole --createdb --no-password dcache
>
> createdb -U dcache chimera


The second line creates  the databases, using the correct database user to
ensure correct database ownership.  At this stage, we need only one
database: chimera.  This database holds dCache's namespace.


Later after installing dcache we run the command `dcache database update`.


### Installing dCache

All dCache packages are available directly from our website’s dCache releases page, under the Downloads
section.

>     
>   rpm -ivh https://www.dcache.org/old/downloads/1.9/repo/9.2/dcache-9.2.19-1.noarch.rpm

 ```ini
Retrieving https://www.dcache.org/old/downloads/1.9/repo/9.2/dcache-9.2.19-1.noarch.rpm
warning: /var/tmp/rpm-tmp.gzWZPS: Header V4 RSA/SHA256 Signature, key ID 3321de4c: NOKEY
Verifying...                          ################################# [100%]
Preparing...                          ################################# [100%]
Updating / installing...
   1:dcache-9.2.19-1                  ################################# [100%]
 ```

### Configuring dCache users

In this tutorial, we will used gPlazma (Grid-Aware Pluggable Authorization Management) service which is a part of dCache,
providing services for access control, which are used by door-cells in order to
implement their access control system.

The dCache RPM comes with a default gPlazma configuration file **/etc/dcache/gplazma.conf**.

 gPlazma requires the CA- and VOMS-root-certificates, that it should use, to be
present in **/etc/grid-security/certificates/** and **/etc/grid-security/**
vomsdir respectively.

>
> mkdir -p /etc/grid-security/
>
> mkdir -p /etc/grid-security/certificates/
>

 X.509 credentials require a certificate authority, 
which require considerable effort to set up. For this tutorial the certificates have been installed on our VMs.


Gplazma is configured by the PAM (Privileged Access Management)-style: the first column is the phases of the authentication process. Each login attempt follows four phases: **auth**, **map**, **account** and **session**. Each phase is comprised of plugins.  Second column describes how to handle errors. Running a plugin is either success or failure, plugins that fail sometimes is expected. There are four different options: **optional** ,  **sufficient**, **required** and **requisite**. 

**optional** label means, the success or failure of this plug-in doesn't matter; always move onto next one in the phase.

**suﬀicient** Success of such a plug-in is enough to satisfy the authentication requirements of the stack of
plug-ins (if a prior required plug-in has failed the success of this one is ignored). A failure of this plug-in is
not deemed as fatal for the login attempt. If the plug-in succeeds gPlazma immediately proceeds with the
next plug-in type or returns control to the door if this was the last stack.

 **requisite**  means failling plugin finishes the phase with failure. 
 **required** failling plugin fails the phase but remaining plugins are still running.


The third column defines plugins that should be used as back-end for its tasks
and services. 

Lets have a look on a complete configuration example and go through the each phase.


>vi etc/dcache/gplazma.conf
                              
 ```ini

cat >/etc/dcache/gplazma.conf <<EOF
auth    optional    x509 #1.1
auth    optional    voms #1.2
auth    sufficient  htpasswd #1.3

map    optional    vorolemap #2.1
map     optional    gridmap #2.2
map     optional    authzdb #2.3


session requisite   roles #3.2
session requisite   authzdb #3.2
EOF                            
```





 During the login process they will be executed in the order **auth**, **map**, **account** and
**session**. The identity plugins are not used during login, but later on to map from UID+GID back to user. Within these groups they are used in the order they are specified.



  **auth**  phase - verifies user’s identity ( Has the user proved who they are?).  Auth-plug-ins are used to read the users public and private credentials and ask some authority, if those are valid for accessing the system.

**#1.1** This configuration tells gPlazma to use the **x.509** plugin used to extracts X.509 certificate chains from the credentials of a user to be used by other plug-ins.

In this tutorila we use  **x.509**  we  need to create the directory **/etc/grid-security/** and **/etc/grid-security/certificates/**




If user comes with grid
certificate and VOMS role: **voms** plugin extracts user’s DN (**#1.2**).

The **htpasswd** plugin uses the Apache HTTPD server’s file format to record username and passwords. This
file may be maintained by the htpasswd command.
Let us create a new password file (/etc/dcache/htpasswd) and add the user admin
with passwords admin:

> touch /etc/dcache/htpasswd
> 
> yum install httpd-tools
> 
> htpasswd -bm /etc/dcache/htpasswd admin admin
>






 **#2** **map** - converts this identity to some dCache user (Who is this user in dCache terms: uid, gids).


 In the next step, any extracted information needs to be mapped to a UID : GID pair to be used inside dCache. Until now, this was typically done with the vorolemap plugin.
 
```ini
 map    optional    vorolemap
 ```

The actual mapping was made inside a dedicated file (by default /etc/grid-security/grid-vorolemap). See step **2.2**.
                                              
 **#2.1** the “grid-mapfile”-file, the client-certificate’s DN is mapped to a
virtual user-name.                      

CA user mapping:

```ini
cat >/etc/grid-security/grid-mapfile <<EOF
"/C=DE/ST=Hamburg/O=dCache.ORG/CN=Kermit the frog" kermit
EOF 
 ```
 
**#2.2** the vorolemap plug-in maps the users DN+FQAN to a username which is then
mapped to UID/GIDs by the authzdb plug-in.

Voms user mapping

```ini

cat >/etc/grid-security/grid-vorolemap <<EOF
"*" "/desy" desyuser
EOF
 ``` 
 
 
 **#2.3** Using the “storage-authzdb-style”-file, this virtual user-name is then mapped to
the actual UNIX user-ID 4 and group-IDs 4 and define the rights on read/write.

The first and the second "/" are home and root directories respectivly.



```ini
cat >/etc/grid-security/storage-authzdb <<EOF
version 2.1

authorize admin              read-write    0    0 / / /
authorize desyuser           read-write 1000 2000 / / /
authorize kermit             read-write 1000 1000 / / / 
EOF
```




 
 **account** - checks whether the user allowed to use this service.


Finally, **session** adds some additional information, for example the user’s home directory.


This ability to split login steps between different plugins may make the process seem complicated; however,
it is also very powerful and allows dCache to work with many different authentication schemes.




### Another way to authorise (TOKENS WLCG)

dCache supports OIDC tokens.

You will need to install oid-agent


On the VM for this tutorial it was needed to instal epel

> dnf config-manager --set-enabled crb
> 
> dnf install epel-release epel-next-release
> 
> dnf repolist epel
> 

Now we can insatll oid-agent and run:

>  dnf install oidc-agent
>
> oidc-agent



```ini

[root@neic-demo-1 centos]# OIDC_SOCK=/tmp/oidc-6XTqy6/oidc-agent.3910; export OIDC_SOCK;
OIDCD_PID=17651; export OIDCD_PID;
echo Agent pid $OIDCD_PID

```

>
> export OIDC_SOCK=/tmp/oidc-BRujtJ/oidc-agent.4673
>
> export OIDCD_PID=18499
> echo $OIDCD_PID
>
>
And now we can generate token with scope

>
> oidc-gen wlcg-with-scope
>

```ini
[1] https://bildungsproxy.aai.dfn.de
[2] https://cilogon.org
[3] https://iam.deep-hybrid-datacloud.eu/
[4] https://aai.egi.eu/auth/realms/egi
[5] https://aai-demo.egi.eu/auth/realms/egi
[6] https://aai-dev.egi.eu/auth/realms/egi
[7] https://login.elixir-czech.org/oidc/
[8] https://b2access.eudat.eu:8443/oauth2
[9] https://iam.extreme-datacloud.eu/
[10] https://fels.scc.kit.edu/oidc/realms/fels
[11] https://accounts.google.com
[12] https://login.helmholtz.de/oauth2
[13] https://login-dev.helmholtz.de/oauth2
[14] https://iam-demo.cloud.cnaf.infn.it/
[15] https://iam-test.indigo-datacloud.eu/
[16] https://oidc.scc.kit.edu/auth/realms/kit
[17] https://auth.didmos.nfdi-aai.de
[18] https://regapp.nfdi-aai.de/oidc/realms/nfdi_demo
[19] https://wlcg.cloud.cnaf.infn.it/
[20] https://alice-auth.web.cern.ch/
[21] https://atlas-auth.web.cern.ch/
[22] https://cms-auth.web.cern.ch/
[23] https://lhcb-auth.web.cern.ch/
Issuer [https://bildungsproxy.aai.dfn.de]:


```

we give the number of the issues in our case it is WLG ([19] https://wlcg.cloud.cnaf.infn.it/
)


```ini
Issuer [https://bildungsproxy.aai.dfn.de]:19


The following scopes are supported: openid profile email offline_access wlcg wlcg.groups storage.read:/ storage.create:/ compute.read compute.modify compute.create compute.cancel storage.modify:/ eduperson_scoped_affiliation eduperson_entitlement eduperson_assurance storage.stage:/ entitlements

```
in the next step we enter wich scopes we want to have: 

> Scopes or 'max' (space separated) [openid profile offline_access]: max



```ini
Scopes or 'max' (space separated) [openid profile offline_access]: max
Generating account configuration ...
accepted

Using a browser on any device, visit:
https://wlcg.cloud.cnaf.infn.it/device

And enter the code: 9Z4ERM
Alternatively you can use the following QR code to visit the above listed URL.

```

After entering the code the divece is authorised:

```ini

Enter encryption password for account configuration 'wlcg-with-scope': 
Confirm encryption Password: 
Everything setup correctly!


```

No we can get the token

>  oidc-token wlcg-with-scope


```ini

eyJraWQiOiJyc2ExIiwiYWxnIjoiUlMyNTYifQ.eyJ3bGNnLnZlciI6IjEuMCIsInN1YiI6ImU0ZmYxOTFlLTc4NDUtNDEzYy1iNDM3LTkzNzhmOTIzZmE4ZCIsImF1ZCI6Imh0dHBzOlwvXC93bGNnLmNlcm4uY2hcL2p3dFwvdjFcL2FueSIsIm5iZiI6MTY4OTE0OTI2NSwic2NvcGUiOiJzdG9yYWdlLmNyZWF0ZTpcLyBvcGVuaWQgb2ZmbGluZV9hY2Nlc3MgcHJvZmlsZSBzdG9yYWdlLnJlYWQ6XC8gc3RvcmFnZS5zdGFnZTpcLyBzdG9yYWdlLm1vZGlmeTpcLyB3bGNnIHdsY2cuZ3JvdXBzIiwiaXNzIjoiaHR0cHM6XC9cL3dsY2cuY2xvdWQuY25hZi5pbmZuLml0XC8iLCJleHAiOjE2ODkxNTA0NjQsImlhdCI6MTY4OTE0OTI2NSwianRpIjoiNjRkMWQ0MTktYjkyMi00OTFmLWE5MzctODgyMTEyMDdjMGY3IiwiY2xpZW50X2lkIjoiZTlmOGRiMzktNGZkZS00NjdiLWI1ZjgtYjI1ZDllNDg5ZmZjIiwid2xjZy5ncm91cHMiOlsiXC93bGNnIiwiXC93bGNnXC94ZmVycyJdfQ.K392AfD0kGI72zZHXRYNOK7VEQF1742epUKSQaD14B7wn62fNRNtQekO9hMhpGTQ2nIYnHjeOCYAcg4J9H5Tkk7yUqXc6uya4qMRZ0t2qwfO5Ky_qsoOK0vOZJ9D8ZtDYCowmmdWbHQlqbUCHwi8KNyUk1gJo9RSNah-sL799Fc

```

>
> TOKEN=$(oidc-token wlcg-with-scope)


And hier is our token

>echo $TOKEN | cut -d "." -f 2 | base64 -d 2>|/dev/null | jq
>

```ini

{
  "wlcg.ver": "1.0",
  "sub": "e4ff191e-7845-413c-b437-9378f923fa8d",
  "aud": "https://wlcg.cern.ch/jwt/v1/any",
  "nbf": 1716831360,
  "scope": "storage.create:/ openid offline_access profile storage.read:/ storage.stage:/ storage.modify:/ wlcg wlcg.groups",
  "iss": "https://wlcg.cloud.cnaf.infn.it/",
  "exp": 1716832560,
  "iat": 1716831360,
  "jti": "7eeb100e-ffcf-47dd-afa5-b615c302473e",
  "client_id": "bf263e34-6e38-4b0a-91fd-b303b01ae5cc",
  "wlcg.groups": [
    "/wlcg",
    "/wlcg/xfers"
  ]
}
```

Now we need to adapt our gplazma and layout configurations.
We add to /etc/dcache/gplazma.conf 

```ini
auth   optional   x509
auth   optional   voms
auth   optional   oidc
```
where the oidc plugin extracts the storage claims, groups and unique identifier from the IAM-token. In the next step, any extracted information needs to be mapped to a UID : GID pair to be used inside dCache. Until now, this was typically done with the vorolemap plugin. This is used during the map step inside the /etc/dcache/gplazma.conf file:

```ini
map   optional   vorolemap

```

The actual mapping was made inside a dedicated file (by default /etc/grid-security/grid-vorolemap). In a similar fashion, the new token based credential have to be mapped as well. 


```ini
map   sufficient   multimap gplazma.multimap.file=/etc/dcache/multi-mapfile.wlcg_jwt
# Or after sufficient 
#map     optional        multimap gplazma.multimap.file=/etc/dcache/multi-mapfile.wlcg_jwt

```

in **mylayout.conf** we add :


```ini
gplazma.oidc.audience-targets=https://wlcg.cern.ch/jwt/v1/any
gplazma.oidc.provider!wlcg=https://wlcg.cloud.cnaf.infn.it/ -profile=wlcg -prefix=/ -suppress=audience


```


 **gplazma.conf** looks like this now.

                              
 ```ini

cat >/etc/dcache/gplazma.conf <<EOF
auth    optional    x509 #1.1
auth    sufficient  htpasswd #1.3
auth    optional     oidc

map     optional   multimap gplazma.multimap.file=/etc/dcache/multi-mapfile.wlcg_jwt

map     optional    vorolemap #2.1
map     optional    gridmap #2.2iodc
map     requisite   authzdb #2.3

session requisite   roles #3.2
session requisite   authzdb #3.2

EOF                            
```

> vi **/etc/dcache/multi-mapfile.wlcg_jwt**

                             
 ```ini

op:wlcg               uid:1999 gid:1999,true username:wlcg_oidc

```

We need to change **/etc/grid-security/storage-authzdb**  

 ```ini
authorize admin         read-write    0    0 / / /
authorize wlcg_oidc     read-write 1999 1999 / / /
```

>chown dcache:dcache /etc/grid-security/host*.pem



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
 - Data storage nodes, talks all protocols

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

[dCacheDomain/gplazma]
gplazma.oidc.audience-targets=https://wlcg.cern.ch/jwt/v1/any
gplazma.oidc.provider!wlcg=https://wlcg.cloud.cnaf.infn.it/ -profile=wlcg -prefix=/ -suppress=audience


[dCacheDomain/poolmanager]
[dCacheDomain/webdav]
#webdav.authn.basic = true
webdav.authn.protocol=https
webdav.authz.readonly=false
webdav.cell.name=WebDAV-${host.name}

 

```

**Note**

In this first installation of dCache your dCache will not be connected to a tape sytem. 
Therefore the values for `pnfsmanager.default-retention-policy` and `pnfsmanager.default-access-latency` must be changed in the file **/etc/dcache/dcache.conf**.


>     pnfsmanager.default-retention-policy=REPLICA
>     pnfsmanager.default-access-latency=ONLINE


> `dcache.broker.scheme = none`
>

tells the domain that it is running stand-alone, and should not attempt to contact other domains. We will cover these in the next example, where  configurations for different domains  will be explained.

 
Now we can add a new cell: Pool which is a service responsible for storing the contents of files and there must be always at least one pool.


The `dcache` script provides an easy way to create the pool directory
structure and add the pool service to a domain.  In the following
example, we will create a pool using storage located at
`/srv/dcache/pool-1` and add this pool to the `dCacheDomain` domain.



No we will use the following command:

 > dcache pool create /srv/dcache/pool-1 pool1 dCacheDomain
 >

```ini

 Created a pool in /srv/dcache/pool-1. The pool was added to dCacheDomain
in file:/etc/dcache/layouts/mylayout.conf.
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
 
[dCacheDomain/gplazma]

[dCacheDomain/pool]
pool.name=pool1
pool.path=/srv/dcache/pool-1
pool.wait-for-files=${pool.path}/data
```


So we have added a new cell pool to the dCacheDomain.

It is important to undretsand that in this case we have one domain.




## Starting dCache

There are two ways to start dCache: 1) using sysV-like daemon, 2) Using systemd service.

##### Using sysV -like daemon

 


The domain log file (/var/log/dcache/dCacheDomain.log) also contains some details, logged as dCache
starts

Using systemd service dCache uses systemd’s generator functionality to create a service for each defined
domain in the layout file. That’s why, before starting the service all dynamic systemd units should be
generated:

  > systemctl daemon-reload
  > systemctl start dcache.target


To inspect all generated units of dcache.target the systemd list-dependencies command can be used. In
our simple installation with just one domain hosting several services this would look like

> systemctl list-dependencies dcache.target

```console-root
systemctl list-dependencies dcache.target
|dcache.target
|● ├─dcache@dCacheDomain.service


```

To check the status we use the following command:

> systemctl status dcache@* 

To stop and restart dcache.target command are:

> systemctl restart dcache.target
> 
> systemctl stop dcache.target
> 
> journalctl -f -u dcache@dCacheDomain




```ini
[root@neic-demo-3 centos]# chimera
Type 'help' for help on commands.
Type 'exit' or Ctrl+D to exit.
chimera:/# mkdir test
chimera:/# chown 1999:1999 test
chimera:/# ls test
```
So now you can upload a file:

> curl  -v  -k -L -u   admin:admin   --upload-file /etc/grid-security/hostcert.pem https://neic-demo-2.desy.de:2880/test/file.test.2

And using our tokens


> dnf install davix
> 
> davix-ls -k -H "Authorization: Bearer ${TOKEN}" https://neic-demo-2.desy.de:2880/

To write a file we do 

```

 ```ini

[root@neic-demo-2 centos]# davix-put -k -H "Authorization: Bearer ${TOKEN}" /etc/grid-security/hostcert.pem https://neic-demo-2.desy.de:2880/test/test.file.1
[root@neic-demo-2 centos]# davix-ls -k -H "Authorization: Bearer ${TOKEN}" https://neic-demo-2.desy.de:2880/
lost%2Bfound
test
[root@neic-demo-2 centos]# davix-ls -k -H "Authorization: Bearer ${TOKEN}" https://neic-demo-2.desy.de:2880/test
test.file.1
```


We can have a look on a log to see what are the messages we are getting

>journalctl -f -u dcache@dCacheDomain.service

```console-root

Jan 05 13:44:15 os-46-install1.novalocal dcache@dCacheDomain[25977]: 05 Jan 2023 13:44:15 (pool1) [] Pool mode changed to enabled
```



we can see the file on the pool


```console-root
[centos@os-46-install1 ~]$ ls /srv/dcache/pool-1/data
0000441B2048C3434F6282C1E1E4EAC9D8CA

```






# Grouping CELLs - core Domain and pools as satellite:


dCache will work correctly if everything runs in a single domain. dCache could be configured  to run each service in a separate
domain. 
When a dCache instance spans multiple domains, there needs to be some mechanism for sending messages between services located
in different domains.

The domains communicate with each other via TCP using connections that are
established at start-up. The topology is controlled by the location manager service. When configured, all
domains connect with a core domain, which routes all messages to the appropriate domains. This forms a
star topology.
To reduce the number of TCP connections, domains may be configured to be **core**, **none** domains or **satellite**
domains.

The simplest deployment has a single core domain and all other domains as satellite domains, mostly POOL cells.
In the following example we will add a new Pool domains as satellite  domains. 

In this case we have :

 - Independent JVMs
 - Shared CPU
 - Per-process Log file
 - All components run the same version (you can run different versions if needed)
 

we will add two new pool domains:

 > dcache pool create /srv/dcache/pool-1 pool1 poolsDomain1
 > 
 > dcache pool create /srv/dcache/pool-2 pool2 poolsDomain2
 
We need to change the **dcache.broker.scheme = core**

Please note, that we are still on the same node


```ini

dcache.enable.space-reservation = false

[coreDomain]
dcache.broker.scheme = core
[coreDomain/zookeeper]
[coreDomain/pnfsmanager]
 pnfsmanager.default-retention-policy = REPLICA
 pnfsmanager.default-access-latency = ONLINE

[coreDomain/poolmanager]

[coreDomain/webdav]
 webdav.authn.basic = true
 
[poolsDomain1]
[poolsDomain1/pool1]
pool.name=pool1
pool.path=/srv/dcache/pool-1
pool.wait-for-files=${pool.path}/data

[poolsDomain2]
[poolsDomain2/pool2]
pool.name=pool2
pool.path=/srv/dcache/pool-2
pool.wait-for-files=${pool.path}/data
```

**NOTE**
> [corelDomain]
> dcache.broker.scheme = core
> indicates that **coreDomain** is a core domain and if the **satellite pool2** will need to connect to coreDomain to send a  a message to satellite pool2.


 

```console-root
systemctl list-dependencies dcache.target
|dcache.target
|● ├─dcache@coreDomain.service
|● ├─dcache@poolsDomain1.service 
|● ├─dcache@poolsDomain2.service


```

We reload and restart dcache everytime wenn we add new configurations:

systemctl restart dcache.target

 > systemctl daemon-reload
 > 
 > systemctl restart dcache.target

Now in **/var/log/dcache/** there will be created a log file for each domain

```ini
dcache status
|DOMAIN
|coreDomain
|pool1Domain
|pool2Domain
STATUS PID USER
LOG
stopped
dcache /var/log/dcache/coreDomain.log
stopped
dcache /var/log/dcache/poolsDomain1.log
stopped
dcache /var/log/dcache/poolsDomain2.log

```

To check wethre pools have been enabeled we will use


>  journalctl -u dcache@poolsDomain1.service
>  
>  journalctl -u dcache@poolsDomain2.service



Cells specifique properties could be found in **/usr/share/dcache/defaults/**





In general when grouping cells the rules to be followed are are following:


- each domain is a single java process
- each domain MUST have a dCache instance unique name
- all domains connected to the same zookeeper are part of a single
  dCache instance
 - pool names (cellnames) MUST be unique within dCache insance  
  
- in muti-domain setup at lease one domain MUST be `core` domain.

- cells have full qualified name like `cellname@domain`


# Grouping CELLs - On a different hosts:

- Share-nothing option
- Components can run different, but compatible versions.
- Better throughput
- Zookeeper should be used 




1. less /etc/dcache/layouts/dcache-head-test01.conf

```ini

dcache.enable.space-reservation = false

[${host.name}_coreDomain]

dcache.broker.scheme = core


[${host.name}_coreDomain/zookeeper]
[${host.name}_coreDomain/pnfsmanager]
 pnfsmanager.default-retention-policy = REPLICA
 pnfsmanager.default-access-latency = ONLINE

[${host.name}_coreDomain/poolmanager]
[${host.name}_coreDomain/webdav]
 webdav.authn.basic = true

[${host.name}_coreDomain/gplazma]

[${host.name}_coreDomain/pool]
pool.name=pool1
pool.path=/srv/dcache/pool-1
pool.wait-for-files=${pool.path}/data

[${host.name}_coreDomain/pool]
pool.name=pool2
pool.path=/srv/dcache/pool-2
pool.wait-for-files=${pool.path}/data


```

2. less /etc/dcache/layouts/dcache-head-test02.conf

```ini

dcache.zookeeper.connection = os-46-install1.desy.de


[poolsDomainA]
[poolsDomainA/pool]
pool.name=poolA
pool.path=/srv/dcache/pool-A
pool.wait-for-files=${pool.path}/data
```

By default dcache.broker.scheme proprty is satellite so you do not need to set it.

Let us check the log, to see if it is enabled.
> journalctl -u dcache@poolsDomainA.service



Now we can do a file migration from one pool to another. To do so we will need to add a new service to our core domain, which will be the **admin** service.


```ini

dcache.enable.space-reservation = false

[${host.name}_coreDomain]

dcache.broker.scheme = core


[${host.name}_coreDomain/zookeeper]
[${host.name}_coreDomain/pnfsmanager]
 pnfsmanager.default-retention-policy = REPLICA
 pnfsmanager.default-access-latency = ONLINE

[${host.name}_coreDomain/poolmanager]
[${host.name}_coreDomain/webdav]
 webdav.authn.basic = true

[${host.name}_coreDomain/gplazma]
[${host.name}_coreDomain/admin]

...


Using **admin** you can get information about all services and perform different operations on them. In this example we will use migration move command to move a file from one pol to other.   

```console-root

[os-46-install1] (pool1@dCacheDomain) admin > \l
acm
gPlazma
PnfsManager
pool1
pool2
PoolManager
WebDAV-os-46-install1
zookeeper
...

After adding a new pool domain on dcache-head-test2 we will see the newly added **PoolA** cell in admin interface.

```console-root
[os-46-install1] (pool1@dCacheDomain) admin > \l
acm
gPlazma
PnfsManager
pool1
pool2
poolA
PoolManager
WebDAV-os-46-install1
zookeeper
```




Now we will migrate the file using the following command:

> migration move -target=pool -- poolA 


```console-root
[os-46-install1] (local) admin > \c pool1
[os-46-install1] (pool1@dCacheDomain) admin > rep ls
000078AB06BD15FE4F30A4BFA0CBF2350008 <C-------X--L(0)[0]> 1388880 si={<Unknown>:<Unknown>}
0000441B2048C3434F6282C1E1E4EAC9D8CA <C-------X--L(0)[0]> 1388880 si={<Unknown>:<Unknown>}
0000D769014D0D934AF588E2B8C0463EDA25 <C-------X--L(0)[5]> 1388880 si={<Unknown>:<Unknown>}

[os-46-install1] (pool1@dCacheDomain) admin > migration move -target=pool -- poolA
[os-46-install1] (pool1@dCacheDomain) admin > migration move -target=pool -- poolA



```

We cann monitor the migration process 

```console-root
[os-46-install1] (pool1@dCacheDomain) admin > migration info 2
Command    : migration move -target=pool -- poolA
State      : RUNNING
Queued     : 2
Attempts   : 3
Targets    : poolA
Completed  : 0 files; 0 bytes; 0%
Total      : 4166640 bytes
Concurrency: 1
Running tasks:
[67] 0000D769014D0D934AF588E2B8C0463EDA25: TASK.Copying -> [poolA@local]
Most recent errors:
16:10:16 [63] 000078AB06BD15FE4F30A4BFA0CBF2350008: PnfsManager failed (No such file or directory with PNFSID: 000078AB06BD15FE4F30A4BFA0CBF2350008)
16:10:26 [65] 0000441B2048C3434F6282C1E1E4EAC9D8CA: PnfsManager failed (No such file or directory with PNFSID: 0000441B2048C3434F6282C1E1E4EAC9D8CA)

[os-46-install1] (pool1@dCacheDomain) admin > \l poolA
poolA

[os-46-install1] (pool1@dCacheDomain) admin > \c poolA
[os-46-install1] (poolA@poolsDomainA) admin > rep ls
0000D769014D0D934AF588E2B8C0463EDA25 <-----------L(0)[0]> 0 si={<Unknown>:<Unknown>}


```

3. real installation example

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









    
