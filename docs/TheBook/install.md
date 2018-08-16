Chapter 2. Installing dCache
============================

**Table of Contents**

+  [Installing a dCache instance](#installing-a-dcache-instance)  

     [Prerequisites](#prerequisites)  
     [Installation of the dCache Software](#installation-of-the-dCache-software)  
     [Readying the PostgreSQL server for the use with dCache](#readying-the-postgresql-server-for-the-use-with-dcache)  
     [Configuring Chimera](#configuring-chimera)  
     [Configuring dCache](#configuring-dcache)  
     [Installing dCache on several nodes](#installing-dcache-on-several-nodes)  


+  [Securiting your dCache installation](#securiting-your-dcache-installation)
+  [Upgrading a dCache Instance](#upgrading-a-dcache-instance)

The first section describes the installation of a fresh dCache instance using RPM files downloaded from [the dCache home-page]. It is followed by a guide to upgrading an existing installation. In both cases we assume standard requirements of a small to medium sized dCache instance without an attached [tertiary storage system](rf-glossary.md#tertiary-storage-system). The third section contains some pointers on extended features.

INSTALLING A dCache INSTANCE
============================

In the following the installation of a dCache instance will be described. The Chimera name space provider, some management components, and the **SRM** need a PSQL server installed. We recommend running this PSQL on the local node. The first section describes the configuration of a PSQL server. After that the installation of CHIMERA and of the dCache components will follow. During the whole installation process root access is required.

PREREQUISITES
-------------

In order to install dCache the following requirements must be met:

-   An RPM-based Linux distribution is required for the following procedure. For Debian derived systems we provide Debian packages and for Solaris the Solaris packages or the tarball.

-   dCache requires Java 8 JRE. Please use the latest patch-level and check for upgrades frequently. It is recommended to use JDK as dCache scripts can make use of some extra features that JDK provides to gather more diagnostic information (heap-dump, etc). This helps when tracking down bugs.

-   PostgreSQL must be installed and running. We recommend the use of PostgreSQL version 9.5 (at least PostgreSQL version 8.3 is required).

    > **IMPORTANT**
    >
    > For good performance it is necessary to maintain and tune your PostgreSQL server. There are several good books on this topic, one of which is [PostgreSQL 9.0 High Performance](https://www.2ndquadrant.com/de/buecher/).

INSTALLATION OF THE dCache SOFTWARE
-----------------------------------

The RPM packages may be installed right away, for example using the command:

    [root] # rpm -ivh dcache-dCache-PACKAGE-VERSION.noarch.rpm

The actual sources lie at [https://www.dcache.org/downloads/IAgree.shtml](https://www.dcache.org/downloads/IAgree.shtml). To install for example Version  2.16.0-1 you would use this:

    [root] # rpm -ivh https://www.dcache.org/downloads/1.9/repo/dCache-VERSION/dcache-dCache-PACKAGE-VERSION.noarch.rpm

The client can be found in the download-section of the above url, too.

READYING THE POSTGRESQL SERVER FOR THE USE WITH dCache
------------------------------------------------

Using a PostgreSQL server with dCache places a number of requirements on the database. You must configure PostgreSQL for use by dCache and create the necessary PostgreSQL user accounts and database structure. This section describes how to do this.

### Starting PSQL

Install the PostgreSQL server with the tools of the operating system.

Initialize the database directory (for PSQL version 9.2 this is `/var/lib/pgsql/9.2/data/`) , start the database server, and make sure that it is started at system start-up.

    [root] # service postgresql-9.2 initdb
    Initializing database:                                     [  OK  ]
    [root] # service postgresql-9.2 start
    Starting postgresql-9.2 service:                           [  OK  ]
    [root] # chkconfig postgresql-9.2 on

### Enabling local trust

Perhaps the simplest configuration is to allow password-less access to the database and the following documentation assumes this is so.

To allow local users to access PSQL without requiring a password, ensure the file `pg_hba.conf`, which (for PSQL version 9.2) is located in `/var/lib/pgsql/9.2/data`, contains the following lines.

    # TYPE  DATABASE        USER            ADDRESS                 METHOD

    # "local" is for Unix domain socket connections only
    local   all             all                                     trust
    # IPv4 local connections:
    host    all             all             127.0.0.1/32            trust
    # IPv6 local connections:
    host    all             all             ::1/128                 trust

> **NOTE**
>
> Please note it is also possible to run dCache with all PSQL accounts requiring passwords. See [the section called “Configuring Access to PostgreSQL”](cookbook-postgres.md#configuring-access-to-postgresql) for more advice on the configuration of PSQL.  


**RESTARTING POSTGRESQL**

If you have edited PSQL configuration files, you *must* restart PSQL for those changes to take effect. On many systems, this can be done with the following command:

     [root] # service postgresql-9.2 restart
     Stopping postgresql-9.2 service:        [  OK  ]
     Starting postgresql-9.2 service:        [  OK  ]


CONFIGURING CHIMERA
-------------------

Chimera is a library providing a hierarchical name space with associated meta data. Where pools in dCache store the content of files, Chimera stores the names and meta data of those files. Chimera itself stores the data in the PostgreSQL database we just set up. The properties of Chimera are defined in **/usr/share/dcache/defaults/chimera.properties**. See [Chapter 4, Chimera](config-chimera.md) for more information.



### Creating users and databases for dCache  

Create the Chimera database and user.  

    [root] # createdb -U postgres chimera  
    CREATE DATABASE  
    [root] # createuser -U postgres --no-superuser --no-createrole --createdb --pwprompt chimera  
    Enter password for new role:   
    Enter it again:  
    You do not need to enter a password.  

The dCache components will access the database server with the user srmdcache.   

    [root] # createuser -U postgres --no-superuser --no-createrole --createdb --pwprompt srmdcache  
    Enter password for new role:  
    Enter it again:  
    You do not need to enter a password.  
  
  Several management components running on the head node as well as the **SRM** will use the database dcache for storing their state information:  

      [root] # createdb -U srmdcache dcache  

There might be several of these on several hosts. Each is used by the dCache components running on the respective host.  

Create the database used for the billing plots.  

     [root] # createdb -O srmdcache -U postgres billing  

And run the command `dcache database update`.

    [root] # dcache database update  
    PnfsManager@dCacheDomain:  
    INFO  - Successfully acquired change log lock  
    INFO  - Creating database history table with name: databasechangelog  
    INFO  - Reading from databasechangelog  
    many more like this...  
      

          
Now the configuration of Chimera is done.

Before the first start of dCache replace the file **/etc/dcache/gplazma.conf** with an empty file.

    [root] # mv /etc/dcache/gplazma.conf /etc/dcache/gplazma.conf.bak
    [root] # touch /etc/dcache/gplazma.conf


dCache can be started now.

    [ROOT] # dcache start
    Starting dCacheDomain done

So far, no configuration of dCache is done, so only the predefined domain is started.

CONFIGURING dCache
------------------

### Terminology

dCache consists of one or more domains. A domain in dCache is a Java Virtual Machine hosting one or more dCache *cells*. Each domain must have a name which is unique throughout the dCache instance and a cell must have a unique name within the domain hosting the cell.

A *service* is an abstraction used in the dCache configuration to describe atomic units to add to a domain. It is typically implemented through one or more cells. dCache keeps lists of the domains and the services that are to be run within these domains in the *layout files*. The layout file may contain domain- and service- specific configuration values. A pool is a cell providing physical data storage services.

### Configuration files

In the setup of dCache, there are three main places for configuration files:

-   **/usr/share/dcache/defaults**
-   **/etc/dcache/dcache.conf**
-   **/etc/dcache/layouts**

The folder **/usr/share/dcache/defaults** contains the default settings of the dCache. If one of the default configuration values needs to be changed, copy the default setting of this value from one of the files in **/usr/share/dcache/defaults** to the file **/etc/dcache/dcache.conf**, which initially is empty and update the value.

> **NOTE**
>
>In this first installation of dCache your dCache will not be connected to a tape sytem. Therefore please change the values for pnfsmanager.default-retention-policy and pnfsmanager.default-access-latency in the file **/etc/dcache/dcache.conf**.


>
>     pnfsmanager.default-retention-policy=REPLICA
>     pnfsmanager.default-access-latency=ONLINE

Layouts describe which domains to run on a host and which services to run in each domain. For the customized configuration of your dCache you will have to create a layout file in **/etc/dcache/layouts**. In this tutorial we will call it the **mylayout.conf** file.

> **IMPORTANT**
>
> Do not update configuration values in the files in the defaults folder, since changes to these files will be overwritten by updates.

As the files in **/usr/share/dcache/defaults/** do serve as succinct documentation for all available configuration parameters and their default values it is quite useful to have a look at them.



### Defining domains and services

Domains and services are defined in the layout files. Depending on your site, you may have requirements upon the doors that you want to configure and domains within which you want to organise them.

A domain must be defined if services are to run in that domain. Services will be started in the order in which they are defined.

Every domain is a Java Virtual Machine that can be started and stopped separately. You might want to define several domains for the different services depending on the necessity of restarting the services separately.

The layout files define which domains to start and which services to put in which domain. Configuration can be done per domain and per service.

A name in square brackets, *without* a forward-slash (`/`) defines a domain. A name in square brackets *with* a forward slash defines a service that is to run in a domain. Lines starting with a hash-symbol (`#`) are comments and will be ignored by dCache.

There may be several layout files in the layout directory, but only one of them is read by dCache when starting up. By default it is the **single.conf**. If the dCache should be started with another layout file you will have to make this configuration in **/etc/dcache/dcache.conf**.

    dcache.layout=mylayout

This entry in **/etc/dcache/dcache.conf** will instruct dCache to read the layout file **/etc/dcache/layouts/mylayout.conf** when starting up.

These are the first lines of **/etc/dcache/layouts/single.conf**:

    dcache.broker.scheme=none

    [dCacheDomain]
    [dCacheDomain/admin]
    [dCacheDomain/poolmanager]

[dCacheDomain] defines a domain called dCacheDomain. In this example only one domain is defined. All the services are running in that domain. Therefore no messagebroker is needed, which is the meaning of the entry messageBroker=none.
[dCacheDomain/admin] declares that the admin service is to be run in the dCacheDomain domain.

Example:
This is an example for the **mylayout.conf** file of a single node dCache with several domains.

    [dCacheDomain]
    [dCacheDomain/topo]
    [dCacheDomain/info]

    [namespaceDomain]
    [namespaceDomain/pnfsmanager]
    [namespaceDomain/cleaner]
    [namespaceDomain/dir]

    [poolmanagerDomain]
    [poolmanagerDomain/poolmanager]

    [adminDoorDomain]
    [adminDoorDomain/admin]

    [httpdDomain]
    [httpdDomain/httpd]
    [httpdDomain/billing]

    [gPlazmaDomain]
    [gPlazmaDomain/gplazma]

> **NOTE**
>
> If you defined more than one domain, a messagebroker is needed, because the defined domains need to be able to communicate with each other. This means that if you use the file **single.conf** as a template for a dCache with more than one domain you need to delete the line messageBroker=none. Then the default value will be used which is messageBroker=cells, as defined in the defaults **/usr/share/dcache/defaults/dcache.properties**.

### Creating and configuring pools

dCache will need to write the files it keeps in pools. These pools are defined as services within dCache. Hence, they are added to the layout file of your dCache instance, like all other services.

The best way to create a pool, is to use the `dcache` script and restart the domain the pool runs in. The pool will be added to your layout file.

    [<domainname>/pool]
    name=<poolname>
    path=/path/to/pool
    pool.wait-for-files=${path}/data

The property `pool.wait-for-files` instructs the pool not to start up until the specified file or directory is available. This prevents problems should the underlying storage be unavailable (e.g., if a RAID device is offline).

> **NOTE**
>
> Please restart dCache if your pool is created in a domain that did not exist before.

    [root] # dcache pool create /srv/dcache/p1 pool1 poolDomain
    Created a pool in /srv/dcache/p1. The pool was added to poolDomain in
    file:/etc/dcache/layouts/mylayout.conf.

In this example we create a pool called pool1 in the directory **`/srv/dcache/p1`**. The created pool will be running in the domain `poolDomain`.

> **MIND THE GAP!**
>
>The default gap for poolsizes is 4GiB. This means you should make a bigger pool than 4GiB otherwise you would have to change this gap in the dCache admin tool. See the example below. See also [the section called “The Admin Interface”.](intouch.md#the-admin-interface)
>
>       (local) admin > cd <poolname>
>       (<poolname>) admin > set gap 2G
>       (<poolname>) admin > save

Adding a pool to a configuration does not modify the pool or the data in it and can thus safely be undone or repeated.

### Starting dCache

Restart dCache to start the newly configured components **dcache restart** and check the status of dCache with **dcache status**.
 
    EXAMPLE:
    
    [root] # dcache restart
    Stopping dCacheDomain 0 1 done
    Starting dCacheDomain done
    Starting namespaceDomain done
    Starting poolmanagerDomain done
    Starting adminDoorDomain done
    Starting httpdDomain done
    Starting gPlazmaDomain done
    Starting poolDomain done
    [root] # dcache status
    DOMAIN            STATUS  PID   USER
    dCacheDomain      running 17466 dcache
    namespaceDomain   running 17522 dcache
    poolmanagerDomain running 17575 dcache
    adminDoorDomain   running 17625 dcache
    httpdDomain       running 17682 dcache
    gPlazmaDomain     running 17744 dcache
    poolDomain        running 17798 dcache

Now you can have a look at your dCache via The Web Interface, see [the section called “The Web Interface for Monitoring dCache”:](intouch.md#the-web-interface-for-monitoring-dcache) http://<httpd.example.org>:2288/, where <httpd.example.org> is the node on which your httpd service is running. For a single node dCache this is the machine on which your dCache is running.  


### JAVA heap size

By default the JAVA heap size and the maximum direct buffer size are defined as

    dcache.java.memory.heap=512m
    dcache.java.memory.direct=512m

Again, these values can be changed in **/etc/dcache/dcache.conf**.

For optimization of your dCache you can define the Java heap size in the layout file separately for every domain.


      [dCacheDomain]
      dcache.java.memory.heap=2048m
      dcache.java.memory.direct=0m
      ...
      [utilityDomain]
      dcache.java.memory.heap=384m
      dcache.java.memory.direct=16m

> **NOTE**
>
> dCache uses Java to parse the configuration files and will search for Java on the system path first; if it is found there, no further action is needed. If Java is not on the system path, the environment variable `JAVA_HOME` defines the location of the Java installation directory. Alternatively, the environment variable `JAVA` can be used to point to the Java executable directly.
>
> If `JAVA_HOME` or `JAVA` cannot be defined as global environment variables in the operating system, then they can be defined in either **/etc/default/dcache** or **/etc/dcache.env**. These two files are sourced by the init script and allow `JAVA_HOME`, `JAVA` and `dCache_HOME` to be defined.

Installing dCache on several nodes
----------------------------------

Installing dCache on several nodes is not much more complicated than installing it on a single node. Think about how dCache should be organised regarding services and domains. Then adapt the layout files, as described in [in the section called “Defining domains and services”](install.md#defining-domains-and-services), to the layout that you have in mind. The files **/etc/dcache/layouts/head.conf** and **/etc/dcache/layouts/pool.conf**  contain examples for a dCache head-node and a dCache pool respectively.

> **IMPORTANT**
>
> You must configure a domain called **dCacheDomain** but the other domain names can be chosen freely.

>Please make sure that the domain names that you choose are unique. Having the same domain names in different layout files on different nodes may result in an error.

On any other nodes than the head node, the property `dcache.broker.host` has to be added to the file **/etc/dcache/dcache.conf.** This property should point to the host containing the special domain dCacheDomain, because that domain acts implicitly as a broker.

> **Tip**
>
> On dCache nodes running only pool services you do not need to install PostgreSQL. If your current node hosts only these services, the installation of PostgreSQL can be skipped.

SECURING YOUR dCache INSTALLATION
=================================

dCache uses the LocationManager to discover the network topology of the internal communication: to which domains this domain should connect. The domain contacts a specific host and queries the information using UDP port `11111`. The response describes how the domain should react: whether it should allow incoming connections and whether it should contact any other domains.

Once the topology is understood, dCache domains connect to each other to build a network topology. Messages will flow over this topology, enabling the distributed system to function correctly. By default, these connections use TCP port `11111`.

It is essential that both UDP and TCP port `11111` are firewalled and that only other nodes within the dCache cluster are allowed access to these ports. Failure to do so can result in remote users running arbitrary commands on any node within the dCache cluster.

Upgrading a dCache Instance
===========================

> **IMPORTANT**
>
> Always read the release notes carefully before upgrading!

Upgrading to bugfix releases within one supported branch (e.g. from 2.16.0 to 2.16.1) may be done by upgrading the packages with


       [root] # rpm -Uvh <packageName>

Now dCache needs to be started again.

<!---
  [the dCache home-page]: http://www.dcache.org
  [PostgreSQL 9.0 High Performance]: http://www.2ndquadrant.com/books/postgresql-9-0-high-performance
  []: http://www.dcache.org/downloads/IAgree.shtml
  [???]: #cb-postgres-configure
  [1]: #cf-chimera
  [2]: #intouch-admin
  [3]: #intouch-web
  [section\_title]: #in-install-layout
  -->
