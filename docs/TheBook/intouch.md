Chapter 3. Getting in Touch with dCache
=======================================

Table of Contents
------------------

* [Checking the Functionality](#checking-the-functionality)  

     [dCache without mounted namespace](#dcache-without-mounted-namespace)  
     [WebDAV](#webdav)  
     [dCap](#dcap)  
     
     
* [The Web Interface for Monitoring dCache](#the-web-interface-for-monitoring-dcache)  
* [The Admin Interface](#the-admin-interface)  

     [First steps](#first-steps)  
     [Access with ssh2](#access-with-ssh2)  
     [Access with ssh1](#access-with-ssh1)  
     [How to use the Admin Interface](#how-to-use-the-admin-interface)  
     [Create a new user](#create-a-new-user)  
     [Use of the ssh Admin Interface by scripts](#use-of-the-ssh-admin-interface-by-scripts)  

* [Authentication and Authorization in dCache](#authentication-and-authorization-in-dcache)  
* [How to work with secured dCache](#how-to-work-with-secured-dCache)

     [GSIdCap](#gsidcap)
     [SRM](#srm)
     [WebDAV with certificates](#webdav-with-certificates)
   
 * [Files](#files)



This section is a guide for exploring a newly installed dCache system. The confidence obtained by this exploration will prove very helpful when encountering problems in the running system. This forms the basis for the more detailed stuff in the later parts of this book. The starting point is a fresh installation according to [the section called “Installing a dCache instance”.](install.md#installing-a-dcache-instance)



CHECKING THE FUNCTIONALITY
==========================

Reading and writing data to and from a dCache instance can be done with a number of protocols. After a standard installation, these protocols are **dCap**, **GSIdCap**, and **GridFTP**. In addition dCache comes with an implementation of the **SRM** protocol which negotiates the actual data transfer protocol.  

dCache WITHOUT MOUNTED NAMESPACE
--------------------------------

Create the root of the Chimera namespace and a world-writable directory by

    [root] # /usr/bin/chimera mkdir /data
    [root] # /usr/bin/chimera mkdir /data/world-writable
    [root] # /usr/bin/chimera chmod 777 /data/world-writable

WEBDAV
------

To use **WebDAV** you need to define a **WebDAV** service in your layout file. You can define this service in an extra domain, e.g. [webdavDomain] or add it to another domain.

    [webdavDomain]
    [webdavDomain/webdav]
    webdav.authz.anonymous-operations=FULL

to the file **/etc/dcache/layouts/mylayout.conf**.

> **NOTE**
>
> Depending on the client you might need to set `webdav.redirect.on-read=false` and/or ` webdav.redirect.on-write=false`.
>
>     #  ---- Whether to redirect GET requests to a pool
>     #
>     #   If true, WebDAV doors will respond with a 302 redirect pointing to
>     #   a pool holding the file. This requires that a pool can accept
>     #   incoming TCP connections and that the client follows the
>     #   redirect. If false, data is relayed through the door. The door
>     #   will establish a TCP connection to the pool.
>     #
>     (one-of?true|false)webdav.redirect.on-read=true
>
>     #  ---- Whether to redirect PUT requests to a pool
>     #
>     #   If true, WebDAV doors will respond with a 307 redirect pointing to
>     #   a pool to which to upload the file. This requires that a pool can
>     #   accept incoming TCP connections and that the client follows the
>     #   redirect. If false, data is relayed through the door. The door
>     #   will establish a TCP connection to the pool. Only clients that send
>     #   a Expect: 100-Continue header will be redirected - other requests
>     #   will always be proxied through the door.
>     #
>     (one-of?true|false)webdav.redirect.on-write=true

Now you can start the WEBDAV domain

    [root] # dcache start webdavDomain

and access your files via http://<webdav-door.example.org>:2880 with your browser.

You can connect the webdav server to your file manager and copy a file into your dCache.

To use curl to copy a file into your dCache you will need to set webdav.redirect.on-write=false.  


Write the file `test.txt`

    [root] # curl -T test.txt http://webdav-door.example.org:2880/data/world-writable/curl-testfile.txt

and read it

    [root] # curl http://webdav-door.example.org:2880/data/world-writable/curl-testfile.txt

DCAP
----

To be able to use dCap you need to have the dCap door running in a domain.

    [dCacheDomain]
    [dCacheDomain/dcap]

For anonymous access you need to set the property `dcap.authz.anonymous-operations` to `FULL`.

        [dCacheDomain]
        [dCacheDomain/dcap]
          dcap.authz.anonymous-operations=FULL

For this tutorial install dCap on your worker node. This can be the machine where your dCache is running.

Get the GLITE repository (which contains dCap) and install DCAP using `yum`.

       [root] # cd /etc/yum.repos.d/
       [root] # wget http://grid-deployment.web.cern.ch/grid-deployment/glite/repos/3.2/glite-UI.repo
       [root] # yum install dcap

Create the root of the Chimera namespace and a world-writable directory for **dCap** to write into as described [above](#dcache-without-mounted-namespace).

Copy the data (here **/bin/sh** is used as example data) using the **dccp** command and the **dCap** protocol describing the location of the file using a URL, where <dcache.example.org> is the host on which the dCache is running

       [root] # dccp -H /bin/sh dcap://<dcache.example.org>/data/world-writable/my-test-file-1
       [##########################################################################################] 100% 718 kiB
       735004 bytes (718 kiB) in 0 seconds

and copy the file back.

       [root] # dccp -H dcap://<dcache.example.org>/data/world-writable/my-test-file-1 /tmp/mytestfile1
       [##########################################################################################] 100% 718 kiB
       735004 bytes (718 kiB) in 0 seconds

To remove the file you will need to mount the namespace.


THE WEB INTERFACE FOR MONITORING dCache
=======================================

In the standard configuration the dCache web interface is started on the head node (meaning that the domain hosting the httpd service is running on the head node) and can be reached via port 2288. Point a web browser to http://<head-node.example.org>:2288/ to get to the main menu of the dCache web interface. The contents of the web interface are self-explanatory and are the primary source for most monitoring and trouble-shooting tasks.

The “Cell Services” page displays the status of some important [cells](rf-glossary.md#cell) of the dCache instance.

The “Pool Usage” page gives a good overview of the current space usage of the whole dCache instance. In the graphs, free space is marked yellow, space occupied by [cached files](rf-glossary.md#cached-replica) (which may be deleted when space is needed) is marked green, and space occupied by [precious replica](rf-glossary.md#precious-replica), which cannot be deleted is marked red. Other states (e.g., files which are currently written) are marked purple.

The page “Pool Request Queues” (or “Pool Transfer Queues”) gives information about the number of current requests handled by each pool. “Actions Log” keeps track of all the transfers performed by the pools up to now.

The remaining pages are only relevant with more advanced configurations: The page “Pools” (or “Pool Attraction Configuration”) can be used to analyze the current configuration of the [pool selection unit](rf-glossary.md#pool-selection-unit) in the pool manager. The remaining pages are relevant only if a [tertiary storage system (HSM)](rf-glossary.md#hsm-type) is connected to the dCache instance.

THE ADMIN INTERFACE
===================

> **JUST USE COMMANDS THAT ARE DOCUMENTED HERE**
>
> Only commands described in this documentation should be used for the administration of a dCache system.

FIRST STEPS
-----------

dCache has a powerful administration interface. It can be accessed with the **ssh1** or with the **ssh2** protocol. The server is part of the **adminDoor** domain.

It is useful to define the admin service in a seperate domain. This allowes to restart the admin service seperatly from other services. In the example in [the section called “Installing a dCache instance”](install.md) this domain was called adminDoorDomain.

> EXAMPLE:
    [adminDoorDomain]  
    [adminDoorDomain/admin]  

> **NOTE**
>
> The admin interface is using **ssh2**. It used to be available using `ssh1`, which is insecure and therefore discouraged. If you want to run the admin service with ssh1 you need to define the ssh1 service.
>
>     [adminDoorDomain]
>     [adminDoorDomain/ssh1]
>

ACCESS WITH SSH2
----------------

There are two ways of authorizing administrators to access the dCache `ssh2` admin interface. The preferred method authorizes users through their public key. The second method employs `gPlazma2` and the **dcache.kpwd** file. Thereby authorization mechanisms can be added later by deploying another `gPlazma2` plugin. The configuration of both authorization mechanisms is described in the following.



> **Note**
>
> All configurable values of the ssh2 admin interface can be found in the **/usr/share/dcache/defaults/admin.properties** file. Please do NOT change any value in this file. Instead enter the key value combination in the **/etc/dcache/dcache.conf**.



### Public Key Authorization

To authorize administrators through their public key just insert it into the file **authorized_keys2** which should by default be in the directory **/etc/dcache/admin** as specified in the file **/usr/share/dcache/defaults/admin.properties** under admin.paths.authorized-keys=. Keys have to be in one line and should have a standard format, such as:

    ssh-dss AAAAB3....GWvM= /Users/JohnDoe/.ssh/id_dsa

> **IMPORTANT**
>
> Please make sure that the copied key is still in one line. Any line-break will prevent the key from being read.

> **NOTE**
>
> You may omit the part behind the equal sign as it is just a comment and not used by dCache.

Key-based authorization will always be the default. In case the user key can not be found in the file **authorized_keys2** or the file does not exist, ssh2Admin will fall back to authorizing the user via `gPlazma2` and the **dcache.kpwd** file.



Now you can login to the admin interface by

    [user] $ ssh -l admin -p 22224 headnode.example.org

         dCache Admin (VII) (user=admin)


    (local) admin >

### Access via **gPlazma2** and the **dcache.kpwd** File

To use `gPlazma` make sure that you defined a `gPlazmaDomain` in your layout file.

>   Example:   
>   Part of the layout file in **/etc/dcache/layouts**:   
>   
>    <gplazma-${host.name}>Domain    
>    <gplazma-${host.name}>Domain/gplazma    

To use gPlazma2 you need to specify it in the **/etc/dcache/dcache.conf** file:    

>     This is the main configuration file of dCache.  
>      
>    ...  
>      
>     use gPlazma2  
>    gplazma.version=2   

Moreover, you need to create the file **/etc/dcache/gplazma.conf** with the content  

>    auth optional kpwd "kpwd=etc/dcache/dcache.kpwd"   
>    map optional kpwd "kpwd=etc/dcache/dcache.kpwd"   
>    session optional kpwd "kpwd=etc/dcache/dcache.kpwd"   

and add the user `admin` to the **`/etc/dcache/dcache.kpwd`** file using the `dcache` script.  

>    Example:    
>    [user] $ dcache kpwd dcuseradd admin -u 12345 -g 1000 -h / -r / -f / -w read-write -p password   
>    writing to /etc/dcache/dcache.kpwd :  

>
>    done writing to /etc/dcache/dcache.kpwd :     
>       
>    [user] $     

adds this to the **/etc/dcache/dcache.kpwd** file:  

>    # set pwd    
>    passwd admin 4091aba7 read-write 12345 1000 / /  

Edit the file **/etc/dcache/dcachesrm-gplazma.policy** to switch on the `kpwd-plugin`. For more information about `gPlazma` see [Chapter 10, Authorization in dCache](config-plazma.md).

Now the user `admin` can login to the admin interface with his password `password` by:

    [user] $ ssh -l admin -p 22224 headnode.example.org  
    admin@headnode.example.org's password:  

        dCache Admin (VII) (user=admin)  


    (local) admin >   

To allow other users access to the admin interface add them to the `/etc/dcache/dcache.kpwd` file as described above.

Just adding a user in the **dcache.kpwd** file is not sufficient. The generated user also needs access rights that can only be set within the admin interface itself.

See [the section called “Create a new user”](#create-a-new-user) to learn how to create the user in the admin interface and set the rights.

Access with SSH1
----------------

Connect to the server using `ssh1` with:

    [user] $ ssh -c blowfish -p 22223 -l admin headnode.example.org


The initial password is “`dickerelch`” (which is German for “fat elk”) and you will be greeted by the prompt

       dCache Admin (VII) (user=admin)


    (local) admin >

The password can now be changed with

     (local) admin > cd acm   
     (acm) admin > create user admin  
     (acm) admin > set passwd -user=admin <newPasswd> <newPasswd>   
     (acm) admin > ..  
     (local) admin > logoff  

HOW TO USE THE ADMIN INTERFACE
------------------------------

The command `help` lists all commands the cell knows and their parameters. However, many of the commands are only used for debugging and development purposes.

> **WARNING**
>
> Some commands are dangerous. Executing them without understanding what they do may lead to data loss.

Starting from the local prompt ((`local) admin >`) the command `cd` takes you to the specified [cell](rf-glossary.md#cell). In general the address of a cell is a concatenation of cell name @ symbol and the domain name. `cd` to a cell by:

    (local) admin > **cd <cellName>@<domainName>**

> **NOTE**
>
> If the cells are well-known, they can be accessed without adding the domain-scope. See [Chapter 5, The Cell Package](config-cellpackage.md) for more information.

The domains that are running on the dCache-instance, can be viewed in the layout-configuration (see  [Chapter 2, Installing dCache](install.md)). Additionally, there is the `topo` cell, which keeps track of the instance's domain topology. If it is running, it can be used to obtain the list of domains the following way:  

> **NOTE**  
>
> The topo cell rescans every five minutes which domains are running, so it can take some time until `ls` displays the full   domain list.  


Example:  
As the topo cell is a `well-known` cell you can `cd` to it directly by `cd topo`.    

Use the command `ls` to see which domains are running.    

      (local) admin > cd topo      
      (topo) admin > ls    
      adminDoorDomain  
      gsidcapDomain  
      dcapDomain  
      utilityDomain  
      gPlazmaDomain  
      webdavDomain  
      gridftpDomain  
      srmDomain  
      dCacheDomain  
      httpdDomain  
      namespaceDomain  
      poolDomain  
      (topo) admin > ..  
      (local) admin >  


The escape sequence `..` takes you back to the local prompt.  

The command `logoff` exits the admin shell.  

If you want to find out which cells are running on a certain domain, you can issue the command `ps` in the System `cell` of the domain.  

Example:  
For example, if you want to list the cells running on the `poolDomain`, `cd` to its `System` cell and issue the `ps` command.  

      (local) admin > cd System@poolDomain    
      (System@poolDomain) admin > ps  
        Cell List  
      ------------------  
      c-dCacheDomain-101-102  
      System  
      pool_2  
      c-dCacheDomain-101  
      pool_1  
      RoutingMgr  
      lm  

The cells in the domain can be accessed using `cd` together with the cell-name scoped by the domain-name. So first, one has to   get back to the local prompt, as the `cd` command will not work otherwise.  

> **NOTE**  
>  
> Note that `cd` only works from the local prompt. If the cell you are trying to access does not exist, the `cd` command will   complain.  
>  
>     Example:    
>     (local) admin > cd nonsense  
>     java.lang.IllegalArgumentException: Cannot cd to this cell as it doesn't exist  
> Type `..` to return to the `(local) admin >` prompt.  

Login to the routing manager of the `dCacheDomain` to get a list of all well-known cells you can directly `cd` to without having to add the domain.  

    Example:  
      (System@poolDomain) admin > ..  
      (local) admin > cd RoutingMgr@dCacheDomain  
      (RoutingMgr@dCacheDoorDomain) admin > ls  
      Our routing knowledge :  
       Local : [PoolManager, topo, LoginBroker, info]  
       adminDoorDomain : [pam]  
      gsidcapDomain : [DCap-gsi-example.dcache.org]  
      dcapDomain : [DCap-example.dcache.org]  
      utilityDomain : [gsi-pam, PinManager]  
      gPlazmaDomain : [gPlazma]  
      webdavDomain : [WebDAV-example.dcache.org]  
       gridftpDomain : [GFTP-example.dcache.org]  
      srmDomain : [RemoteTransferManager, CopyManager, SrmSpaceManager, SRM-example.dcache.org]  
       httpdDomain : [billing, srm-LoginBroker, TransferObserver]  
      poolDomain : [pool_2, pool_1]  
       namespaceDomain : [PnfsManager, dirLookupPool, cleaner]  

All cells know the commands `info` for general information about the cell and `show pinboard` for listing the last lines of the [pinboard](rf-glossary.md#pinboard) of the cell. The output of these commands contains useful information for solving problems.  

It is a good idea to get aquainted with the normal output in the following cells: `PoolManager, PnfsManager`, and the pool cells (e.g.,` <poolHostname>_1`).  

The most useful command of the pool cells is [rep ls](reference.md#rep-ls).  To execute this command `cd` into the pool. It lists the files which are stored in the pool by their `pnfs` IDs:  

    Example:    

      (RoutingMgr@dCacheDoorDomain) admin > ..  
      (pool_1) admin > rep ls  
      000100000000000000001120 <-P---------(0)[0]> 485212 si={myStore:STRING}  
      000100000000000000001230 <C----------(0)[0]> 1222287360 si={myStore:STRING}  
  Each file in a pool has one of the 4 primary states: “cached” (<C---), “precious” (<-P--), “from client” (<--C-), and        “from store” (<---S).  


See [the section called “How to Store-/Restore files via the Admin Interface”](config-hsm.md#how-to-store-restore-files-via-the-admin-interface) for more information about `rep ls`.  

The most important commands in the `PoolManager` are: `rc ls` and `cm ls -r`.  

`rc ls` lists the requests currently handled by the `PoolManager`. A typical line of output for a read request with an error condition is (all in one line):  

    Example:  
    
      (pool_1) admin > ..  
      (local) admin > cd PoolManager  
      (PoolManager) admin > rc ls  
      000100000000000000001230@0.0.0.0/0.0.0.0 m=1 r=1 [<unknown>]  
      [Waiting 08.28 19:14:16]  
      {149,No pool candidates available or configured for 'staging'}  

As the error message at the end of the line indicates, no pool was found containing the file and no pool could be used for staging the file from a tertiary storage system.  




See [the section called “Obtain information via the dCache Command Line Admin Interface”](config-hsm.md#how-to-monitor-whats-going-on) for more information about the command `rc ls`

Finally, [cm ls](reference.md#cm-ls) with the option `-r` gives the information about the pools currently stored in the cost module of the pool manager. A typical output is:

    Example:

      (PoolManager) admin > cm ls -r
      pool_1={R={a=0;m=2;q=0};S={a=0;m=2;q=0};M={a=0;m=100;q=0};PS={a=0;m=20;q=0};PC={a=0;m=20;q=0};
          (...continues...)   SP={t=2147483648;f=924711076;p=1222772572;r=0;lru=0;{g=20000000;b=0.5}}}
      pool_1={Tag={{hostname=example.org}};size=0;SC=0.16221282938326134;CC=0.0;}
      pool_2={R={a=0;m=2;q=0};S={a=0;m=2;q=0};M={a=0;m=100;q=0};PS={a=0;m=20;q=0};PC={a=0;m=20;q=0};
          (...continues...)   SP={t=2147483648;f=2147483648;p=0;r=0;lru=0;{g=4294967296;b=250.0}}}
      pool_2={Tag={{hostname=example.org}};size=0;SC=2.7939677238464355E-4;CC=0.0;}
      
      
While the first line for each pool gives the information stored in the cache of the cost module, the second line gives the    costs (SC: [space cost](rf-glossary.md#space-cost), CC: [performance cost](rf-glossary.md#performance-cost)) calculated for a (hypothetical) file of zero size. For details on how these are calculated and their meaning, see [the section called “Classic Partitions”](#config-poolmanager.md#classic-partitions).

CREATE A NEW USER
-----------------

To create a new user, <new-user> and set a new password for the user `cd` from the local prompt `((local) admin >)` to the acm, the access control manager, and run following command sequence:   

(local) admin > cd acm  

    (local) admin > cd acm  
    (acm) admin > create user <new-user>  
    (acm) admin > set passwd -user=<new-user> <newPasswd> <newPasswd>  


For the new created users there will be an entry in the directory **/etc/dcache/admin/users/meta.**  

> **NOTE**  
>
> As the initial user `admin` has not been created with the above command you will not find him in the directory **/etc/dcache/admin/users/meta.**  

Give the new user access to the PnfsManager.  

      (acm) admin > create acl cell.<cellName>.execute   
      (acm) admin > add access -allowed cell.<cellName>.execute <new-user>  

Example:  
Give the new user access to the PnfsManager.  
      (acm) admin > create acl cell.PnfsManager.execute  
      (acm) admin > add access -allowed cell.PnfsManager.execute <new-user>  
      
Now you can check the permissions by:  
      (acm) admin > check cell.PnfsManager.execute <new-user>  
      Allowed  
      (acm) admin > show acl cell.PnfsManager.execute  
      <noinheritance>  
      <new-user> -> true   

The following commands allow access to every cell for a user <new-user>:  

    (acm) admin > create acl cell.*.execute  
    (acm) admin > add access -allowed cell.*.execute <new-user>  


The following command makes a user as powerful as admin (dCache’s equivalent to the root user):

    (acm) admin > create acl *.*.*
    (acm) admin > add access -allowed *.*.* <new-user>


USE OF THE SSH ADMIN INTERFACE BY SCRIPTS
-----------------------------------------

The `ssh` admin interface can be used non-interactively by scripts. For this the dCache-internal ssh server uses public/private key pairs.

The file **/etc/dcache/authorized_keys** contains one line per user. The file has the same format as** ~/.ssh/authorized_keys** which is used by `sshd`. The keys in **/etc/dcache/authorized_keys** have to be of type RSA1 as dCache only supports SSH protocol 1. Such a key is generated with  

       [user] $ ssh-keygen -t rsa1 -C 'SSH1 key of <user>'  
      Generating public/private rsa1 key pair.  
      Enter file in which to save the key (/home/<user>/.ssh/identity):  
      Enter passphrase (empty for no passphrase):  
      Enter same passphrase again:  
      Your identification has been saved in /home/<user>/.ssh/identity.  
      Your public key has been saved in /home/<user>/.ssh/identity.pub.  
      The key fingerprint is:  
      c1:95:03:6a:66:21:3c:f3:ee:1b:8d:cb:46:f4:29:6a SSH1 key of <user>  

The passphrase is used to encrypt the private key (now stored in **/home/<user>/.ssh/identity**). If you do not want to enter the passphrase every time the private key is used, you can use ssh-add to add it to a running ssh-agent. If no agent is running start it with


    [user] $ if [ -S $SSH_AUTH_SOCK ] ; then echo "Already running" ; else eval `ssh-agent` ; fi

and add the key to it with

    [user] $ ssh-add
    Enter passphrase for SSH1 key of <user>:
    Identity added: /home/<user>/.ssh/identity (SSH1 key of <user>)

Now, insert the public key ~/.ssh/identity.pub as a separate line into /etc/dcache/authorized_keys. The comment field in this line “SSH1 key of <user>” has to be changed to the dCache user name. An example file is:

    1024 35 141939124(... many more numbers ...)15331 admin

Using ssh-add -L >> **/etc/dcache/authorized_keys** will not work, because the line added is not correct. The key manager within dCache will read this file every minute.

Now, the `ssh` program should not ask for a password anymore. This is still quite secure, since the unencrypted private key is only held in the memory of the `ssh-agent`. It can be removed from it with


    [user] $ ssh-add -d
    Identity removed: /home/<user>/.ssh/identity (RSA1 key of <user>)


In scripts, one can use a “Here Document” to list the commands, or supply them to `ssh` as standard-input (stdin). The following demonstrates using a Here Document:

    #!/bin/sh
    #
    #  Script to automate dCache administrative activity

    outfile=/tmp/$(basename $0).$$.out

    ssh -c blowfish -p 22223 admin@adminNode > $outfile << EOF
    cd PoolManager
    cm ls -r
    (more commands here)
    logoff
    EOF

or, the equivalent as stdin.

    #!/bin/bash
    #
    #   Script to automate dCache administrative activity.

    echo -e 'cd pool_1\nrep ls\n(more commands here)\nlogoff' \
      | ssh -c blowfish -p 22223 admin@adminNode \
      | tr -d '\r' > rep_ls.out
      
      

AUTHENTICATION AND AUTHORIZATION IN dCache  
------------------------------------------

In dCache digital certificates are used for authentication and authorisation. To be able to verify the chain of trust when using the non-commercial grid-certificates you should install the list of certificates of grid Certification Authorities (CAs). In case you are using commercial certificates you will find the list of CAs in your browser.  

      [root] # wget http://grid-deployment.web.cern.ch/grid-deployment/glite/repos/3.2/lcg-CA.repo    
      --2011-02-10 10:26:10--  http://grid-deployment.web.cern.ch/grid-deployment/glite/repos/3.2/lcg-CA.repo    
      Resolving grid-deployment.web.cern.ch... 137.138.142.33, 137.138.139.19    
      Connecting to grid-deployment.web.cern.ch|137.138.142.33|:80... connected.   
      HTTP request sent, awaiting response... 200 OK    
      Length: 449 [text/plain]  
      Saving to: `lcg-CA.repo'  
 
      100%[====================================================================>] 449         --.-K/s   in 0s  

      2011-02-10 10:26:10 (61.2 MB/s) - `lcg-CA.repo' saved [449/449]  
      [root] # mv lcg-CA.repo /etc/yum.repos.d/  
      [root] # yum install lcg-CA  
      Loaded plugins: allowdowngrade, changelog, kernel-module  
      CA                                                                                     |  951 B     00:00  
      CA/primary                                                                             |  15 kB     00:00  
      CA
      ...  

You will need a server certificate for the host on which your dCache is running and a user certificate. The host certificate needs to be copied to the directory **/etc/grid-security/** on your server and converted to **hostcert.pem** and **hostkey.pem** as described in [Using X.509 Certificates](config-gplazma.md#using-x509-certificates). Your user certificate is usually located in **.globus**. If it is not there you should copy it from your browser to **.globus** and convert the **.p12** file to **usercert.pem** and **userkey.pem**.  

Example:  

If you have the clients installed on the machine on which your dCache is running you will need to add a user to that machine in order to be able to execute the `voms-proxy-init` command and execute `voms-proxy-init` as this user.  


      [root] # useradd johndoe  

Change the password of the new user in order to be able to copy files to this account.  

      [root] # passwd johndoe  
      Changing password for user johndoe.  
      New UNIX password:  
      Retype new UNIX password:  
      passwd: all authentication tokens updated successfully.  
      [root] # su johndoe  
      [user] $ cd  
      [user] $ mkdir .globus  

Copy your key files from your local machine to the new user on the machine where the dCache is running.  

      [user] $ scp .globus/user*.pem johndoe@<dcache.example.org>:.globus  

Install glite-security-voms-clients (contained in the gLite-UI).  

      [root] # yum install glite-security-voms-clients  

Generate a proxy certificate using the command `voms-proxy-init`.  

    Example:    
    [user] $ voms-proxy-init    
    Enter GRID pass phrase:    
    Your identity: /C=DE/O=GermanGrid/OU=DESY/CN=John Doe   

    Creating proxy .............................................. Done   
    Your proxy is valid until Mon Mar  7 22:06:15 2011   


With `voms-proxy-init -voms <yourVO>` you can add VOMS attributes to the proxy. A user’s roles (Fully Qualified Attribute Names) are read from the certificate chain found within the proxy. These attributes are signed by the user’s VOMS server when the proxy is created. For the `voms-proxy-init -voms` command you need to have the file **/etc/vomses** which contains entries about the VOMS servers like  


    Example:  
    "desy" "grid-voms.desy.de" "15104" "/C=DE/O=GermanGrid/OU=DESY/CN=host/grid-voms.desy.de" "desy" "24"  
    "atlas" "voms.cern.ch" "15001" "/DC=ch/DC=cern/OU=computers/CN=voms.cern.ch" "atlas" "24"  
    "dteam" "lcg-voms.cern.ch" "15004" "/DC=ch/DC=cern/OU=computers/CN=lcg-voms.cern.ch" "dteam" "24"  
    "dteam" "voms.cern.ch" "15004" "/DC=ch/DC=cern/OU=computers/CN=voms.cern.ch" "dteam" "24"  
          

Now you can generate your voms proxy containing your VO.  

    Example:  

      [user] $ voms-proxy-init -voms desy  
      Enter GRID pass phrase:  
      Your identity: /C=DE/O=GermanGrid/OU=DESY/CN=John Doe  
      Creating temporary proxy ................................... Done  
      Contacting  grid-voms.desy.de:15104 [/C=DE/O=GermanGrid/OU=DESY/CN=host/grid-voms.desy.de] "desy" Done  
      Creating proxy .................... Done  
      Your proxy is valid until Thu Mar 31 21:49:06 2011  
 
 
Authentication and authorization in dCache is done by the GPLAZMA service. Define this service in the layout file.  

      [gPlazmaDomain]  
      [gPlazmaDomain/gplazma]  

In this tutorial we will use the [gplazmalite-vorole-mapping plugin](config-gplazma.md#the-gplazmalite-vorole-mapping-plug-in). To this end you need to edit the **/etc/grid-security/grid-vorolemap** and the **/etc/grid-security/storage-authzdb** as well as the **/etc/dcache/dcachesrm-gplazma.policy**.  
 
Example:  
The **/etc/grid-security/grid-vorolemap:**  
      "/C=DE/O=GermanGrid/OU=DESY/CN=John Doe" "/desy" doegroup  
The **/etc/grid-security/storage-authzdb:**  
      version 2.1  

      authorize  doegroup read-write 12345 1234 / / /  
      
The **/etc/dcache/dcachesrm-gplazma.policy:**   
      # Switches  
      xacml-vo-mapping="OFF"  
      saml-vo-mapping="OFF"  
      kpwd="OFF"  
      grid-mapfile="OFF"  
      gplazmalite-vorole-mapping="ON"  

      # Priorities  
      xacml-vo-mapping-priority="5"  
      saml-vo-mapping-priority="2"  
      kpwd-priority="3"  
      grid-mapfile-priority="4"  
      gplazmalite-vorole-mapping-priority="1"  
      
          

HOW TO WORK WITH SECURED dCache
-------------------------------

If you want to copy files into dCache with GSIdCap, SRM or WebDAV with certificates you need to follow the instructions in the section [above](#authentication-and-authorization-in-dcache).  



GSIDCAP  
-------

To use `GSIdCap` you must run a `GSIdCap` door. This is achieved by including the `gsidcap` service in your layout file on the machine you wish to host the door.  

    [gsidcapDomain]  
    [gsidcapDomain/dcap]  
    dcap.authn.protocol=gsi  

In addition, you need to have libdcap-tunnel-gsi installed on your worker node, which is contained in the gLite-UI.  


> **NOTE**
>
> As ScientificLinux 5 32bit is not supported by GLITE there is no libdcap-tunnel-gsi for SL5 32bit.

    [root] # yum install libdcap-tunnel-gsi

It is also available on the [dCap downloads page](https://www.dcache.org/downloads/dcap/).

    Example:
    [root] # rpm -i http://www.dcache.org/repository/yum/sl5/x86_64/RPMS.stable//libdcap-tunnel-gsi-2.47.5-0.x86_64.rpm  

The machine running the GSIdCap door needs to have a host certificate and you need to have a valid user certificate. In addition, you should have created a [voms proxy](config-gplazma.md#creating-a-voms-proxy) as mentioned [above](#authentication-and-authorization-in-dcache).  

Now you can copy a file into your dCache using GSIdCap  

    [user] $ dccp /bin/sh gsidcap://<dcache.example.org>:22128/data/world-writable/my-test-file3
    801512 bytes in 0 seconds  

and copy it back  

    [user] $ dccp gsidcap://<dcache.example.org>:22128/data/world-writable/my-test-file3 /tmp/mytestfile3.tmp
    801512 bytes in 0 seconds  

SRM  
---

To use the `SRM` you need to define the `srm` service in your layout file.  

    [srmDomain]  
    [srmDomain/srm]  

In addition, the user needs to install an `SRM` client for example the `dcache-srmclient`, which is contained in the gLite-UI, on the worker node and set the `PATH` environment variable.  

    [root] # yum install dcache-srmclient  

You can now copy a file into your dCache using the SRM,  

    [user] $ srmcp -2 file:////bin/sh srm://dcache.example.org:8443/data/world-writable/my-test-file4  

copy it back  

    [user] $ srmcp -2 srm://dcache.example.org:8443/data/world-writable/my-test-file4 file:////tmp/mytestfile4.tmp  

and delete it  

    [user] $ srmcp -2 srm://dcache.example.org:8443/data/world-writable/my-test-file4  

If the grid functionality is not required the file can be deleted with the `NFS` mount of the CHIMERA namespace:  

    [user] $ rm /data/world-writable/my-test-file4  

WEBDAV WITH CERTIFICATES  
------------------------

To use `WebDAV` with certificates you change the entry in **/etc/dcache/layouts/mylayout.conf** from  

    [webdavDomain]  
    [webdavDomain/webdav]  
    webdav.authz.anonymous-operations=FULL  
    webdav.root=/data/world-writable  

to  

    [webdavDomain]  
    [webdavDomain/webdav]  
    webdav.authz.anonymous-operations=NONE  
    webdav.root=/data/world-writable  
    webdav.authn.protocol=https  

Then you will need to import the host certificate into the dCache keystore using the command  

    [root] # dcache import hostcert  

and initialise your truststore by  

    [root] # dcache import cacerts  

Now you need to restart the WEBDAV domain  

    [root] # dcache restart webdavDomain  

and access your files via https://<dcache.example.org>:2880 with your browser.



> **IMPORTANT**
>
> If the host certificate contains an extended key usage extension, it must include the extended usage for server authentication. Therefore you have to make sure that your host certificate is either unrestricted or it is explicitly allowed as a certificate for `TLS Web Server Authentication`.

### Allowing authenticated and non-authenticated access with WebDAV  

You can also choose to have secure and insecure access to your files at the same time. You might for example allow access without authentication for reading and access with authentication for reading and writing.

    [webdavDomain]  
    [webdavDomain/webdav]  
    webdav.root=/data/world-writable  
    webdav.authz.anonymous-operations=READONLY  
    port=2880  
    webdav.authn.protocol=https  

You can access your files via https://<dcache.example.org>:2880 with your browser.

Files
=====

In this section we will have a look at the configuration and log files of dCache.

The dCache software is installed in various directories according to the Filesystem Hierarchy Standard. All configuration files can be found in  **/etc/dcache**.

Log files of domains are by default stored in **/var/log/dcache/<domainName>.log.**

More details about domains and cells can be found in [Chapter 5, The Cell Package.](config-cellpackage.md)

The most central component of a dCache instance is the PoolManager cell. It reads additional configuration information from the file **/var/lib/dcache/config/poolmanager.conf** at start-up. However, it is not necessary to restart the domain when changing the file. We will see an example of this below.  

<!--
  [???]: #in-install
  [above]: #dcache-unmounted
  [1]: #cf-gplazma
  [section\_title]: #intouch-admin-new-user
  [2]: #cf-cellpackage
  [3]: #in
  [4]: #cmd-rep_ls
  [5]: #cf-tss-pools-admin
  [6]: #cmd-rc_ls
  [7]: #cf-tss-monitor-clAdmin
  [8]: #cmd-cm_ls
  [9]: #cf-pm-classic
  [Using X.509 Certificates]: #cf-gplazma-certificates
  [gplazmalite-vorole-mapping plugin]: #cf-gplazma-plug-inconfig-vorolemap
  [10]: #intouch-certificates
  [DCAP downloads page]: http://www.dcache.org/downloads/dcap/
  [voms proxy]: #cf-gplazma-certificates-voms-proxy-init
--!>
