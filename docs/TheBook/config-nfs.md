Chapter 12. dCache as NFSv4.1 Server 
==================================== 

Table of Contents

* [Setting up](#setting-up)
* [Configuring NFSv4.1 door with GSS-API support](#configuring-nfsv4.1-door-with-gss-api-support)
* [Configuring principal-id mapping for NFS access](#configuring-principal-id-mapping-for-nfs-access)



This chapter explains how to configure dCache in order to access it via the `NFSv4.1` protocol, allowing clients to mount dCache and perform POSIX IO using standard `NFSv4.1` clients.

> **Important**
>
> The `pNFS` mentioned in this chapter is the protocol `NFSv4.1/pNFS` and not the namespace pnfs.

Setting up
==========

To allow file transfers in and out of dCache using NFSv4.1/pNFS, a new NFSv4.1 door must be started. This door acts then as the mount point for NFS clients.

To enable the NFSv4.1 door, you have to change the layout file corresponding to your dCache-instance. Enable the NFS within the domain that you want to run it by adding the following line

    ..
    [<domainName>/nfs]
    nfs.version = 4.1
    ..
    
Example:
You can just add the following lines to the layout file:

    ..
    [nfs-${host.name}Domain]
    [nfs-${host.name}Domain/nfs]
    nfs.version = 4.1
    ..

In addition to run an NFSv4.1 door you need to add exports to the **/etc/exports** file. The format of **/etc/exports** is similar to the one which is provided by Linux:

    #
    <path> [host [(options)]]

Where <options> is a comma separated combination of:

**ro**  
matching clients can access this export only in read-only mode

**rw**  
matching clients can access this export only in read-write mode

**noacl**  
dCache ACLs will be ignored; only posix access permissions will be considered. This is the default.

**acl**  
dCache ACLs will be respected; if present, they override posix permissions. To view the ACLs at the NFS client side, use `nfs4_getfacl` which is in EL7 package `nfs4-acl-tools`.

**sec=krb5**  
matching clients must access **NFS** using RPCSEC_GSS authentication. The Quality of Protection (QOP) is *NONE*, e.g., the data is neither encrypted nor signed when sent over the network. Nevertheless the RPC packets header still protected by checksum. 

**sec=krb5**  
matching clients have to access **NFS** using RPCSEC_GSS authentication. The Quality of Protection (QOP) is *INTEGRITY*. The RPC requests and response are protected by checksum. 

**sec=krb5p**  
matching clients have to access **NFS** using RPCSEC_GSS authentication. The Quality of Protection (QOP) is *PRIVACY*. The RPC requests and response are protected by encryption. 


For example:

    /pnfs/dcache.org/data *.dcache.org (rw,sec=krb5i)

Notice, that security flavour used at mount time will be used for client - pool communication as well.

Multiple specifications can be declared like this:

    /pnfs/dcache.org/data *.dcache.org(rw) externalhost.example.org(ro)

In this example, hosts in the dcache.org may read and write, while host externalhost.example.org may only read.

If there are multiple path specifications, the shortest matching path wins. If there are multiple host/subnet specifications, the most precise specification wins.

Configuring NFSv4.1 door with GSS-API support
=============================================

Adding `sec=krb5` into **/etc/exports** is not sufficient to get kerberos authentication to work.

All clients, pool nodes and node running DOOR-NFS4 must have a valid kerberos configuration. Each clients, pool node and node running DOOR-NFS4 must have a **/etc/krb5.keytab** with `nfs` service principal:

    nfs/host.domain@<YOUR.REALM>

The **/etc/dcache/dcache.conf** on pool nodes and node running `NFSv4.1 door` must enable kerberos and RPCSEC_GSS: 

    nfs.rpcsec_gss=true
    dcache.authn.kerberos.realm=<YOUR.REALM>
    dcache.authn.jaas.config=/etc/dcache/gss.conf
    dcache.authn.kerberos.key-distribution-center-list=your.kdc.server

The **/etc/dcache/gss.conf** on pool nodes and node running `NFSv4.1` door must configure Java’s security module: 

    com.sun.security.jgss.accept {
    com.sun.security.auth.module.Krb5LoginModule required
    doNotPrompt=true
    useKeyTab=true
    keyTab="${/}etc${/}krb5.keytab"
    debug=false
    storeKey=true
    principal="nfs/host.domain@<YOUR.REALM>";
    };

Now your `NFS` client can securely access dCache.

Configuring principal-id mapping for NFS access
===============================================

The `NFSv4.1` uses utf8 based strings to represent user and group names. This is the case even for non-kerberos based accesses. Nevertheless UNIX based clients as well as dCache internally use numbers to represent uid and gids. A special service, called `idmapd`, takes care for principal-id mapping. On the client nodes the file **/etc/idmapd.conf** is usually responsible for consistent mapping on the client side. On the server side, in case of dCache mapping done through gplazma2. The `identity` type of plug-in required by id-mapping service. Please refer to [Chapter 10, Authorization in dCache](config-gplazma.md) for instructions about how to configure `gPlazma.

Note, that nfs4 domain on clients must match nfs.domain value in **dcache.conf**.

To avoid big latencies and avoiding multiple queries for the same information, like ownership of a files in a big directory, the results from `gPlazma` are cached within `NFSv4.1 door`. The default values for cache size and life time are good enough for typical installation. Nevertheless they can be overriden in **dcache.conf** or layoutfile: 

    ..
    # maximal number of entries in the cache
    nfs.idmap.cache.size = 512

    # cache entry maximal lifetime
    nfs.idmap.cache.timeout = 30

    # time unit used for timeout. Valid values are:
    # SECONDS, MINUTES, HOURS and DAYS
    nfs.idmap.cache.timeout.unit = SECONDS
    ..

  [???]: #cf-gplazma
