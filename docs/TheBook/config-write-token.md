Chapter 21.  Using Space Reservations without SRM
=================================================

Table of Contents
-----------------

* [The Space Reservation](#the-space-reservation)
* [The WriteToken tag](#the-writetoken-tag)
* [Copy a File into the WriteToken](#copy-a-file-into-the-writetoken)

If you are using space reservations, i.e. you set

    dcache.enable.space-reservation=true

in your configuration file and all of your pools are in [link groups](config-PoolManager.md#link-groups), then you can only write into dCache if a link group is available for your transfer. Using the `SRM` you can specify the link group to write into. If you want to use another protocol like `curl` or `xrootd` you cannot specify a link group. In this case you need to use the `WriteToken` directory tag. 

The Space Reservation
=====================

Before you can create a `WriteToken` tag you need to have a space reservation.

Space reservations are made for link groups. The file [`LinkGroupAuthorization.conf`](config-SRM.md#the spacemanagerlinkgroupauthorizationfile) needs to contain the link groups that can be used for space reservations. You need to specify the location of the file in the **/etc/dcache/dcache.conf** file.

    spacemanager.authz.link-group-file-name=/etc/dcache/LinkGroupAuthorization.conf

Example:  


In this example we will create the link group `WriteTokenLinkGroup`. Login to the [admin interface](intouch.md#the-admin-interface), `cd` to the `SrmSpaceManager` and list the current space reservations.

    (local) admin > cd SrmSpaceManager
    (SrmSpaceManager) admin > ls
    Reservations:
    total number of reservations: 0
    total number of bytes reserved: 0

    LinkGroups:
    total number of linkGroups: 0
    total number of bytes reservable: 0
    total number of bytes reserved  : 0
    last time all link groups were updated: Wed Aug 07 15:20:48 CEST 2013(1375881648312)

Currently there are no space reservations and no link groups. We create the link group `WriteTokenLinkGroup`.

    (SrmSpaceManager) admin > ..
    (local) admin > cd PoolManager
    (PoolManager) admin > psu create pgroup WriteToken_poolGroup
    (PoolManager) admin > psu addto pgroup WriteToken_poolGroup pool1
    (PoolManager) admin > psu removefrom pgroup default pool1
    (PoolManager) admin > psu create link WriteToken_Link any-store world-net any-protocol
    (PoolManager) admin > psu set link WriteToken_Link -readpref=10 -writepref=10 -cachepref=0 -p2ppref=-1
    (PoolManager) admin > psu add link WriteToken_Link WriteToken_poolGroup
    (PoolManager) admin > psu create linkGroup WriteToken_LinkGroup
    (PoolManager) admin > psu set linkGroup custodialAllowed WriteToken_LinkGroup true
    (PoolManager) admin > psu set linkGroup replicaAllowed WriteToken_LinkGroup true
    (PoolManager) admin > psu set linkGroup nearlineAllowed WriteToken_LinkGroup true
    (PoolManager) admin > psu set linkGroup onlineAllowed WriteToken_LinkGroup true
    (PoolManager) admin > psu addto linkGroup WriteToken_LinkGroup WriteToken_Link
    (PoolManager) admin > save
    (PoolManager) admin > ..
    (local) admin >

    (local) admin >cd SrmSpaceManager
    (SrmSpaceManager) admin > ls
    Reservations:
    total number of reservations: 0
    total number of bytes reserved: 0

    LinkGroups:
    0 Name:WriteToken_LinkGroup FreeSpace:6917935104 ReservedSpace:0 AvailableSpace:6917935104 VOs: onlineAllowed:true         nearlineAllowed:true replicaAllowed:true custodialAllowed:true outputAllowed:true UpdateTime:Wed Aug 07 15:42:03 CEST  2013(1375882923234)
    total number of linkGroups: 1
    total number of bytes reservable: 6917935104
    total number of bytes reserved  : 0
    last time all link groups were updated: Wed Aug 07 15:42:03 CEST 2013(1375882923234)

A space reservation can only be made, when there is a link group in the **LinkGroupAuthorization.conf** that can be used for the space reservation. Therefore, we configure the **LinkGroupAuthorization.conf** such that the link group `WriteToken_LinkGroup` can be used.  

    #SpaceManagerLinkGroupAuthorizationFile
    # this is comment and is ignored

    LinkGroup WriteToken_LinkGroup
    */Role=*

Now we can make a space reservation for that link group.  

    (SrmSpaceManager) admin > reserve -desc=WriteToken 6000000 10000
    10000 voGroup:null voRole:null retentionPolicy:CUSTODIAL accessLatency:ONLINE linkGroupId:0 size:6000000 created:Fri Aug 09     12:28:18 CEST 2013 lifetime:10000000ms expiration:Fri Aug 09 15:14:58 CEST 2013 description:WriteToken state:RESERVED used:0 allocated:0 

    (SrmSpaceManager) admin > ls
    Reservations:
    10000 voGroup:null voRole:null retentionPolicy:CUSTODIAL accessLatency:ONLINE linkGroupId:0 size:6000000 created:Fri Aug 09     12:26:26 CEST 2013 lifetime:10000000ms expiration:Fri Aug 09 15:13:06 CEST 2013 description:WriteToken state:RESERVED used:0 allocated:0 
    total number of reservations: 1
    total number of bytes reserved: 6000000

    LinkGroups:
    0 Name:WriteToken_LinkGroup FreeSpace:6917849088 ReservedSpace:6000000 AvailableSpace:6911849088 VOs:{*:*} onlineAllowed:true   nearlineAllowed:true replicaAllowed:true custodialAllowed:true outputAllowed:true UpdateTime:Fri Aug 09 12:25:57 CEST 2013(1376043957179)
    total number of linkGroups: 1
    total number of bytes reservable: 6911849088
    total number of bytes reserved  : 6000000 
    (SrmSpaceManager) admin >

The `WriteToken` Tag
====================

The `WriteToken` tag is a [directory tag](config-chimera.md#directory-tag). Create the `WriteToken` tag with

    [root] # /usr/bin/chimera writetag <directory> WriteToken [<IdOfSpaceReservation>]

Example:  

In the beginning of the Book we created the directory **/data** and the subdirectory **/data/world-writable**.

    [root] # /usr/bin/chimera ls /data/
    total 3
    drwxr-xr-x  3 0 0 512 Jul 23 14:59 .
    drwxrwxrwx  3 0 0 512 Jul 24 14:33 ..
    drwxrwxrwx 12 0 0 512 Jul 24 14:41 world-writable

Now, we create the directory **data/write-token** into which we want to write

    [root] # /usr/bin/chimera mkdir /data/write-token
    [root] # /usr/bin/chimera 777 chmod /data/write-token
    [root] # /usr/bin/chimera ls /data/
    total 4
    drwxr-xr-x  4 0 0 512 Aug 09 12:48 .
    drwxrwxrwx  3 0 0 512 Jul 24 14:33 ..
    drwxrwxrwx 12 0 0 512 Jul 24 14:41 world-writable
    drwxrwxrwx  2 0 0 512 Aug 09 12:48 write-token

and echo the space reservation into the WriteToken tag.

    [root] # /usr/bin/chimera writetag /data/write-token WriteToken [10000]
    
    
Copy a File into the `WriteToken`
=================================

Given that you have a `WriteToken` tag which contains the id of a valid space reseravtion, you can copy a file into a space reservation even if you are using a protocol that does not support space reservation.

Example:  

In the above example we echoed the id of a space reservation into the `WriteToken` tag. We can now copy a file into this space reservation.

    [root] # curl -T test.txt http://webdav-door.example.org:2880/data/write-token/curl-test.txt
    [root] #

 <!-- [link groups]: #cf-pm-linkgroups
  [`LinkGroupAuthorization.conf`]: #cf-srm-linkgroupauthfile
  [admin interface]: #intouch-admin
  [directory tag]: #chimera-tags
