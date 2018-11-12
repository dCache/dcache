PoolManager Commands
======================

rc ls
------

rc ls - List the requests currently handled by the CELL-POOLMNGR

synopsis
---------
rc ls [<regularExpression>] [-w]

Description
-----------

Lists all requests currently handled by the pool manager. With the option `-w` only the requests currently waiting for a response are listed. Only requests satisfying the regular expression are shown.

cm ls
------

cm ls - List information about the pools in the cost module cache.

synopsis
---------
cm ls [-r] [-d] [-s] [<fileSize>]

**-r ** 
Also list the tags, the space cost, and performance cost as calculated by the cost module for a file of size fileSize (or zero)

**-d**  
Also list the space cost and performance cost as calculated by the cost module for a file of size fileSize (or zero)

**-t ** 
Also list the time since the last update of the cached information in milliseconds.

Description
-----------

A typical output reads

  (PoolManager) admin > cm ls -r -d -t 12312434442
<poolName1>={R={a=0;m=2;q=0};S={a=0;m=2;q=0};M={a=0;m=100;q=0};PS={a=0;m=20;q=0};PC={a=0;m=20;q=0};
    (...line continues...)  SP={t=2147483648;f=924711076;p=1222772572;r=0;lru=0;{g=20000000;b=0.5}}}
<poolName1>={Tag={{hostname=<hostname>}};size=543543543;SC=1.7633947200606475;CC=0.0;}
<poolName1>=3180
<poolName2>={R={a=0;m=2;q=0};S={a=0;m=2;q=0};M={a=0;m=100;q=0};PS={a=0;m=20;q=0};PC={a=0;m=20;q=0};
    (...line continues...)  SP={t=2147483648;f=2147483648;p=0;r=0;lru=0;{g=4294967296;b=250.0}}}
<poolName2>={Tag={{hostname=<hostname>}};size=543543543;SC=0.0030372862312942743;CC=0.0;}
<poolName2>=3157

set pool decision
------------------

set pool decision -Set the factors for the calculation of the total costs of the pools.

synopsis
----------
set pool decision [-spacecostfactor=<scf>] [-cpucostfactor=<ccf>] [-costcut=<cc>]

scf  
The factor (strength) with which the space cost will be included in the total cost.

ccf  
The factor (strength) with which the performance cost will be included in the total cost.

cc  
Deprecated since version 5 of the pool manager.

Description
-----------
