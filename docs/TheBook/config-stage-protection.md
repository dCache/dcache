CHAPTER 20.  STAGE PROTECTION
=============================

Table of Contents
-----------------

+ [Configuration of Stage Protection](#configuration-of-stage-protection)  
+ [Definition of the White List](#definition-of-the-white-list)  


A dCache system administrator may specify a list of DNs/FQANs which are allowed to trigger tape restores for files not being available on disk. Users, requesting tape-only files, and not being on that *white list, will* receive a permission error and no tape operation is launched. Stage protection can be enhanced to allow authorization specific to a dCache storage group. The additional configuration parameter is optional allowing the stage protection to be backwards compatible when stage authorization is not specific to a storage group.

CONFIGURATION OF STAGE PROTECTION
=================================

Stage protection can optionally be configured in the poolmanager rather than on the doors and the pinmanager. Thus the white list needs to be present on a single node only. To enable this, define the following parameter in **/etc/dcache/dcache.conf**:


    dcache.authz.staging.pep=PoolManager

The file name of the white list must be configured by setting the **dcache.authz.staging** parameter in **/etc/dcache/dcache.conf**:

    dcache.authz.staging=PATH-ODC-ED/StageConfiguration.conf

The parameter needs to be defined on all nodes which enforce the stage protection, i.e., either on the doors and the `pinmanager`, or in the `poolmanager` depending on the stage policy enforcement point.

DEFINITION OF THE WHITE LIST
============================

The Stage Configuration File will contain a white list. Each line of the white list may contain up to three regular expressions enclosed in double quotes. The regular expressions match the DN, FQAN, and the Storage Group written in the following format:

    "<DN>" ["<FQAN>" ["<StorageGroup>"] ]  

Lines starting with a hash symbol `#` are discarded as comments.  

The regular expression syntax follows the syntax defined for the [Java Pattern class](http://docs.oracle.com/javase/6/docs/api/java/util/regex/Pattern.html).

Example:  
Here are some examples of the White List Records:    

    ".*" "/atlas/Role=production"  
    "/C=DE/O=DESY/CN=Kermit the frog"  
    "/C=DE/O=DESY/CN=Beaker" "/desy"  
    "/O=GermanGrid/.*" "/desy/Role=.*"  

This example authorizes a number of different groups of users:

-   Any user with the FQAN  tlas/Role=production.  
-   The user with the DN /C=DE/O=DESY/CN=Kermit the frog, irrespective of which VOMS groups he belongs to.  
-   The user with the DN /C=DE/O=DESY/CN=Beaker but only if he is also identified as a member of VO desy (FQAN /desy)  
-   Any user with DN and FQAN that match /O=GermanGrid/.\* and /desy/Role=.\* respectively.

If a storage group is specified all three parameters must be provided. The regular expression `".*"` may be used to authorize any DN or any FQAN. Consider the following example:

Example:  
    ".*" "/atlas/Role=production" "h1:raw@osm"
    "/C=DE/O=DESY/CN=Scooter" ".*" "sql:chimera@osm"

In the example above:  

-   Any user with
    FQAN
    /atlas/Role=production
    is allowed to stage files located in the storage group
    h1:raw@osm
    .
-   The user  
    /C=DE/O=DESY/CN=Scooter
    , irrespective of which VOMS groups he belongs to, is allowed to stage files located in the storage group
    sql:chimera@osm
    .

With the plain `dCap` protocol the DN and FQAN are not known for any users.

Example:  
In order to allow all `dCap` users to stage files the white list should contain the following record:

    "" ""
    
In case this line is commented or not present in the white list, all DCAP users will be disallowed to stage files.

It is possible to allow all `dCap` users to stage files located in a certain storage group.

Example:   
In this example, all `dCap` users are allowed to stage files located in the storage group `h1:raw@osm`:

    "" "" "h1:raw@osm" 

  [Java Pattern class]: http://java.sun.com/javase/6/docs/api/java/util/regex/Pattern.html
