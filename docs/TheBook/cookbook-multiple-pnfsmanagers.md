Running dCache with multiple pnfs and PnfsManagers
==================================================

Tigran Mkrtchyan
This chapter contains solutions for several running one dCache instance with multiple pnfses. Due to missinterpretation of serverId and serverName in erlier versions, this feature is available in dCache release 1.6.7 or later.

Multiple pnfs instances support based on the pnfsid with domain and convinied pnfs path. A pnfsid contains two parts - ID and serverID:

    000000000000000000000000.myserver.mydomain

Basicly, serverID is never used. While a pnfsID is unique only in the pnfs instance, ID+serverID uniueq as long as serverID is unique. This makes possible to destingwish between requsest to different pnfs instances.

Path based redirection based on convinied paths:

    /pnfs/myserver.mydomain/....

To enable multiple pnfses suport you need to start the PnfsManagerBroker cell and declare it as a PnfsManager. The main function of PnfsManagerBroker is a message redirection based on pnfs id of request or path. The domain invormation of a message, which sented to the domain PnfsManager, omited. Never the less all replyes returned to orign of a request become the domain.

           client(door)          => PnfsManager( broker ) : create /pnfs/mydomain/file0001
           PnfsManager( broker ) => PnfsManager-mydomain  : create /pnfs/mydomain/file0001
           PnfsManager-mydomain  => PnfsManager( broker ) : ID = 000000000000000000000000
           PnfsManager( broker ) => client(door)          : ID = 000000000000000000000000.mydomain

           client(door)          => PnfsManager( broker ) : stat 000000000000000000000000.mydomain
           PnfsManager( broker ) => PnfsManager-mydomain  : stat 000000000000000000000000
           PnfsManager-mydomain  => PnfsManager( broker ) : size=...; mdate=...;uid=....
           PnfsManager( broker ) => client(door)          : size=...; mdate=...;uid=....
           
           client(door)          => PnfsManager( broker ) : cachelocation /pnfs/mydomain/file0001
           PnfsManager( broker ) => PnfsManager-mydomain  : cachelocation /pnfs/mydomain/file0001
           PnfsManager-mydomain  => PnfsManager( broker ) : pool=poola, ID = 000000000000000000000000
           PnfsManager( broker ) => client(door)          : pool=poola, ID = 000000000000000000000000.mydomain
           
        

As soon as

      
    #
    # Multiple PnfsManagers
    #
    # forwarder
    create diskCacheV111.namespace.PnfsManagerBroker PnfsManager

    # for desy.de
    create diskCacheV111.namespace.PnfsManagerV3 PnfsManager-pnfs01.desy.de \
           "diskCacheV111.util.OsmInfoExtractor \
            -enableLargeFileSimulation \
            -storeFilesize  \
            -cmRelay=broadcast \
            -threads=4 \
            -default=pnfs01.desy.de\
            -defaultPnfsServer=pnfs01.desy.de \
            -pnfs=/pnfs/pnfs01.desy.de \
            -namespace-provider=diskCacheV111.namespace.provider.BasicNameSpaceProviderFactory \
            ..... \
           "
      
      
