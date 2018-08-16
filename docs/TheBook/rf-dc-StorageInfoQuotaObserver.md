The StorageInfoQuotaObserver cell
=================================

The StorageInfoQuotaObserver keeps track on spaces for all attached pools. The space granularity is based on the StorageInfo. It records precious, total, pinned, free and removable spaces of currently available pools. Pools, not active are not counted. Spaces may be queried by pool, storageinfo or link. For link queries, additional, link specific information is provided for convenience.

Calling Sequence
----------------

    #
    define context QuotaManagerSetup  endDefine
       set pool query interval 180
       set pool query steps     20
       set pool query break    200
       set poolmanager query interval 350
       set pool validity timeout 400
    endDefine
    #
    create diskCacheV111.services.space.StorageInfoQuotaObserver QuotaManager \
                  "default -export"
    #

Parameter setter commands
-------------------------

These commands allow to customize the behaviour of the StorageInfoQuotaObserver. They many determine how often information is updated and how aggressive the cells queries other services for updates. The meaning of the `set
      pool/poolmanager query interval` is obvious. Because of the fact, that the number of pools to query can be rather large, the cell allows to send the space update queries in junks with some time inbetween. The junk size is set by `set pool
      query steps` and the break between sending junks by `set pool query break`. If no pool information arrived within the `set pool validity timeout` the corresponding pool is declared OFFLINE and the spaces are no longer counted.

| Command                        | Argument Type | Argument Unit | Meaning                                                                |
|--------------------------------|---------------|---------------|------------------------------------------------------------------------|
| set pool query interval        | Time          | Seconds       | Time interval between pool space queries                               |
| set poolmanager query interval | Time          | Seconds       | Time interval between pool manager pool/link queries                   |
| set pool query break           | Time          | Milli-seconds | Time interval between pool query 'steps'                               |
| set pool query steps           | Counter       | None          | Number of space queries between 'break'                                |
| set pool validity timeout      | Time          | Seconds       | If if pool info arrived within this time, the pool is declared OFFLINE |

Information query commands
--------------------------

-   `show pool [poolName]`

-   `show link
    	   [-a]` Lists spaces per link. The -a option provides additional information, eg. the storage classes and pools assigned to the particular link.

-   `show sci` Lists spaces per storage element.

Messages
--------

This cells currently replies on the following cell messages. The different sections are all relative to `diskCacheV111.vehicles.`

### `PoolMgrGetPoolLinks`

The StorageInfoQuotaCell provides a list of `PoolLinkInfo` structures, one per known link, on arrival of the message. Each `PoolLinkInfo` is filled with the name of the link, the list of storage classes, this link is associtated to, and the totally available space, left in this link. OFFLINE pools are not counted.

### `QuotaMgrCheckQuotaMessage`

StorageInfoQuotaCell provides the soft and hard quota defined for the specified `StorageClass` together with the space used.
