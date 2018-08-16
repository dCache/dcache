Chapter 29. dCache Default Port Values 
======================================

You can use the command `dcache ports` to get the list of ports used by dCache.

| Port number     | Description                                                                                        | Component                                                                      |
|-----------------|----------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------|
| 32768 and 32768 | is used by the NFS layer within dCache which is based upon rpc. This service is essential for rpc. | NFS                                                                            |
| 1939 and 33808  | is used by portmapper which is also involved in the rpc dependencies of dCache.                    | portmap                                                                        |
| 34075           | is for postmaster listening to requests for the PSQL database for dCache database functionality.   | Outbound for **SRM**, PnfsDomain, dCacheDomain and doors; inbound for PSQL server. |
| 33823           | is used for internal dCache communication.                                                         | By default: outbound for all components, inbound for dCache domain.            |
| 8443            | is the **SRM** port. See [ Chapter 13, dCache Storage Resource Manager ](config-srm.md)                                                                         | Inbound for **SRM**                                                                |
| 2288            | is used by the web interface to dCache.                                                            | Inbound for httpdDomain                                                        |
| 22223           | is used for the dCache admin interface. See [ the section called “The Admin Interface”](intouch.md#the-admin-interface)                                               | Inbound for adminDomain                                                        |
| 22125           | is used for the dCache **dCap** protocol.                                                              | Inbound for **dCap** door                                                          |
| 22128           | is used for the dCache **GSIdCap** .                                                                   | Inbound for **GSIdCap** door                                                       |

  [???]: #cf-srm
  [1]: #intouch-admin
