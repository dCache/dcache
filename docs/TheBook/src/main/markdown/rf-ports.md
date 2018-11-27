Chapter 29. dCache Default Port Values
======================================

You can use the command `dcache ports` to get the current list of ports used by
a running dCache instance.

The table below lists the *default* port values as defined in the \*.properties
files in  /usr/share/dcache/defaults/. Those files also provide information
about which ports may be deprecated.

 

| Port number | Description                                                                                     | Component                                                                         |
|-------------|-------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------|
| 111         | is used by portmapper for NFS                                                                   | portmap                                                                           |
| 1094        | is the **xrootd** port                                                                          | In- and outbound traffic on **xrootd** doors                                      |
| 2049        | is the **NFS** port                                                                             | In- and outbound traffic on **nfs** doors                                         |
| 2181        | Communication between dCache domains and a (standalone) Zookeeper cluster                       | zookeeper                                                                         |
| 2288        | used to access dCache's legacy web interface (via http)                                         | Inbound for httpdDomain                                                           |
| 2811        | is the **GSIFTP** port                                                                          | **ftp** doors                                                                     |
| 2880        | is the **WebDAV** port                                                                          | In- and outbound traffic on **webdav** doors                                      |
| 3456        | is for postmaster listening to requests for the PSQL database for dCache database functionality | Outbound for **SRM**, PnfsDomain, dCacheDomain and doors; inbound for PSQL server |
| 3880        | used to access dCache View (via https)                                                          | frontend                                                                          |
| 8443        | is the **SRM** port. See [the SRM chapter](config-SRM.md)                                       | Inbound for **SRM**                                                               |
| 11111       | is used for internal dCache communication (deprecated)                                          | By default: outbound for all components, inbound for dCache domain                |
| 22112       | provides access to the **info** service                                                         | info                                                                              |
| 22725       | is used for **Kerberos dCap**                                                                   | Inbound for **dCap** doors                                                        |
| 22126       | is the port for standard **FTP**                                                                | **ftp** doors                                                                     |
| 22127       | is the **Kerberos FTP** port                                                                    | **ftp** doors                                                                     |
| 22125       | is used for dCache's **dCap** protocol                                                          | Inbound for **dCap** doors                                                        |
| 22128       | is used for **GSIdCap**                                                                         | Inbound for **GSIdCap** doors                                                     |
| 22129       | is used for **dCap** authentication purposes                                                    | Inbound for **dCap** doors                                                        |
| 22224       | is used to access the dCache [Admin Interface via ssh](intouch.md#the-admin-interface)          | Inbound for adminDomain                                                           |
