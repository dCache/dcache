Protocols
=========

DCAP options mover and client options
=====================================

DCAP is the native random access I/O protocol for files within dCache. In additition to the usual data transfer mechanisms, it supports all necessary file metadata and name space manipulation operations.

In order to optimize I/O transferrates and memory consumption DCAP allows to configure parameters within the client and the server. e.g:

-   TCP Socket send and receive buffer sizes.

-   I/O buffers sizes.

TCP send/recv buffer sizes from the servers point of view
---------------------------------------------------------

There are two parameters per I/O direction, determining the actual TCP send/recv buffer size used for each transfer. Those values can be set within the `config/pool.batch` file on the pool nodes.

-   `defaultSend/RecvBufferSize` : this value is used if the dCap client doesn't try to set this value. The default value for this parameter is 256K Bytes.

-   `maxSend/RecvBufferSize` : this value is the maximum value, the mover is allowed to use. It's used if either the `defaultSend/RecvBufferSize` is larger or the client asks for a larger value. The default value for this parameter is 1MBytes.

On the server side, the max/defaultSend/RecvBuffer value can either be set in the `config/pool.batch` file or in the `config/*.poollist` files.

Using the batch context :

    set context dCap3-maxSendBufferSize value in bytes
    set context dCap3-maxRecvBufferSize value in bytes
    set context dCap3-defaultSendBufferSize value in bytes
    set context dCap3-defaultRecvBufferSize value in bytes

Or it may specified in the create ... command line

      create diskCacheV111.pools.MultiProtocolPool2 ${0} \
      "!MoverMap \
      ${1} \
      -defaultSendBufferSize=value in bytes \
      *** \
      -${2} -${3} -${4} -${5} -${6} -${7} -${8} \
    "

The most appropriate way to specify those values on the server side is certainly to add the corresponding entry in the `config/...poollist`. The entry would look like

    dcache30_1  /dcache/pool  sticky=allowed maxSendBufferSize=value in bytes tag.hostname=dcache30 ***

Please note the different ways of using the '=' and the '-' sign in the different alternatives.

TCP send/recv buffer sizes from the dCap clients point of view
--------------------------------------------------------------

For a full list of dCap library API calls and dccp options, please refer to to `http://www.dcache.org/manuals/libdcap.shtml` and `http://www.dcache.org/manuals/dccp.shtml` respectively. To set the local and remote TCP buffer send and receive buffers either use the API call `dc_setTCPSend/ReceiveBuffer(int size)` or the `-r SIZE -s SIZE` dccp options. In both cases the value is transferred to the remote mover which tries to set the corresponding values. Please not the the server protects itself by having a maximum size for those values which it doesn't exceed. Please check the section 'TCP send/recv buffer sizes from the servers point of view' to learn how to change those values.

Specifying dCap open timeouts
=============================

In cases where dccp/dcap requests a file which is still on tertiary storage, the user resp. the administrator might what to limit the time, dccp/dCap waits in the open call until the file has been fetched from backend storage. This, so called `openTimeout`, can be specified on the server or on the client. In all cases the `-keepAlive` must be specified with an appropriate number of seconds on the cell create command in the door batch files. The following mechanisms are available to specify open timeouts :

| Precedence | Mechanism                | Key Name         | Example                          |
|------------|--------------------------|------------------|----------------------------------|
| Lowest     | context                  | dCap-openTimeout | set context dCap-openTimeout 200 |
| ...        | context                  | openTimeout      | set context openTimeout 200      |
| ...        | cell create command line | openTimeout      | -openTimeout=200                 |
| Highest    | dccp command line        | -o               | dccp -o200 SOURCE DESTINATION    |

    #
    #    dCap    D o o r (create command line example)
    #
    create dmg.cells.services.login.LoginManager DCap-2 \
                "${specialDCapPort} \
                 diskCacheV111.doors.DCapDoor \
                 -export \
                 *** \
                 -keepAlive=60 \
                 -openTimeout=300 \
                 *** \
                 -loginBroker=LoginBroker"

    #
    #    dCap    D o o r (context example)
    #
    set context dCap-openTimeout 200
    #
    create dmg.cells.services.login.LoginManager DCap-2 \
                "${specialDCapPort} \
                 diskCacheV111.doors.DCapDoor \
                 -export \
                 *** \
                 -keepAlive=60 \
                 *** \
                 -loginBroker=LoginBroker"

    PROMPT-USER dccp -o200 /pnfs/desy.de/data/dteam/private/myfile /dev/null

If the openTimeout expires while a read transfer is already active, this transfer will be interrupted, but it will automatically resume because the client can't destinguish between a network failure and a timeout. So the timeout disturbes the read but it will finally succeed. This is different for write. If a write is interrupted by a timeout in the middle of a transfer, dccp will stuck. (This is not a feature and needs further investigation).

Using the dCap protocol for strict file checking
================================================

The DCAP protocol allows to check whether a dataset is on tape only or has a copy on a dCache disk. The DCAP library API call is ` int dc_check(const char *path, const char
       *location)` and the dccp options are `-t -1
       -P`. For a full list of dCap library API calls and dccp options, please refer to to `http://www.dcache.org/manuals/libdcap.shtml` and `http://www.dcache.org/manuals/dccp.shtml` respectively. Using a standard dCache installation those calls will return a guess on the file location only. It is neither checked whether the file is really on that pool or if the pool is up. To get a strict checking a DOOR-DCAP has to be started with a special (-check=strict) option.

    #
    #    dCap    D o o r
    #
    create dmg.cells.services.login.LoginManager DCap-strict \
                "${specialDCapPort} \
                 diskCacheV111.doors.DCapDoor \
                 -check=strict \
                 -export \
                 -prot=telnet -localOk \
                 -maxLogin=1500 \
                 -brokerUpdateTime=120 \
                 -protocolFamily=dcap \
                 -loginBroker=LoginBroker"

This door will do a precise checking (-check=strict). To get the dCap lib and dccp to use this door only, the `dCache_DOOR` environment variable has to be set to `doorHost:specialDCapPort` in the shell, dccp is going to be used. In the following example we assume that the `specialDCapPort` has been set to `23126` :

    PROMPT-USER export dCache_DOOR=dcachedoorhost:23126
    PROMPT-USER dccp -P -t -1 /pnfs/domain.tv/data/cms/users/waste.txt

If PROG-DCCP returns `File is not cached` and this dCache instance is connected to an HSM, the file is no longer on one of the dCache pools but is assumed to have a copy within the HSM. If the PROG-DCCP returns this message and no HSM is attached, the file is either on a pool which is currently down or the file is lost.

Passive DCAP
============

The DCAP protocol, similiar to FTP, uses a control channel to request a transfer which is subsequently done through data channels. Per default, the data channel is initiated by the server, connecting to an open port in the client library. This is commonly known as active transfer. Starting with dCache 1.7.0 the DCAP protocol supports passive transfer mode as well, which consequently means that the client connects to the server pool to initiate the data channel. This is essential to support DCAP clients running behind firewalls and within private networks.

Preparing the server for dCap passive transfer
----------------------------------------------

The port(s), the server pools should listens on, can be specified by the `org.dcache.net.tcp.portrange` variable, as part of the 'java\_options' directive in the `config/dCacheSetup` configuration file. A range has to be given if pools are split amoung multiple JVMs. E.g:

    java_options="-server ... -Dorg.dcache.dcap.port=0 -Dorg.dcache.net.tcp.portrange=33115:33145"

Switching the DCAP library resp. PROG-DCCP to PASSIVE
-----------------------------------------------------

> **Note**
>
> The commonly used expression 'passive' is seen from the server perspective and actually means 'server passive'. From the client perspective this is of course 'active'. Both means that the client connects to the server to establish the data connection. This mode is supported by the server starting with 1.7.0 and dccp with 1-2-40 (included in 1.7.0)

The following DCAP API call switches all subsequent dc\_open calls to server-passive mode if this mode is supported by the corresponding door. (dCache Version &gt;= 1.7.0).

    void dc_setClientActive()

The environment variable dCache\_CLIENT\_ACTIVE switches the DCAP library to server-passive. This is true for DCAP, DCAP preload and PROG-DCCP.

PROG-DCCP switches to server-passive when issuing the `-A` command line option.

Access to SRM and GRIDFTP server from behind a firewall
=======================================================

This describes firewall issues from the clients perspective. [???] discusses the server side.

When files are transferred in GRIDFTP active mode from GRIDFTP server to the GRIDFTP client, server establishes data channel(s) by connecting to the client. In this case client creates a TCP socket, bound to some particular address on the client host, and sends the client host IP and port to the server. If the client host is running a firewall, firewall might refuse server's connection to the client's listening socket. Common solution to this problem is establishing a range of ports on the client's host that are allowed to be connected from Internet by changing firewall rules.Once the port range is defined the client can be directed to use one of the ports from the port ranges when creating listening tcp sockets.

Access with PROG-SRMCP
----------------------

If you are using PROG-SRMCP as a client you need to do the following:

-   create a directory `$HOME/.globus` if it does not exist.

-   create and/or edit a file `$HOME/.globus/cog.properties` by appending a new line reading

        tcp.port.range=min,max

    where min and max are the lower and upper bounds of the port range.

With the latest PROG-SRMCP release you can use the `globus_tcp_port_range` option:

    PROMPT-USER PROG-SRMCP -globus_tcp_port_range=minValue:maxValue ...

A range of ports open for TCP connections is specified as a pair of positive integers separated by ":". This is not set by default.

Access with PROG-GLOBUS-URL-COPY
--------------------------------

If you are transferring files from gridftp server using PROG-GLOBUS-URL-COPY, you need to define an environment variable GLOBUS\_TCP\_PORT\_RANGE, in the same shell in which PROG-GLOBUS-URL-COPY will be executed.

In sh/bash you do that by invoking the following command:

    PROMPT-USER export GLOBUS_TCP_PORT_RANGE="min,max"

in csh/tcsh you invoke:

    PROMPT-USER setenv GLOBUS_TCP_PORT_RANGE "min,max"

here min and max are again the lower and upper bounds of the port range

Disableing unauthenticated DCAP via SRM
=======================================

In some cases SRM transfers fail because they are tried via the plain DCAP protocol (URL starts with `dcap://`). Since plain DCAP is unauthenticated, the dCache server will have no information about the user trying to access the system. While the transfer will succeed if the UNIX file permissions allow access to anybody (e.g. mode 777), it will fail otherwise.

Usually all doors are registered in SRM as potential access points for dCache. During a protocol negotiation the SRM chooses one of the available doors. You can force PROG-SRMCP to use the GSIDCAP protocol (`-protocol=gsidcap`) or you can unregister plain, unauthenticated DCAP from known protocols: From the file `config/door.batch` remove `-loginBroker=LoginBroker` and restart DOOR-DCAP with

    PROMPT-ROOT jobs/door stop
    PROMPT-ROOT jobs/door -logfile=dCacheLocation/log/door.log start

  [???]: #cb-net-firewall
