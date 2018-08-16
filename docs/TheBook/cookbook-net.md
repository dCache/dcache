Chapter 25. Complex Network Configuration
=========================================

Table of Contents

+ [Firewall Configuration](#firewall-configuration)  

    [Basic Installation](#basic-installations)  
    [Multi-Node with Firewalls](#multi-node-with-firewalls)  

+ [GridFTP Connections via two or more Network Interfaces](#gridftp-connections-cia-two-or-more-network-interfaces)  

+ [GridFTP with Pools in a Private Subnet](#gridftp-with-pools-in-a-private-subnet)  


This chapter contains solutions for several non-trivial network configurations. The first section discusses the interoperation of dCache with firewalls and does not require any background knowledge about dCache other than what is given in the installation guide ([Chapter 2, Installing dCache](install.md)) and the first steps tutorial ([Chapter 3, Getting in Touch with dCache](intouch.md). The following sections will deal with more complex network topologies, e.g. private subnets. Even though not every case is covered, these cases might help solve other problems, as well. Intermediate knowledge about dCache is required. 

> **Warning**
>
> The TCP and UDP ports used for dCache internal communication (port `11111` by default) *MUST* be subject to firewall control so that only other dCache nodes can access them. Failure to do this will allow an attacker to issue arbitrary commands on any node within your dCache cluster, as whichever user the dCache process runs.

Firewall Configuration
======================

The components of a dCache instance may be distributed over several hosts (nodes). Some of these components are accessed from outside and consequently the firewall needs to be aware of that. We contemplate two communication types, the dCache internal communication and the interaction from dCache with clients.

Since dCache is very flexible, most port numbers may be changed in the configuration. The command `dcache ports` will provide you with a list of services and the ports they are using.

Basic Installation
------------------

This section assumes that all nodes are behind a firewall and have full access to each other.

**dCache internal.**

-   As we assume that all nodes are behind a firewall and have full access to each other there is nothing to be mentioned here.

-   On the pool nodes the LAN range ports need to be opened to allow pool to pool communication. By default these are ports `33115-33145` (set by the properties `dcache.net.lan.port.min` and `dcache.net.lan.port.max`).

**dCache communication with client.**

-   The door ports need to be opened to allow the clients to connect to the doors.

-   The WAN/LAN range ports need to be opened to allow the clients to connect to the pools. The default values for the WAN port range are `20000-25000`. The WAN port range is defined by the properties `dcache.net.wan.port.min` and `dcache.net.wan.port.max`.

Multi-Node with Firewalls
-------------------------

Multinode setup with firewalls on the nodes.

**dCache internal.**

-   The `LocationManager` server runs in the `dCacheDomain`. By default it is listening on UDP port `11111`. Hence, on the head node port `11111` needs to be opened in the firewall to allow connections to the `LocationManager`. Remember to limit this so that only other dCache nodes are allowed access. 

-   On the pool nodes the LAN range ports need to be opened to allow pool to pool communication. By default these are ports `33115-33145` (set by the properties `dcache.net.lan.port.min` and `dcache.net.lan.port.max`). 

**dCache communication with client.**

-   The door ports need to be opened to allow the clients to connect to the doors.

-   The WAN/LAN range ports need to be opened to allow the clients to connect to the pools. The default values for the WAN port range are `20000-25000`. The WAN port range is defined by the properties `dcache.net.wan.port.min` and `dcache.net.wan.port.max`.

More complex setups are described in the following sections.

GRIDFTP Connections via two or more Network Interfaces
======================================================

Description
-----------

The host on which the `GridFTP` door is running has several network interfaces and is supposed to accept client connections via all those interfaces. The interfaces might even belong to separate networks with no routing from one network to the other.

As long as the data connection is opened by the `GridFTP` server (passive FTP mode), there is no problem with having more than one interface. However, when the client opens the data connection (active FTP mode), the door (FTP server) has to supply it with the correct interface it should connect to. If this is the wrong interface, the client might not be able to connect to it, because there is no route or the connection might be inefficient.

Also, since a `GridFTP` server has to authenticate with an `X.509` grid certificate and key, there needs to be a separate certificate and key pair for each name of the host or a certificate with alternative names. Since each network interface might have a different name, several certificates and keys are needed and the correct one has to be used, when authenticating via each of the interfaces.

Solution
--------

Define two domains, one for the internal and one for the external use. Start a separate SRM and GRIDFTP service in these domains.

The `srm` and the `gridftp` service have to be configured with the property `listen`, only to listen on the interface they should serve. The locations of the grid host certificate and key files for the interface have to be specified explicitly with the properties `dcache.authn.hostcert.cert` and `dcache.authn.hostcert.key`.

In this example we show a setup for two `GridFTP` doors serving two network interfaces with the hostnames `door-internal` (111.111.111.5) and `door-external` (222.222.222.5) which are served by two GRIDFTP doors in two domains.

    [internalDomain]
    listen=111.111.111.5
    dcache.authn.hostcert.cert=PATH-ODE-ED/interface-cert-internal.pem
    dcache.authn.hostcert.key=PATH-ODE-ED/interface-key-internal.pem
    [internalDomain/srm]
    srm.cell.name=srm-internal
    srm.protocols.loginbroker=loginbroker-internal
    srm.net.host=door-internal
    [internalDomain/ftp]
    ftp.authn.protocol = gsi
    ftp.cell.name=GFTP-door-internal
    dcache.service.loginbroker=loginbroker-internal

    [externalDomain]
    listen=222.222.222.5
    dcache.authn.hostcert.cert=PATH-ODE-ED/interface-cert-external.pem
    dcache.authn.hostcert.key=PATH-ODE-ED/interface-key-external.pem
    [externalDomain/srm]
    srm.cell.name=srm-external
    srm.protocols.loginbroker=loginbroker-external
    srm.net.host=door-external
    [externalDomain/ftp]
    ftp.authn.protocol = gsi
    ftp.cell.name=GFTP-door-external
    dcache.service.loginbroker=loginbroker-external

GRIDFTP with Pools in a Private Subnet
======================================

Description
-----------

If pool nodes of a dCache instance are connected to a secondary interface of the `GridFTP` door, e.g. because they are in a private subnet, the `GridFTP` door will still tell the pool to connect to its primary interface, which might be unreachable.

The reason for this is that the control communication between the door and the pool is done via the network of `TCP` connections which have been established at start-up. In the standard setup this communication is routed via the dCache domain. However, for the data transfer, the pool connects to the `GridFTP` door. The IP address it connects to is sent by the `GridFTP` door to the pool via the control connection. Since the `GridFTP` door cannot find out which of its interfaces the pool should use, it normally sends the IP address of the primary interface.

Solution
--------

Tell the `GridFTP` door explicitly which IP it should send to the pool for the data connection with the `ftp.net.internal` property.

Example:

E.g. if the pools should connect to the secondary interface of the `GridFTP` door host which has the IP address `10.0.1.1`, set

    ftp.net.internal=10.0.1.1

in the **/etc/dcache/dcache.conf** file.

  [???]: #in
  [1]: #intouch
