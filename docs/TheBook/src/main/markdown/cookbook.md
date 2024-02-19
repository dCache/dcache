Part III. Cookbook
==================

This part contains guides for specific tasks a system administrator might want to perform.

## Table of Contents

- [dCache Clients](cookbook-clients.md)
    - [GSI-FTP](cookbook-clients.md#gsi-ftp)
    - [Listing a directory](cookbook-clients.md#listing-a-directory)
    - [Checking a file exists](cookbook-clients.md#checking-a-file-exists)
    - [Deleting files](cookbook-clients.md#deleting-files)
    - [Copying files](cookbook-clients.md#copying-files)
    - [dCap](cookbook-clients.md#dcap)
    - [dccp](cookbook-clients.md#dccp)
    - [Using the dCache client interposition library.](cookbook-clients.md#using-the-dcache-client-interposition-library)
    - [SRM](cookbook-clients.md#srm)
      - [Creating a new directory.](cookbook-clients.md#creating-a-new-directory)
      - [Removing files from dCache](cookbook-clients.md#removing-files-from-dcache)
      - [Removing empty directories from dCache](cookbook-clients.md#removing-empty-directories-from-dcache)
      - [srmcp for SRM v1](cookbook-clients.md#srmcp-for-srm-v1)
      - [srmcp for SRM v2.2](cookbook-clients.md#srmcp-for-v2.2)
    - [ldap](cookbook-clients.md#ldap)
    - [Using the LCG commands with dCache](cookbook-clients.md#using-the-lcg-commands-with-dcache)
      - [The lcg-gt Application](cookbook-clients.md#the-lcg-gt-application)
      - [The lcg-sd Application](cookbook-clients.md#the-lcg-sd-application)

- [Using dcap](cookbook-dCap.md)

- [High Availability with Replicable Services](cookbook-ha-with-replicable-services.md)
    - [Pools](cookbook-ha-with-replicable-services.md#pools)
    - [Doors](cookbook-ha-with-replicable-services.md#doors)
    - [Critical Central Services](cookbook-ha-with-replicable-services.md#critical-central-services)
    - [Non-critical Central Services](cookbook-ha-with-replicable-services.md#non-critical-central-services)

- [Migration of classic SE (nfs, disk) to dCache](cookbook-classic-se-to-dcache.md)

- [Pool Operations](cookbook-pool.md)
    - [Checksums](cookbook-pool.md#checksums)
    - [How to configure checksum calculation](cookbook-pool.md#how-to-configure-checksum-calculation)
    - [Migration Module](cookbook-pool.md#migration-module)
    - [Overview and Terminology](cookbook-pool.md#overview-and-terminology)
    - [Command Summary](cookbook-pool.md#command-summary)
    - [Examples](cookbook-pool.md#examples)
    - [Renaming a Pool](cookbook-pool.md#renaming-a-pool)
    - [Pinning Files to a Pool](cookbook-pool.md#pinning-files-to-a-pool)
    - [Keeping metadata on MongoDB](cookbook-pool.md#keeping-metadata-on-mongodb)
    - [Handling orphan movers](cookbook-pool.md#handling-orphan-movers)

- [PostgreSQL and dCache](cookbook-postgres.md)
    - [Installing a PostgreSQL Server](cookbook-postgres.md#installing-a-psql-server)
    - [Configuring Access to PostgreSQL](cookbook-postgres.md#configuring-access-to-psql)
    - [Performance of the PostgreSQL Server](cookbook-postgres.md#performance-of-the-postgresql-server)

- [Transport Security](cookbook-transport-security.md)
    - [Configuring a secure WebDAV door](cookbook-transport-security.md#configuring-a-secure-webdav-door)
    - [Configuring Java](cookbook-transport-security.md#configuring-java)
    - [Testing protocols and cipher suites](cookbook-transport-security.md#testing-protocols-and-cipher-suites)
    - [HTTP header hardening](cookbook-transport-security.md#http-header-hardening)
    - [DNS CAA records](cookbook-transport-security.md#dns-caa-records)

- [Writing HSM Plugins](cookbook-writing-hsm-plugins.md)
    - [Nearline Requests](cookbook-writing-hsm-plugins.md#nearline-requests)
    - [Identifying Replicas](cookbook-writing-hsm-plugins.md#identifying-replicas)
    - [Request Lifecycle](cookbook-writing-hsm-plugins.md#request-lifecycle)
    - [The Nearline Storage SPI](cookbook-writing-hsm-plugins.md#the-nearline-storage-spi)
    - [AbstractBlockingNearlineStorage](cookbook-writing-hsm-plugins.md#abstractblockingnearlinestorage)
    - [Maven Archetype](cookbook-writing-hsm-plugins.md#maven-archetype)
    - [Examples](cookbook-writing-hsm-plugins.md#examples)

- [QoS Policies](cookbook-qos-policies.md)
    - [QoS Policy Schema](cookbook-qos-policies.md#qos-policy-definition)
    - [QoS Policy Management](cookbook-qos-policies.md#managing-policies)
    - [Applying a QoS Policy to a file](cookbook-qos-policies.md#applying-policies-to-files)

- [Advanced Tuning](cookbook-advanced.md)
   - [Multiple Queues for Movers in each Pool](cookbook-advanced.md#multiple-queues-for-movers-in-each-pool)
      - [Description](cookbook-advanced.md#description)
      - [Solution](cookbook-advanced.md#solution)
      - [Configuration](cookbook-advanced.md#configuration)
      - [Tunable Properties for Multiple Queues](cookbook-advanced.md#tunable-properties-for-multiple-queues)
   - [Tunable Properties](cookbook-advanced.md#tunable-properties)
      - [dCap](cookbook-advanced.md#dcap)
      - [GridFTP](cookbook-advanced.md#gridftp)
      - [SRM](cookbook-advanced.md#srm)

- [Complex Network Configuration](cookbook-net.md)
    - [Firewall Configuration](cookbook-net.md#firewall-configuration)
    - [Basic Installation](cookbook-net.md#basic-installation)
    - [Multi-Node with Firewalls](cookbook-net.md#multi-node-with-firewalls)
    - [GridFTP Connections via two or more Network Interfaces](cookbook-net.md#gridftp-connections-via-two-or-more-network-interfaces)
    - [GridFTP with Pools in a Private Subnet](cookbook-net.md#gridftp-with-pools-in-a-private-subnet)

- [Debugging](cookbook-debugging.md)
