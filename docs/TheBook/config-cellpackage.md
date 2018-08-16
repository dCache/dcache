CHAPTER 5. THE CELL PACKAGE
===========================

All of dCache makes use of the cell package. It is a framework for a distributed and scalable server system in Java. The dCache system is divided into cells which communicate with each other via messages. Several cells run simultaneously in one domain.

Each domain runs in a separate Java virtual machine and each cell is run as a separate thread therein. Domain names have to be unique. The domains communicate with each other via `TCP` using connections that are established at start-up. The topology is controlled by the location manager service. In the standard configuration, all domains connect with the `dCacheDomain`, which routes all messages to the appropriate domains. This forms a star topology.

> **ONLY FOR MESSAGE COMMUNICATION**
>
> The `TCP` communication controlled by the location manager service is for the short control messages sent between cells. Any transfer of the data stored within dCache does not use these connections; instead, dedicated `TCP` connections are established as needed.

A single node provides the location-manager service. For a single-host dCache instance, this is `localhost`; for multi-host dCache instances, the hostname of the node providing this service must be configured using the `serviceLocatorHost` property.

The domain that hosts the location manager service is also configurable. By default, the service runs within the `dCacheDomain` domain; however, this may be changed by setting the `dcache.broker.domain` property. The port that the location manager listens on is also configurable, using the `dcache.broker.port` property; however, most sites may leave this property unaltered and use the default value.

Within this framework, cells send messages to other cells addressing them in the form cellName@domainName. This way, cells can communicate without knowledge about the host they run on. Some cells are [well known](rf-glossary.md#well-known-cell), i.e. they can be addressed just by their name without @domainName. Evidently, this can only work properly if the name of the cell is unique throughout the whole system. If two well known cells with the same name are present, the system will behave in an undefined way. Therefore it is wise to take care when starting, naming, or renaming the well known cells. In particular this is true for pools, which are well known cells.

A domain is started with a shell script **bin/dcache start** domainName. The routing manager and location manager cells are started in each domain and are part of the underlying cell package structure. Each domain will contain at least one cell in addition to them.  


