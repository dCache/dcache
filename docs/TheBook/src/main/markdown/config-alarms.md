CHAPTER 16. THE ALARMS SERVICE
==============================

Table of Contents
-----------------

* [The Basic Setup](#the-basic-setup)  
      [Configure where the alarms service is Running](#configure-where-the-alarms-service-is-running)  
      [Types of Alarms](#types-of-alarms)    
      [Alarm Priority](#alarm-priority)  
      [Working with Alarms: Shell Commands](#working-with-alarms-shell-commands)   
      [Working with Alarms: Admin Commands](#working-with-alarms-admin-commands)    
      [Working with Alarms: The DCache-View Alarms Tab](#working-with-alarms-the-dcache-view-alarms-tab)       
* [Advanced Service Configuration: Enabling Automatic Cleanup](#advanced-service-configuration-enabling-automatic-cleanup)   
* [Advanced Service Configuration: Enabling Email Alerts](#advanced-service-configuration-enabling-email-alerts)        
* [Miscellaneous Properties of the Alarms Service](#miscellaneous-properties-of-the-alarm-service)     
* [Alarms SPI](#alarms-spi--service-provider-interface-)

dCache has an `alarms` backend service which records failures (*alarms*) 
requiring more or less urgent intervention. The service stores alarms by 
either in an XML file or an RDBMS. The service is turned off by default.

The alarms service is contacted by the frontend service; alarms are collected 
using a simple time range query.  Further filtering of the alarms occurs 
via the RESTful alarms API.   The web dCache-View alarm page provides for 
both the timestamp range and the filtering and sorting of alarms by fields. 
Admins also have access to buttons which allow them to
mark alarms as closed or to delete them altogether.

THE BASIC SETUP  
===============

It is not necessary to run the `alarms` service in a separate domain, though 
depending on the individual system configuration it may still be advisable not 
to embed the service in a domain already burdened with higher memory requirements. 

The alarms service stores only logging messages which are marked as alarms,
regardless of the logging level at which these alarms are generated (usually
as ERROR, but it is theoretically possible to create an alarm even as a TRACE
logging event).

In order to capture any alarms from other domains at startup, it is also necessary 
to arrange for the alarm service to start before the other doors and pools.

Currently, no support is given for multiplexed alarms services; the frontend
can only communicate with one such service.

Add the `alarms` service to a domain in the layout file in the usual manner:

        [alarmsDomain]
        [alarmsDomain/alarms]
        alarms.db.type=rdbms
        ...
        
OR

        [someotherDomain]
        [someotherDomain/alarms]
        alarms.db.type=rdbms
        [someotherDomain/someotherservice]
        ...
        

Note that the storage type setting `alarms.db.type` must be defined either 
in the layout or **/etc/dcache/dcache.conf** file because its default value 
is `off`; this can be set to either `xml`, or `rdbms`. In the latter case, 
the standard set of properties can be used to configure the connection url, 
db user, and so forth. Before using the `rdbms` option for the first time,
 be sure to run:

            createdb -U alarms.db.user alarms
        
to create the database; as usual, the actual schema will be initialized 
automatically when the service is started.

For the XML option, the storage file is usually found in the shared directory 
for alarms (corresponding to `alarms.dir`); the usual path 
is **/var/lib/dcache/store.xml**, but the location can be changed by 
setting `alarms.db.xml.path`. This will automatically be propagated 
to `alarms.db.url`. 

As a rule of thumb, the choice between XML and RDBMS is dictated by 
how much history is to be preserved. While the XML option is more lightweight 
and easier to configure, it is limited by performance, experiencing considerable 
read and write slowdown as the file fills (beyond 1000 entries or so). 
If you do not need to maintain records of alarms (and either manually delete 
alarms which have been serviced, or use the built-in cleanup feature – see below), 
then this option should be sufficient. Otherwise, the extra steps of installing 
postgreSQL on the appropriate node and creating the alarms database (as above) 
may be worth the effort.

CONFIGURE WHERE THE ALARMS SERVICE IS RUNNING
-----------------------------------------------

The alarms infrastructure is actually a wrapper around the logging layer 
and makes use of a simple tcp socket logger to transmit logging events to the 
server. In each domain, the **/etc/dcache/logback.xml** configuration 
references the following properties to control remote logging:

            dcache.log.level.remote=off
            dcache.log.server.host=localhost
            dcache.log.server.port=9867
        
As with the `alarms` service database type, remote logging is turned off by 
default. Under normal circumstances it should be sufficient to set this to `warn` 
in order to receive alarms. All alarms are currently in fact guaranteed to be 
sent at this logging level or higher. Setting this to a lower level should not 
affect performance, because even if there were alarms generated at, say, the
INFO level, all events are filtered for being marked as alarms in the client 
before being sent over the wire; however, at present there is no need to set 
the level any lower.

For the alarms endpoint, if all of your dCache domains run on the same host, 
then the default `localhost` value will work. But usually your dCache will 
not be configured to run on a single node, so each node will need to know 
the destination of the remote logging events. On all the nodes except where 
the actual `alarms` service resides, you will thus need 
to modify the **/etc/dcache/dcache.conf** file or the layout file to 
set the `dcache.log.server.host` property (and restart dCache if it is already up). 
The default port should usually not need to be modified; in any case, it needs 
to correspond to whatever port the service is running on. From inspection of 
the **/usr/share/dcache/alarms.properties** file, you can see that the alarms-specific 
properties mirror the logger properties:

         #  ---- Host on which this service is running
         alarms.net.host=${dcache.log.server.host}
         #  ---- TCP port the alarms service listens on
         alarms.net.port=${dcache.log.server.port}
        
The first property should not need any adjustment, but if `alarms.net.port` 
is modified, be sure to modify the `dcache.log.server.port` property on the other 
nodes to correspond to it. In general, it is advisable to work directly 
with the `dcache.log.server` properties everywhere.

Example:
An example of a dCache which consists of a head node, some door nodes and some pool nodes. 
If the head node contains the alarms service, set the property 
`dcache.log.server.host` on the pool nodes and on the door nodes to:

                dcache.log.server.host=<head-node>
           
TYPES OF ALARMS
---------------

The dCache alarm system runs on top of the logging system 
(and more specifically, depends on the `ch.qos.logback` logging library). 
It promotes normal logging events to alarm status by a special marker.
They all carry this general logging marker `ALARM` and also can carry sub-markers 
for type and uniqueness identifiers. They also carry information indicating the 
host, domain and service which emits them.   All alarms are internally
defined by dCache.  For getting the list of types, see the commands below.

ALARM PRIORITY
--------------

The notion of alarm or alert carries the implication that this particular error 
or condition requires user attention/intervention; there may be, however, 
differences in urgency which permit the ordering of such notices in terms of 
degree of importance. dCache allows the administrator complete control over 
this prioritization.

The available priority levels are:

-   CRITICAL
-   HIGH
-   MODERATE
-   LOW

Any alarm can be set to whatever priority level is deemed appropriate. 
This can be done through the admin interface commands (see below). Without 
any customization, all alarms are given a default priority level. 
This level can itself be changed via the value of 
<variable>alarms.priority-mapping.default</variable>, which by default 
is `critical`.

Filtering based on priority is possible both in the webadmin page (see below), and for alarms sent via email (<variable>alarms.email.threshold</variable>; fuller discussion of how to enable email alarms is given in a later section).

WORKING WITH ALARMS: SHELL COMMANDS
-----------------------------------

Some basic alarm commands are available as part of the dCache shell. 
The following is an abbreviated description; for fuller information, 
see the dCache man page.

**alarm send**    
Send an arbitrary alarm message to the alarm server. 
The remote server address is taken from the local values for 
<variable>dcache.log.server.host</variable> and <variable>dcache.log.server.port</variable>. 
If the [-t=TYPE] option is used, it must be a defined alarm type.

**alarm list**  
Displays a list of all alarm types currently defined in dCache code. 
Since these types can be modified with any incremental release, 
a listing in this manual would be of limited value. It is easy enough to 
check which ones currently are defined using this command or 
the predefined ls admin command.

WORKING WITH ALARMS: ADMIN COMMANDS
-----------------------------------

A similar set of commands is available through the admin interface. 
To see fuller information for each of these, do `\h [command]`.

**predefined ls**   
Print a list of all internally defined alarms.

**priority get default**   
Get the current default alarm priority value.

**priority ls [type]**  
Print a single priority level or sorted list of priority levels for all known alarms.

**priority reload [path]**   
Reinitialize priority mappings from saved changes.

**priority restore all**   
Set all defined alarms to the current default priority value.

**priority save [path]**    
Save the current priority mappings to persistent back-up.

**priority set type low|moderate|high|critical**  
Set the priority of the alarm type.

**priority set default low|moderate|high|critical**    
Set the default alarm priority value.

**send [OPTIONS] message**    
Send an alarm to the alarm service.

> **NOTE**
>
> It is possible to change the file location by setting the 
alarms.priority-mapping.path and/or alarms.dir properties 
in the layout or /etc/dcache/dcache.conf. As can be seen from the admin commands, 
it is also possible to specify the path as an option on the respective 
save and reload commands. Note, however, that this is meant mainly for temporary 
or back-up purposes, as the path defined in the local dcache configuration 
will remain unaltered after that command completes and the priority map will be 
reloaded from there once again whenever the domain is restarted.

> **NOTE**
>
> Any changes made via the priority set default command are in-memory only. 
To change this default permanently, set the <variable>alarms.priority-mapping.default</variable> 
property in the layout or /etc/dcache/dcache.conf.


WORKING WITH ALARMS: THE DCACHE-VIEW ALARMS TAB
---------------------------------------------

The Alarms View is available to all users currently as read-only.  To be
able to open/close or delete alarms, the user must log in with the 'admin' role
if that user has it.   Please refer to the dCache-View and frontend documentation
for further information.

The alarms table allows for filtering and sorting on all alarms fields, and 
also timestamped ranges for the basic query.

ADVANCED SERVICE CONFIGURATION: ENABLING AUTOMATIC CLEANUP
==========================================================

An additional feature of the alarms infrastructure is automatic cleanup of 
processed alarms. An internal thread runs every so often, and purges all alarms 
marked as `closed` with a timestamp earlier than the given window. 
This daemon can be configured using the properties 
`alarms.enable.cleaner`, 
`alarms.cleaner.timeout`, 
`alarms.cleaner.timeout.unit`, 
`alarms.cleaner.delete-entries-before` and 
`alarms.cleaner.delete-entries-before.unit`. 
The cleaner is off by default. This feature is mainly useful when running 
over an XML store, to mitigate slow-down due to bloat; nevertheless, 
there is nothing prohibiting its use with RDBMS.

ADVANCED SERVICE CONFIGURATION: ENABLING EMAIL ALERTS
=====================================================

To configure the server to send alarms via email, you need to set a series of 
alarm properties. No changes are necessary to any **logback.xml** file. 
The most important properties:

**alarms.enable.email, alarms.email.threshold** 
Off (false) and `critical` by default.

**alarms.email.smtp-host, alarms.email.smtp-port**  
Email server destination. The port defaults to 25.

**SMTP authentication and encryption**  
The SMTP client used by dCache supports authentication via plain user 
passwords as well as both the STARTTLS and SSL protocols. 
Note that STARTTLS differs from SSL in that, in STARTTLS, the connection 
is initially non-encrypted and only after the STARTTLS command 
is issued by the client (if the server supports it) does the connection 
switch to SSL. In SSL mode, the connection is encrypted right from the start. 
Which of these to use is usually determined by the server.

If username and password are left undefined, unauthenticated sends 
will be attempted, which may not be supported by the server.

The values to use for plain user/password authentication default 
to undefined. NOTE: while using SSL will guarantee encryption over the wire, 
there is currently no way of storing an encrypted password. Two possible 
workarounds: a. Set up an admin account with a plaintext password that is 
protected by root privileges but which can be shared among adminstrators or 
those with access to the host containing this file; b. Set up a host-based 
authentication to the server; the email admin will usually require the client 
IP, and it will need to be static in that case.

**sender and recipient**  
Only one sender may be listed, but multiple recipients can be indicated 
by a comma-separated list of email addresses.

See the shared defaults **/usr/share/dcache/alarms.properties** file 
for additional settings.

Miscellaneous Properties of the ALARMS Service
===================================================

There are a number of other settings avaible for customization; 
check the files **/usr/share/dcache/alarms.properties** for the complete 
list with explanations.

ALARMS SPI (service provider interface)
===================================================

It is possible to plug functionality into the alarms service by
implementing the SPI interface.  An example of this can be found
here:  

https://github.com/dCache/dcache-snow

  [Regular Expressions]: http://docs.oracle.com/javase/tutorial/essential/regex
