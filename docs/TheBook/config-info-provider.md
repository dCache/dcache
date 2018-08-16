CHAPTER 19. GLUE INFO PROVIDER
==============================

Table of Contents
---------------------
+ [Internal collection of information](#internal-collection-of-information)
+ [Configuring the info service](#configuring-the-info-service)
+ [Testing the info provider](#testing-the-info-provider)
+ [Decommissioning the old info provider](#decommissioning-the-old-info-provider)
+ [Publishing dCache information](#publishing-dcache-information)
+ [Troubleshooting BDII problems](#troubleshooting-bdii-problems)
+ [Updating information](#updating-information)


The GLUE information provider supplied with dCache provides the information about the dCache instance in a standard format called GLUE. This is necessary so that WLCG infrastructure (such as FTS) and clients using WLCG tools can discover the dCache instance and use it correctly.

The process of configuring the info-provider is designed to have the minimum overhead so you can configure it manually; however, you may prefer to use an automatic configuration tool, such as YAIM.

> **NOTE**
>
> Be sure you have at least v2.0.8 of glue-schema RPM installed on the node running the info-provider.

This chapter describes how to enable and test the dCache-internal collection of information needed by the info-provider. It also describes how to configure the info-provider and verify that it is working correctly. Finally, it describes how to publish this information within BDII, verify that this is working and troubleshoot any problems.

> **WARNING**
>
> Please be aware that changing information provider may result in a brief interruption to published information. This may have an adverse affect on client software that make use of this information.

INTERNAL COLLECTION OF INFORMATION
==================================

The info-provider takes as much information as possible from dCache. To achieve this, it needs the internal information-collecting service, `info`, to be running and a means to collect that information: `httpd`. Make sure that both the `httpd` and `info` services are running within your dCache instance. By default, the `info` service is started on the admin-node; but it is possible to configure dCache so it runs on a different node. You should run only one `info` service per dCache instance.

The traditional (pre-1.9.7) allocation of services to domains has the `info` cell running in the `infoDomain` domain. A dCache system that has been migrated from this old configuration will have the following fragment in the node's layout file:

    [infoDomain]
    [infoDomain/info]

It is also possible to run the `info` service inside a domain that runs other services. The following example show the N domain that hosts the `admin, httpd, topo`  and `info` services.

    [information]
    [information/admin]
    [information/httpd]
    [information/topo]
    [information/info]

For more information on configuring dCache layout files, see [the section called “Defining domains and services”](install.md#defining-domains-and-services).

Use the `dcache services` command to see if a particular node is configured to run the `info` service. The following shows the output if the node has an `information`  domain that is configured to run the `info`  cell.

    [root] # dcache services | grep info
    information info        info               /var/log/dCache/information.log

If a node has no domain configured to host the CELL-INFO service then the above `dcache services` command will give no output:

    [root] # dcache services | grep info

If no running domain within *any* node of your dCache instance is running the `info` service then you must add the service to a domain and restart that domain.

Example:
In this example, the `info` service is added to the `example` domain. Note that the specific choice of domain (example) is just to give a concrete `example`; the same process may be applied to a different domain.

The layouts file for this node includes the following definition for the `example` domain:

    [example]
    [example/admin]
    [example/httpd]
    [example/topo]

By adding the extra line [example/info] to the layouts file, in future, the example domain will host the info service.

    [example]
    [example/admin]
    [example/httpd]
    [example/topo]
    [example/info]

To actually start the CELL-INFO cell, the DOMAIN-EXAMPLE domain must be restarted.

    [root] # dcache restart example
    Stopping example (pid=30471) 0 done
    Starting example done


With the `example`domain restarted, the `info` service is now running.

You can also verify both the `httpd` and `info` services are running using the `wget` command. The specific command assumes that you are logged into the node that has the `httpd` service (by default, the admin node). You may run the command on any node by replacing localhost with the hostname of the node running the `httpd` service.

The following example shows the output from the `wget` when the `info` service is running correctly:

    [root] # wget -O/dev/null http://localhost:2288/info
    --17:57:38--  http://localhost:2288/info
    Resolving localhost... 127.0.0.1
    Connecting to localhost|127.0.0.1|:2288... connected.
    HTTP request sent, awaiting response... 200 Document follows
    Length: 372962 (364K) [application/xml]
    Saving to: `/dev/null'

    100%[===========================================================================
    ===>] 372,962     --.-K/s   in 0.001s

    17:57:38 (346 MB/s) - `/dev/null' saved [372962/372962]

If the `httpd` service isn't running then the command will generate the following output:

    [root] # wget -O/dev/null http://localhost:2288/info
      --10:05:35--  http://localhost:2288/info
                 => `/dev/null'
      Resolving localhost... 127.0.0.1
      Connecting to localhost|127.0.0.1|:2288... failed: Connection refused.

To fix the problem, ensure that the `httpd` service is running within your dCache instance. This is the service that provides the web server monitoring within dCache. To enable the service, follow the same procedure for enabling the `info` cell, but add the `httpd` service within one of the domains in dCache.

If running the `wget` command gives an error message with `Unable to contact the info cell. Please ensure the info cell is running`:

    [root] # wget -O/dev/null http://localhost:2288/info
      --10:03:13--  http://localhost:2288/info
                 => `/dev/null'
      Resolving localhost... 127.0.0.1
      Connecting to localhost|127.0.0.1|:2288... connected.
      HTTP request sent, awaiting response... 503 Unable to contact the info cell.  Pl
    ease ensure the info cell is running.
      10:03:13 ERROR 503: Unable to contact the info cell.  Please ensure the info cel
    l is running..

This means that the `info` service is not running. Follow the instructions for starting the `info` service given above.

CONFIGURING THE INFO PROVIDER 
=============================

In the directory **/etc/dcache** you will find the file **info-provider.xml**. This file is where you configure the info-provider. It provides information that is difficult or impossible to obtain from the running dCache directly.

You must edit the **info-provider.xml** to customise its content to match your dCache instance. In some places, the file contains place-holder values. These place-holder values must be changed to the correct values for your dCache instance.


> **CAREFUL WITH < AND & CHARATERS**
>
> Take care when editing the **info-provider.xml** file! After changing the contents, the file must remain valid, well-formed XML. In particular, be very careful when writing a less-than symbol (`<`) or an ampersand symbol (`&`).
>
> -   Only use an ampersand symbol (`&`) if it is part of an entity reference. An entity reference is a sequence that starts with an ampersand symbol and is terminated with a semi-colon (`;`), for example `&gt;` and `&apos;` are entity markups.
>
>     If you want to include an ampersand character in the text then you must use the `&amp;` entity; for example, to include the text “me & you” the XML file would include `me
>     	    &amp; you`.
>
> -   Only use a less-than symbol (`<`) when starting an XML element; for example, `<constant id="TEST">A test
>     	    value</constant>`.
>
>     If you want to include a less-than character in the text then you must use the `&lt;` entity; for example, to include the text “1 &lt; 2” the XML file would include `1 &lt;
>     	    2`.
> Example:
> The following example shows the `SE-NAME` constant (which provides a human-readable description of the dCache instance) from a well-formed `info-provider.xml` configuration file:
>
>     <constant id="SE-NAME">Simple &amp; small dCache instance for small VOs
>     (typically &lt; 20 users)</constant>
>
> The `SE-NAME` constant is configured to have the value “Simple & small dCache instance for small VOs (typically &lt; 20 users)”. This illustrates how to include ampersand and less-than characters in an XML file.
>
When editing the **info-provider.xml** file, you should *only* edit text between two elements or add more elements (for lists and mappings). You should *never* alter the text inside double-quote marks.

Example:
This example shows how to edit the `SITE-UNIQUE-ID` constant. This constant has a default value `EXAMPLESITE-ID`, which is a place-holder value and must be edited.

    <constant id="SITE-UNIQUE-ID">EXAMPLESITE-ID</constant>

To edit the constant's value, you must change the text between the start- and end-element tags: `EXAMPLESITE-ID`. You *should not* edit the text `SITE-UNIQUE-ID` as it is in double-quote marks. After editing, the file may read:

    <constant id="SITE-UNIQUE-ID">DESY-HH</constant>

The **info-provider.xml** contains detailed descriptions of all the properties that are editable. You should refer to this documentation when editing the **info-provider.xml**.

TESTING THE INFO PROVIDER
=========================

Once you have configured **info-provider.xml** to reflect your site's configuration, you may test that the info provider produces meaningful results.

Running the info-provider script should produce GLUE information in LDIF format; for example:

    [root] # dcache-info-provider | head -20
    #
    #  LDIF generated by Xylophone v0.2
    #
    #  XSLT processing using SAXON 6.5.5 from Michael Kay 1 (http://saxon.sf.ne
     t/)
    #   at: 2011-05-11T14:08:45+02:00
    #

    dn: GlueSEUniqueID=EXAMPLE-FQDN,mds-vo-name=resource,o=grid
    objectClass: GlueSETop
    objectClass: GlueSE
    objectClass: GlueKey
    objectClass: GlueSchemaVersion
    GlueSEStatus: Production
    GlueSEUniqueID: EXAMPLE-FQDN
    GlueSEImplementationName: dCache
    GlueSEArchitecture: multidisk
    GlueSEImplementationVersion: dCache-PATCH-VERSION (ns=Chimera)
    GlueSESizeTotal: 86

The actual values you see will be site-specific and depend on the contents of the **info-provider.xml** file and your dCache configuration.

To verify that there are no problems, redirect standard-out to **/dev/null** to show only the error messages:

    [root] # dcache-info-provider >/dev/null

If you see error messages (which may be repeated several times) of the form:

    [root] # dcache-info-provider >/dev/null
    Recoverable error
    Failure reading http://localhost:2288/info: no more input

then it is likely that either the `httpd` or `info` service has not been started. Use the above `wget` test to check that both services are running. You can also see which services are available by running the `dcache services` and `dcache status` commands.

DECOMMISSIONING THE OLD INFO PROVIDER
=====================================

Sites that were using the old (pre-1.9.5) info provider should ensure that there are no remnants of this old info-provider on their machine. Although the old info-provider has been removed from dCache, it relied on static LDIF files, which might still exist. If so, then BDII will obtain some information from the current info-provider and some out-of-date information from the static LDIF files. BDII will then attempt to merge the two sources of information. The merged information may provide a confusing description of your dCache instance, which may prevent clients from working correctly.

The old info provider had two static LDIF files and a symbolic link for BDII. These are:

-   The file `lcg-info-static-SE.ldif`,

-   The file: `lcg-info-static-dSE.ldif`,

-   The symbolic link `/opt/glite/etc/gip/plugin`, which points to `/opt/d-cache/jobs/infoDynamicSE-plugin-dcache`.

The two files (**lcg-info-static-SE.ldif** and **lcg-info-static-dSE.ldif**) appear in the **/opt/lcg/var/gip/ldif** directory; however, it is possible to alter the location BDII will use. In BDII v4, the directory is controlled by the `static_dir` variable (see **/opt/glite/etc/gip/glite-info-generic.conf** or **/opt/lcg/etc/lcg-info-generic.conf**). For BDII v5, the `BDII_LDIF_DIR` variable (defined in `/opt/bdii/etc/bdii.conf`) controls this behaviour.

You must delete the above three entries: **lcg-info-static-SE.ldif**, **lcg-info-static-dSE.ldif** and the **plugin** symbolic link.

The directory with the static LDIF, **/opt/lcg/var/gip/ldif** or **/opt/glite/etc/gip/ldif** by default, may contain other static LDIF entries that are relics of previous info-providers. These may have filenames like **static-file-SE.ldif**.

Delete any static LDIF file that contain information about dCache. With the info-provider, all LDIF information comes from the info-provider; there should be no static LDIF files. Be careful not to delete any static LDIF files that come as part of BDII; for example, the **default.ldif**file, if present.

PUBLISHING dCache INFORMATION
=============================

BDII obtains information by querying different sources. One such source of information is by running an info-provider command and taking the resulting LDIF output. To allow BDII to obtain dCache information, you must allow BDII to run the dCache info-provider. This is achieved by symbolically linking the **dcache-info-provider ** script into the BDII plugins directory:

    root] # ln -s /usr/sbin/dcache-info-provider
    /opt/glite/etc/gip/provider/

If the BDII daemons are running, then you will see the information appear in BDII after a short delay; by default this is (at most) 60 seconds.

You can verify that information is present in BDII by querying BDII using the `ldapsearch` command. Here is an example that queries for GLUE v1.3 objects:

    PROMPT-ROOT ldapsearch -LLL -x -H ldap://EXAMPLE-HOST:2170 -b o=grid \
    '(objectClass=GlueSE)'
    dn: GlueSEUniqueID=EXAMPLE-FQDN,Mds-Vo-name=resource,o=grid
    GlueSEStatus: Production
    objectClass: GlueSETop
    objectClass: GlueSE
    objectClass: GlueKey
    objectClass: GlueSchemaVersion
    GlueSETotalNearlineSize: 0
    GlueSEArchitecture: multidisk
    GlueSchemaVersionMinor: 3
    GlueSEUsedNearlineSize: 0
    GlueChunkKey: GlueSEUniqueID=EXAMPLE-FQDN
    GlueForeignKey: GlueSiteUniqueID=example.org
    GlueSchemaVersionMajor: 1
    GlueSEImplementationName: dCache
    GlueSEUniqueID: EXAMPLE-FQDN
    GlueSEImplementationVersion: dCache-VERSION-3 (ns=Chimera)
    GlueSESizeFree: 84
    GlueSEUsedOnlineSize: 2
    GlueSETotalOnlineSize: 86
    GlueSESizeTotal: 86

> **CAREFUL WITH THE HOSTNAME**
>
> You must replace EXAMPLE-HOST in the URI <ldap://EXAMPLE-HOST:2170/> with the actual hostname of your node.
>
> It's tempting to use HOST-LOCALHOST in the URI when executing the `ldapsearch` command; however, BDII binds to the ethernet device (e.g., eth0). Typically, HOST-LOCALHOST is associated with the loopback device (lo), so querying BDII with the URI <ldap://localhost:2170/> will fail.

The LDAP query uses the `o=grid` object as the base; all reported objects are descendant objects of this base object. The `o=grid` base selects only the GLUE v1.3 objects. To see GLUE v2.0 objects, the base object must be `o=glue`.

The above `ldapsearch` command queries BDII using the `(objectClass=GlueSE)` filter. This filter selects only objects that provide the highest-level summary information about a storage-element. Since each storage-element has only one such object and this BDII instance only describes a single dCache instance, the command returns only the single LDAP object.

To see all GLUE v1.3 objects in BDII, repeat the above `ldapsearch` command but omit the `(objectClass=GlueSE)` filter: **ldapsearch -LLL -x -H ldap://EXAMPLE-HOST:2170 -b o=grid**. This command will output all GLUE v1.3 LDAP objects, which includes all the GLUE v1.3 objects from the info-provider.

Searching for all GLUE v2.0 objects in BDII is achieved by repeating the above `ldapsearch` command but omitting the `(objectClass=GlueSE)` filter and changing the search base to `o=glue`: **ldapsearch -LLL -x -H ldap://EXAMPLE-HOST:2170 -b       o=glue**. This command returns a completely different set of objects from the GLUE v1.3 queries.

You should be able to compare this output with the output from running the info-provider script manually: BDII should contain all the objects that the dCache info-provider is supplying. Unfortunately, the order in which the objects are returned and the order of an object's properties is not guaranteed; therefore a direct comparison of the output isn't possible. However, it is possible to calculate the number of objects in GLUE v1.3 and GLUE v2.0.

First, calculate the number of GLUE v1.3 objects in BDII and compare that to the number of GLUE v1.3 objects that the info-provider supplies.

    [root] # ldapsearch -LLL -x -H ldap://<dcache-host>:2170 -b o=grid \
    '(objectClass=GlueSchemaVersion)' | grep ^dn | wc -l
    10
    PROMPT-ROOT CMD-INFO-PROVIDER | \
    grep -i "objectClass: GlueSchemaVersion" | wc -l
    10

Now calculate the number of GLUE v2.0 objects in BDII describing your dCache instance and compare that to the number provided by the info-provider:

    [root] # ldapsearch -LLL -x -H ldap://<dcache-host>:2170 -b o=glue | perl -p00e 's/\n //g' | \
    grep dn.*GLUE2ServiceID | wc -l
    27
    PROMPT-ROOT CMD-INFO-PROVIDER | perl -p00e 's/\n //g' | \
    grep ^dn.*GLUE2ServiceID | wc -l
    27

If there is a discrepancy in the pair of numbers obtains in the above commands then BDII has rejecting some of the objects. This is likely due to malformed LDAP objects from the info-provider.

Troubleshooting BDII problems
=============================

The BDII log file should explain why objects are not accepted; for example, due to a badly formatted attribute. The default location of the log file is **/var/log/bdii/bdii-update.log**, but the location is configured by the `BDII_LOG_FILE` option in the **/opt/bdii/etc/bdii.conf** file.

The BDII log files may show entries like:

    2011-05-11 04:04:58,711: [WARNING] dn: o=shadow
    2011-05-11 04:04:58,711: [WARNING] ldapadd: Invalid syntax (21)
    2011-05-11 04:04:58,711: [WARNING] additional info: objectclass: value #1 invalid per syntax

This problem comes when BDII is attempting to inject new information. Unfortunately, the information isn't detailed enough for further investigation. To obtain more detailed information from BDII, switch the `BDII_LOG_LEVEL` option in **/opt/bdii/etc/bdii.conf** to `DEBUG`. This will provide more information in the BDII log file.

Logging at `DEBUG` level has another effect; BDII no longer deletes some temporary files. These temporary files are located in the directory controlled by the `BDII_VAR_DIR` option. This is **/var/run/bdii** by default.

There are several temporary files located in the **/var/run/bdii** directory. When BDII decides which objects to add, modify and remove, it creates LDIF instructions inside temporary files **add.ldif**, **modify.ldif** and **delete.ldif** respectively. Any problems in the attempt to add, modify and delete LDAP objects are logged to corresponding error files: errors with **add.ldif** are logged to **add.err`, `modify.ldif` to `modify.err** and so on.

Once information in BDII has stablised, the only new, incoming objects for BDII come from those objects that it was unable to add previously. This means that **add.ldif** will contain these badly formatted objects and **add.err** will contain the corresponding errors.

UPDATING INFORMATION
====================

The information contained within the `info` service may take a short time to achieve a complete overview of dCache's state. For certain gathered information it may take a few minutes before the information stabilises. This delay is intentional and prevents the gathering of information from adversely affecting dCache's performance.

The information presented by the LDAP server is updated periodically by BDII requesting fresh information from the info-provider. The info-provider obtains this information by requesting dCache's current status from CELL-INFO service. By default, BDII will query the info-provider every 60 seconds. This will introduce an additional delay between a change in dCache's state and that information propagating.

Some information is hard-coded within the **info-provider.xml** file; that is, you will need to edit this file before the published value(s) will change. These values are ones that typically a site-admin must choose independently of dCache's current operations.

  [???]: #in-install-layout
