Storage Descriptor / SRR
========================

The Storage Descriptor format, also known as a Storage Resource
Reporting (SRR) record, is a JSON object that describes a storage
system.  This includes information about which protocol endpoints are
available and about storage accounting information.

The WLCG Storage Space accounting project has a goal of enabling the
high level overview of the total and available space provided by the
WLCG infrastructure. dCache support WLCG Storage Resource Reporting,
which is for service discovery and reporting capacity usage.

The storage-descriptor/SRR JSON object is stored as a regular file
within dCache.  This allows clients to download the information using
any of the supported transfer protocols.  Through the file's
permissions, it is also possible to control who is able to obtain this
information.

## How the file is generated

dCache is supplied with a script called `dcache-storage-descriptor`.
Running this script will combine static information (from dCache
configuration) and dynamic information (from the info service) to
generate a file containing the Storage Descriptor JSON Object.  This
file is stored on the local filesystem, as
`/var/spool/dcache/storage-descriptor.json` by default.

Once the file is written, it must be imported into dCache.  This could
be achieved by NFS-mounting dCache and configuring the script to write
into dCache directly.  Alternatively, it may be uploaded using any of
the supported protocols (HTTP, FTP, dcap, xrootd) with any of the
supported authentication schemes (username+password, X.509, Kerberos,
OIDC, macaroons).  Note that the `dcache-storage-descriptor` script
does not upload the file itself (unless it writes into a mounted
dCache).

To maintain the liveliness of the information, it is recommended to
run the `dcache-storage-descriptor` script periodically using a cron
job.  This cron job could also upload the file into dCache.

## Setting up dCache for Storage Descriptor

The following steps are needed to enable Storage Descriptor.

### 1. Ensure services are running

There are two dCache services on which the `dcache-storage-descriptor`
script relies: `info` and `httpd`.

The `info` service collects information from other parts of dCache and
caches the results.

The `httpd` service provides an HTTP endpoint for admin and script
access.  The `dcache-storage-descriptor` script uses the `httpd`
service to obtain the dynamic information it needs from the `info`
service.

These services may already be running; however, if not, dCache layout
files need to be updated so they are running.  There are no
requirements on the domains within which these services run, nor on
the hosts on which these domains run.

### 2. Review the storage-descriptor properties.

The `dcache-storage-descriptor` script uses dCache's standard
configuration system to adjust how it collects information and to
provide some static details.

The script is run independently from dCache: it is not a service and
is not run within a domain.  Therefore, configuration for the script
must not appear in any of the domain- or service-specific sections of
the layout file.  Instead, the script's behaviour may be adjusted by
adding configuration to the top of the layout file, or in the
`/etc/dcache/dcache.conf` file.

Some of the properties default values are placeholders and must be
modified.  In particular, the `storage-descriptor.unique-id` must be
configured with the unique identifier for this dCache instance.  The
default value (`dcache.example.org`) is not appropriate.

Some properties have default values that may be correct, but should be
reviewed.  The `storage-descriptor.http.host` property is an example.
This describes the host name of the machine running the `httpd`
service.  The default (`localhost`) is good if the `httpd` service is
running on the same machine as the script.

In general, you should look at all the configuration options, as
listed in the defaults file
`/usr/share/dcache/defaults/storage-descriptor.properties`, and
consider which values should be adjusted.

### 3. Configure doors

The Storage Descriptor format includes information about which
endpoints are available in your dCache instance.  The loginbroker
tags, published by doors, controls whether or not a door is include in
the output.

The `storage-descriptor.door.tag` property controls the tag name used
by the script to select doors for publishing.  The default value is
`storage-descriptor`, so (by default) any door publishing this tag is
described by Storage Descriptor output.

By default, all doors include the `storage-descriptor` tag, and
consequently are published in the Storage Descriptor output.  To
suppress publishing a door, configure the door's loginbroker tags so
they exclude this `storage-descriptor` tag.  This may be done within
the dCache configuration or dynamically through the admin interface.
The former is persistent when restarting dCache while the latter does
not require restarting the door's domain.

#### Using dCache configuration

Each door has its own property for controlling which tags are
published; for example, WebDAV doors use the `webdav.loginbroker.tags`
configuration property and FTP doors use the `ftp.loginbroker.tags`
configuration property.  By default, all doors inherit tags from a
common default set of tags: `dcache.loginbroker.tags`, which (by
default) contains the `storage-descriptor` tag.

To prevent all doors from being published, update the
`dcache.loginbroker.tags` configuration property, removing the
`storage-descriptor` tag.

To prevent publishing doors of a particular type, remove the
`storage-descriptor` from `dcache.loginbroker.tags` and add it to the
door-specific configuration (e.g., `webdav.loginbroker.tags` for
WebDAV doors) for all protocols that should be published.
Alternatively, you can update the door-specific configuration for all
protocols that should not be published, copying the desired tags from
`dcache.loginbroker.tags`.

To prevent a specific door from being published, update that door's
definition (in the layout file) to configure the door-specific
property; e.g.,

```ini
[dCacheDomain/webdav]
webdav.cell.name=WebDAV-S-${host.name}
webdav.net.port=2881
webdav.authz.anonymous-operations=READONLY
webdav.authn.protocol=https
webdav.redirect.on-read=false
webdav.redirect.on-write=false
webdav.loginbroker.tags = dcache-view
```

Note that any changes to configuration properties requires the
corresponding domains to be restarted before they have an effect.

#### Using the admin interface.

Connect to the door using the `\c` command:

```
[celebrimbor] (local) admin > \c WebDAV-celebrimbor
```

The prompt will change to indicate that you are now connected to that
specific door.

The `info` command will show the current loginbroker tags:

```
[celebrimbor] (WebDAV-celebrimbor@dCacheDomain) admin > info
--- cache-login-strategy (Processes mapping requests) ---
gPlazma login cache: CacheStats{hitCount=0, missCount=0, loadSuccessCount=0, loadExceptionCount=0, totalLoadTime=0, evictionCount=0}
gPlazma map cache: CacheStats{hitCount=0, missCount=0, loadSuccessCount=0, loadExceptionCount=0, totalLoadTime=0, evictionCount=0}
gPlazma reverse map cache: CacheStats{hitCount=0, missCount=0, loadSuccessCount=0, loadExceptionCount=0, totalLoadTime=0, evictionCount=0}

--- lb (Registers the door with a LoginBroker) ---
    LoginBroker      : LoginBrokerTopic@local
    Protocol Family  : http
    Protocol Version : 1.1
    Port             : 2880
    Addresses        : [localhost/127.0.0.1]
    Tags             : [cdmi, dcache-view, glue, srm, storage-descriptor]
    Root             : /
    Read paths       : [/]
    Write paths      : [/]
    Update Time      : 5 SECONDS
    Update Threshold : 10 %
    Last event       : UPDATE_SENT

--- path-mapper (Mapping between request paths and dCache paths with OwnCloud Sync client-specific path trimming.) ---
Root path : /

--- pool-monitor (Maintains runtime information about all pools) ---
last refreshed = 2019-11-22 10:38:34.824 (20 seconds ago)
refresh count = 6
active refresh target = [>SpaceManager@local]

--- resource-factory (Exposes dCache resources to Milton WebDAV library) ---
Allowed paths: /
IO queue     : 
```

In the above example, the door is publishing five tags: `cdmi`,
`dcache-view`, `glue`, `srm` and `storage-descriptor`.  To remove this
door from storage-descriptor output, use the `lb set tags` command:

```
[celebrimbor] (WebDAV-celebrimbor@dCacheDomain) admin > lb set tags cdmi dcache-view glue srm
```

The info command will now show the `storage-descriptor` tag is no
longer listed:

```
[celebrimbor] (WebDAV-celebrimbor@dCacheDomain) admin > info
--- cache-login-strategy (Processes mapping requests) ---
gPlazma login cache: CacheStats{hitCount=0, missCount=0, loadSuccessCount=0, loadExceptionCount=0, totalLoadTime=0, evictionCount=0}
gPlazma map cache: CacheStats{hitCount=0, missCount=0, loadSuccessCount=0, loadExceptionCount=0, totalLoadTime=0, evictionCount=0}
gPlazma reverse map cache: CacheStats{hitCount=0, missCount=0, loadSuccessCount=0, loadExceptionCount=0, totalLoadTime=0, evictionCount=0}

--- lb (Registers the door with a LoginBroker) ---
    LoginBroker      : LoginBrokerTopic@local
    Protocol Family  : http
    Protocol Version : 1.1
    Port             : 2880
    Addresses        : [localhost/127.0.0.1]
    Tags             : [cdmi, dcache-view, glue, srm]
    Root             : /
    Read paths       : [/]
    Write paths      : [/]
    Update Time      : 5 SECONDS
    Update Threshold : 10 %
    Last event       : UPDATE_SENT

--- path-mapper (Mapping between request paths and dCache paths with OwnCloud Sync client-specific path trimming.) ---
Root path : /

--- pool-monitor (Maintains runtime information about all pools) ---
last refreshed = 2019-11-22 10:41:35.064 (7 seconds ago)
refresh count = 12
active refresh target = [>SpaceManager@local]

--- resource-factory (Exposes dCache resources to Milton WebDAV library) ---
Allowed paths: /
IO queue     : 
```

Note that this change is not permanent: restarting the domain will
return the tags to their configured values.

### 4. Configure tape information

If a site has no tape storage to report, this step may be skipped.

To configure tape usage reporting, the configuration property
`storage-descriptor.paths.tape-info` must be modified to point to an
XML file maintained by the site.

The default value for this property is
`/usr/share/dcache/xml/tape-info-empty.xml`, which is a file delivered
with dCache.  This `tape-info-empty.xml` file serves two purposes:
first, it serves as a realistic file for sites without any tape
storage; second, it provides detailed information on how data should
be structured within a "live" document for sites with tape storage.

Sites that should report tape usage information must provide a "live"
document that adheres to this XML file format.  It is the site's
responsibility to acquire the required information and to ensure the
liveliness of this information.

Note: this tape-info file has the same format as for GLUE-based tape
storage accounting.  A common file may be used to satisfy both uses.

### 5. Install dependencies

The `dcache-storage-descriptor` script requires the `xsltproc`
command.  As this is not included as a dependency in the dCache
package, it may need to be installed manually.

In RedHat-derived distributions, this is usually located within the
`libxslt` package, and may be installed using yum:

```console-root
yum -y -d 1 install libxslt
```

In Debian-derived distributions, this is usually located within the
`xsltproc` package, and may be installed using apt:

```console-root
apt install xsltproc
```

### 6. Run the script manually

As a simple test, try running the script manually.  This should be
sufficient to provide a JSON file:

```console-root
dcache-storage-descriptor
|JSON available at /var/spool/dcache/storage-descriptor.json
```

Note: the script does not need to be run as root; however, it must
have sufficient permissions to write the output JSON file.

You can also supply the URL from which the script should take the XML
data as a command-line option; e.g.,

```console-root
dcache-storage-descriptor http://static-data.example.org:8080/info
|JSON available at /var/spool/dcache/storage-descriptor.json
```

The supplied URL may be anything that curl understands; for example,
the XML info data may be stored in a file and the script run against
that saved data.

```console-root
curl -o /tmp/info.xml http://localhost:2288/info
|  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
|                                 Dload  Upload   Total   Spent    Left  Speed
|100  166k  100  166k    0     0  6930k      0 --:--:-- --:--:-- --:--:-- 7242k
dcache-storage-descriptor file:/tmp/info.xml
|JSON available at /var/spool/dcache/storage-descriptor.json
```

Once the Storage Descriptor output has been generated, it may be
inspected.  In particular, look for any `example.org` entries, which
point that further configuration is necessary.

### 7. Configure cron

Configure cron to run a small script that generates the Storage
Descriptor output and uploads this into dCache.

To do this via NFS, mount dCache anywhere; e.g.,

```console-root
mkdir /dcache
mount -o vers=4.1 localhost:/ /dcache
````

Update the configuration, so the script writes the output directly into dCache; e.g.,

```ini
storage-descriptor.output.path = /dcache/storage-descriptor.json.new
```

As a final step, the cron job would rename the file
`/dcache/storage-descriptor.json.new` to
`/dcache/storage-descriptor.json`.  This is done to make the update
atomic: a client reading the dCache file `/storage-descriptor.json`
will either get the complete old version or the complete new version,
but never see partial content.

To upload the file using WebDAV, you can use any standard client.  For
example, the following curl command uploads the file using username +
password authentication.

```console-root
curl -u fakeUser:fakePassword -T /var/spool/dcache/storage-descriptor.json http://dcache-webdav-door.example.org:2880/specific-path/
```

Different authentication schemes (like X.509, username+password,
Kerberos, Macaroon, trusted-host, OIDC, ...) are supported in
dCache. For details, please see the [authentication
section](https://www.dcache.org/manuals/UserGuide-##SERIES##/webdav.shtml#authentication)
of dCache User Guide's WebDAV chapter.

**Please note that the path to the script and the output of the result
  depend on the package you are running and your configuration.**

### 8. Ensure file is readable through HTTP

The Storage Descriptor file may be stored anywhere in dCache
namespace; however, WLCG currently require that the file be readable
via HTTP and the URL recorded within CRIC.

Currently, WLCG will read the file using an X.509 robot credential
with VOMS asserting WLCG experiment VO membership.  File permissions
and ownership should be chosen to allow read access for such clients.

## Configurable properties of dCache Storage Descriptor

Below is table that comprises list of configurable storage's
properties and their definitions. _Note that any value that with
`${...}` indicate that the value depends on either dcache properties
or the package._


| Properties | Definition | Default value | Possible values |
| --- | --- | --- | --- |
| storage-descriptor.name | The human-readable name that describes this dCache instance. | | |
| storage-descriptor.unique-id | A unique identifier for your dCache instance. | dcache.example.org | |
| storage-descriptor.quality-level | The "quality" of the dCache instance. | production | `development` or `testing` or `pre-production` or `production` |
| storage-descriptor.http.host | Configuration options on where to fetch dynamic information. The name of the machine that is running the dCache web server. This is used to build the URI for fetching dCache's current state. | `localhost` | |
| storage-descriptor.http.port | The TCP port the dCache web server is running on. This is used to build the URI for fetching dCache's current state. | 2288 | |
| storage-descriptor.paths.tape-info | Nearline accounting. The location of the nearline storage XML file. Sites with nearline storage should modify this value to point to a file that they maintain. Sites without nearline storage should leave this value alone. | `${dcache.paths.share}`/xml/tape-info-empty.xml | |
| storage-descriptor.door.tag | Login-provider tag. The tag that doors identify themselves with before they are published. | storage-descriptor | |
| storage-descriptor.output.path | Output path. The location where the JSON output is written. | `/var/spool/dcache/storage-descriptor.json` or `${dcache.home}/var/spool/dcache/storage-descriptor.json` | |
| storage-descriptor.xslt.path | XSLT path. The location of the XSLT stylesheet that transforms the info service's XML into the Storage Descriptor JSON format. | `${dcache.paths.share}`/xml/xslt/storage-descriptor.xsl | |
