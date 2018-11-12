Chapter 11. dCache as xRootd-Server
===================================

Table of Contents
-----------------

* [Setting up](#setting-up)  

     [Parameters](#parameters)  

* [Quick tests](#quick-tests)  

    [Copying files with xrdcp](#copying-files-with-xrdcp)  
    [Accessing files from within ROOT](#accessing-files-from-within-root)  

* [xrootd security](#xrootd-security)  

    [Read-Write access](#read-write-access)  
    [Permitting read/write access on selected directories](#permitting-read/write-access-on-selected-directories)  
    [Token-based authorization](#token-based-authorization)  
    [Strong authentication](#strong-authentication)  
    [Precedence of security mechanisms](#precedence-of-security-mechanisms)  
    [Other configuration options](#other-configuration-options)  
    
* [xrootd third-party transfer](#xrootd-third-party-transfer)

    [Changes to dCache configuration for authenticated (GSI) transfers](#changes-to-dcache-configuration-for-authenticated-gsi-transfers) 
    
    [Signed Hash Verification Support](#signed-hash-verification-support)

This chapter explains how to configure dCache in order to access it via the `xrootd` protocol, allowing `xrootd`-Clients like ROOT’s TXNetfile and xrdcp to do file operations against a dCache instance in a transparent manner. dCache implements version 2.1.6 of `xrootd` protocol.

Setting up
==========

To allow file transfers in and out of dCache using xrootd, a new `xrootd door` must be started. This door acts then as the entry point to all xrootd requests. Compared to the native xrootd server-implementation (produced by SLAC), the `xrootd door` corresponds to the `redirector node`.

To enable the `xrootd door`, you have to change the layout file corresponding to your dCache-instance. Enable the xrootd-service within the domain that you want to run it by adding the following line 
   
       ..
       [<domainName>/xrootd]
       ..

Example:

You can just add the following lines to the layout file:

    ..
    [xrootd-${host.name}Domain]
    [xrootd-${host.name}Domain/xrootd]
    ..

After a restart of the domain running the DOOR-XROOTD, done e.g. by executing

    [root] # ${dCacheHome}/bin/dcache restart xrootd-babelfishDomain
    Stopping xrootd-babelfishDomain (pid=30246) 0 1 2 3 4 5 6 7 done
    Starting xrootd-babelfishDomain done

the xrootd door should be running. A few minutes later it should appear at the web monitoring interface under "Cell Services" (see [the section called “The Web Interface for Monitoring dCache”).](intouch.md#the-web-interface-for-monitoring-dcache)

Parameters
----------

The default port the `xrootd door` is listening on is 1094. This can be changed two ways: 

1.  *Per door*: Edit your instance’s layout file, for example **/etc/dcache/layouts/example.conf** and add the desired port for the xrootd door in a separate line (a restart of the domain(s) running the xrootd door is required): 
        ..
        [xrootd-${host.name}Domain]
        [xrootd-${host.name}Domain/xrootd]
            port = 1095
        ..

2.  *Globally:* Edit **/etc/dcache/dcache.conf ** and add the variable `xrootd.net.port` with the desired value (a restart of the domain(s) running the `xroot door` is required):

        ..
        xrootd.net.port=1095
        ..

 For controlling the `TCP`-portrange within which `xrootd`-movers will start listening in the <pool>Domain, you can add the properties `dcache.net.lan.port.min` and dcache.net.lan.port.max to **/etc/dcache/dcache.conf** and adapt them according to your preferences. The default values can be viewed in **/usr/share/dcache/defaults/dcache.properties**.

    ..
    dcache.net.lan.port.min=30100  
    dcache.net.lan.port.max=30200  
    ..

QUICK TESTS
===========

The subsequent paragraphs describe a quick guide on how to test `xrootd` using the **xrdcp** and **ROOT** clients.

Copying files with xrdcp
------------------------

A simple way to get files in and out of dCache via `xrootd` is the command xrdcp. It is included in every xrootd and ROOT distribution.

To transfer a single file in and out of dCache, just issue   

    [user] $ xrdcp /bin/sh root://<xrootd-door.example.org>/pnfs/<example.org>/data/xrd_test  
    [user] $ xrdcp root://<xrootd-door.example.org>/pnfs/<example.org>/data/xrd_test /dev/null  

Accessing files from within ROOT
--------------------------------

This simple ROOT example shows how to write a randomly filled histogram to a file in dCache:   

    root [0] TH1F h("testhisto", "test", 100, -4, 4);  
    root [1] h->FillRandom("gaus", 10000);  
    root [2] TFile *f = new TXNetFile("root://<door_hostname>//pnfs/<example.org>/data/test.root","new");  
    061024 12:03:52 001 Xrd: Create: (C) 2004 SLAC INFN XrdClient 0.3  
    root [3] h->Write();  
    root [4] f->Write();  
    root [5] f->Close();  
    root [6] 061101 15:57:42 14991 Xrd: XrdClientSock::RecvRaw: Error reading from socket: Success  
    061101 15:57:42 14991 Xrd: XrdClientMessage::ReadRaw: Error reading header (8 bytes)  

 Closing remote `xrootd` files that live in dCache produces this warning, but has absolutely no effect on subsequent ROOT commands. It happens because dCache closes all `TCP` connections after finishing a file transfer, while xrootd expects to keep them open for later reuse.

To read it back into ROOT from dCache:   

   	root [7] TFile *reopen = TXNetFile ("root://<door_hostname>//pnfs/<example.org>/data/test.root","read");   
   	root [8] reopen->ls();   
   	TXNetFile**             //pnfs/<example.org>/data/test.root   
   	TXNetFile*             //pnfs/<example.org>/data/test.root   
  	 KEY: TH1F     testhisto;1     test   

XROOTD security
===============

Read-Write access
-----------------

 Per default dCache xrootd is restricted to read-only, because plain xrootd is completely unauthenticated. A typical error message on the clientside if the server is read-only looks like:  

    [user] $ xrdcp -d 1 /bin/sh root://ford.desy.de//pnfs/desy.de/data/xrd_test2  
    Setting debug level 1   
    061024 18:43:05 001 Xrd: main: (C) 2004 SLAC INFN xrdcp 0.2 beta   
    061024 18:43:05 001 Xrd: Create: (C) 2004 SLAC INFN XrdClient kXR_ver002+kXR_asyncap   
    061024 18:43:05 001 Xrd: ShowUrls: The converted URLs count is 1   
    061024 18:43:05 001 Xrd: ShowUrls: URL n.1: root://ford.desy.de:1094//pnfs/desy.de/data/asdfas.   
    061024 18:43:05 001 Xrd: Open: Access to server granted.   
    061024 18:43:05 001 Xrd: Open: Opening the remote file /pnfs/desy.de/data/asdfas   
    061024 18:43:05 001 Xrd: XrdClient::TryOpen: doitparallel=1  
    061024 18:43:05 001 Xrd: Open: File open in progress.  
    061024 18:43:06 5819 Xrd: SendGenCommand: Server declared: Permission denied. Access is read only.(error code: 3003)  
    061024 18:43:06 001 Xrd: Close: File not opened.  
    Error accessing path/file for root://ford//pnfs/desy.de/data/asdfas  

To enable read-write access, add the following line to **${dCacheHome}/etc/dcache.conf**  

    ..
    xrootdIsReadOnly=false
    ..

and restart any domain(s) running a `xrootd door`.

Please note that due to the unauthenticated nature of this access mode, files can be written and read to/from any subdirectory in the pnfs namespace (including the automatic creation of parent directories). If there is no user information at the time of request, new files/subdirectories generated through `xrootd` will inherit UID/GID from its parent directory. The user used for this can be configured via the `xrootd.authz.user` property. 

Permitting read/write access on selected directories
----------------------------------------------------

 To overcome the security issue of uncontrolled `xrootd` read and write access mentioned in the previous section, it is possible to restrict read and write access on a per-directory basis (including subdirectories).

To activate this feature, a colon-seperated list containing the full paths of authorized directories must be added to **/etc/dcache/dcache.conf.** You will need to specify the read and write permissions separately. 

   	 ..
	 xrootd.authz.read-paths=/pnfs/<example.org>/rpath1:/pnfs/<example.org>/rpath2
 	 xrootd.authz.write-paths=/pnfs/<example.org>/wpath1:/pnfs/<example.org>/wpath2
  	  ..

A restart of the `xrootd` door is required to make the changes take effect. As soon as any of the above properties are set, all read or write requests to directories not matching the allowed path lists will be refused. Symlinks are however not restricted to these prefixes. 

Token-based authorization
-------------------------

The `xrootd` dCache implementation includes a generic mechanism to plug in different authorization handlers. The only plugin available so far implements token-based authorization as suggested in [http://people.web.psi.ch/feichtinger/doc/authz.pdf](https://www.psi.ch/search/phonebook-and-e-mail-directory?q=feichtinger).

The first thing to do is to setup the keystore. The keystore file basically specifies all RSA-keypairs used within the authorization process and has exactly the same syntax as in the native xrootd tokenauthorization implementation. In this file, each line beginning with the keyword KEY corresponds to a certain Virtual Organisation (VO) and specifies the remote public (owned by the file catalogue) and the local private key belonging to that VO. A line containing the statement `"KEY VO:*"` defines a default keypair that is used as a fallback solution if no VO is specified in token-enhanced `xrootd` requests. Lines not starting with the KEY keyword are ignored. A template can be found in **/usr/share/dcache/examples/xrootd/keystore.**

The keys itself have to be converted into a certain format in order to be loaded into the authorization plugin. dCache expects both keys to be binary DER-encoded (Distinguished Encoding Rules for ASN.1). Furthermore the private key must be PKCS #8-compliant and the public key must follow the X.509-standard.

The following example demonstrates how to create and convert a keypair using OpenSSL:   

    Generate new RSA private key 
    [root] # openssl genrsa -rand 12938467 -out key.pem 1024

    Create certificate request
    [root] # openssl req -new -inform PEM -key key.pem -outform PEM -out certreq.pem

    Create certificate by self-signing certificate request
    [root] # openssl x509 -days 3650 -signkey key.pem -in certreq.pem -req -out cert.pem

    Extract public key from certificate
    [root] # openssl x509 -pubkey -in cert.pem -out pkey.pem
    [root] # openssl pkcs8 -in key.pem -topk8 -nocrypt -outform DER -out <new_private_key>
    [root] # openssl enc -base64 -d -in pkey.pem -out <new_public_key>

Only the last two lines are performing the actual conversion, therefore you can skip the previous lines in case you already have a keypair. Make sure that your keystore file correctly points to the converted keys.

To enable the plugin, it is necessary to add the following two lines to the file **/etc/dcache/dcache.conf**, so that it looks like 

        ..
        xrootdAuthzPlugin=org.dcache.xrootd.security.plugins.tokenauthz.TokenAuthorizationFactory
	xrootdAuthzKeystore=<Path_to_your_Keystore>
	..

After doing a restart of dCache, any requests without an appropriate token should result in an error saying "authorization check failed: No authorization token found in open request, access denied.(error code: 3010)".

If both tokenbased authorization and read-only access are activated, the read-only restriction will dominate (local settings have precedence over remote file catalogue permissions). 

Strong authentication
---------------------

The `xrootd`-implementation in dCache includes a pluggable authentication framework. To control which authentication mechanism is used by `xrootd`, add the `xrootdAuthNPlugin` option to your dCache configuration and set it to the desired value. 

Example:

For instance, to enable `GSI` authentication in `xrootd`, add the following line to **/etc/dcache/dcache.conf: **

    ..
    xrootdAuthNPlugin=gsi
    ..

When using `GSI` authentication, depending on your setup, you may or may not want dCache to fail if the host certificate chain can not be verified against trusted certificate authorities. Whether dCache performs this check can be controlled by setting the option `dcache.authn.hostcert.verify`:

    ..
    dcache.authn.hostcert.verify=true
    ..

*Authorization* of the user information obtained by strong authentication is performed by contacting the gPlazma service. Please refer to [Chapter 10, Authorization in dCache](config-gplazma.md) for instructions about how to configure gPlazma. 

> **SECURITY CONSIDERATION**
>
> In general `GSI` on `xrootd` is not secure. It does not provide confidentiality and integrity guarantees and hence does not protect against man-in-the-middle attacks. 

Precedence of security mechanisms
---------------------------------

The previously explained methods to restrict access via `xrootd` can also be used together. The precedence applied in that case is as following:

> **NOTE**
>
> The `xrootd-door` can be configured to use either token authorization or strong authentication with `gPlazma` authorization. A combination of both is currently not possible. 

 The permission check executed by the authorization plugin (if one is installed) is given the lowest priority, because it can controlled by a remote party. E.g. in the case of token based authorization, access control is determined by the file catalogue (global namespace).

The same argument holds for many strong authentication mechanisms - for example, both the `GSI` protocol as well as the `Kerberos` protocols require trust in remote authorities. However, this only affects user *authentication*, while authorization decisions can be adjusted by local site administrators by adapting the `gPlazma` configuration.

To allow local site’s administrators to override remote security settings, write access can be further restricted to few directories (based on the local namespace, the `pnfs`). Setting `xrootd access to read-only has the highest priority, overriding all other settings.

Other configuration options
---------------------------

The `xrootd-door` has several other configuration properties. You can configure various timeout parameters, the thread pool sizes on pools, queue buffer sizes on pools, the `xrootd` root path, the xrootd user and the `xrootd` IO queue. Full descriptions on the effect of those can be found in **/usr/share/dcache/defaults/xrootd.properties.**


XROOTD Third-party Transfer
===========================

Starting with dCache 4.2, native third-party transfers between dCache and another 
xrootd server (including another dCache door) are possible.  These can be done 
either in unauthenticated mode, or with GSI (X509) authentication, 
using the client provided by SLAC (xrdcp or xrdcopy).

To enforce third-party copy, one must execute the transfer using

                xrdcp --tpc only <source> <destination>

One can also try third party and fail over to one-hop two-party (through the client) by using

                xrdcp --tpc first <source> <destination>
                
### TPC from dCache to another xrootd server

Very few changes in the dCache door were needed to accomplish this. 
If dCache is merely to serve as file source, then all that is needed 
is to update to version 4.2+ on the nodes running the xrootd doors.

### TPC from another xrootd server to dCache, or between dCache instances 

As per the protocol, the destination pulls/reads the file from the source and 
writes it locally to a selected pool. This is achieved by an embedded third-party 
client which runs on the pool. Hence, using dCache as destination means the pools 
must also be running dCache 4.2+.

Pools without the additional functionality provided by 4.2+ will not be able 
to act as destination in a third-party transfer and a "tpc not supported" error 
will be reported if `--tpc only` is specified. 
                

Changes to dCache configuration for authenticated (GSI) transfers
-----------------------------------------------------------------

 For dCache as source, gPlazma configuration is identical to that needed for 
 normal two-party reads and writes, with the caveat that the necessary destination 
 DNs must be mapped on the dCache end. This will depend upon the nature of the 
 proxy credential being used by the source.

To use dCache as TPC destination, some additional steps need to be taken.

First, for all pools that will receive files through xrootd TPC, the GSI service 
provider plugin must be loaded by including this in the configuration or layout: 

               pool.mover.xrootd.tpc-authn-plugins=gsi

Second, until the generally agreed upon solution for proxy delegation is adopted 
and implemented, there are two ways of providing authentication 
capability to these pools. 

* Generate a proxy from a credential that will be recognized by the source, 
and arrange to have it placed (and periodically refreshed) on each pool that 
may be the recipient of files transfered via xrootd TPC. The proxy path must 
be indicated to dCache by setting this property:

              xrootd.gsi.tpc.proxy.path={path-to-proxy}

* If this property is left undefined, dCache will auto-generate a proxy from 
the `hostcert.pem` / `hostkey.pem` of the node on which the pool is running. 
While this solution means no cron job is necessary to keep the proxy up to date, 
it is also rather clunky in that it requires the hostcert DNs of all the pools 
to be mapped on the source server end. 


Signed hash verification support 
---------------------------------------------

 The embedded third-party client will honor signed hash verification if the 
 source server indicates it must be observed.

Starting with dCache 5.0, the dCache door/server will also provide the option 
to enable signed hash verification.

However, there is a caveat here. Since dCache redirects reads from the door 
to a selected pool, and since the subsequent connection to the pool is 
unauthenticated (this has always been the case; the connection fails if the 
opaque id token dCache gives back to the client is missing), the only way to 
get signed hash verification on the destination-to-pool connection is to set 
the kXR_secOFrce flag. This means that the pool will then require unix 
authentication from the destination and that it will expect unencrypted hashes.

While the usefulness of unencrypted signed hash verification is disputable, 
the specification nevertheless provides for it, and this was the only way, 
short of encumbering our pool interactions with yet another GSI handshake, 
to allow for sigver on the dCache end at all, since the main subsequent 
requests (open, read, etc.) are made to the pool, not the door.

dCache 5.0 will provide the following properties to control security level 
and force unencrypted signing: 

            dcache.xrootd.security.level={0-4}
            dcache.xrootd.security.force-signing={true,false} 

In the case that the latter is set to true, and one anticipates there will be 
xrootd TPC transfers between two dCache instances or two dCache doors, one 
also would need to include the unix service provider plugin in all the relevant 
pool configurations:

            pool.mover.xrootd.tpc-authn-plugins=gsi,unix                    

<!--  [???]: #intouch-web
  []: http://people.web.psi.ch/feichtinger/doc/authz.pdf
  [1]: #cf-gplazma
