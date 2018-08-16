Chapter 10. Authorization in dCache 
===================================

Table of Contents

+ [Basics](#basics)    
+ [Configuration](#configuration)  

    [Plug-ins](#plug-ins)   

+ [Using X.509 Certificates](#using-x509-certificates)    

    [CA Certificates](#ca-certificates)  
    [User Certificate](#user-certificates)
    [Host Certificate](#host-certificates)
    [VOMS Proxy Certificate](#voms-proxy-certificate)

+ [Configuration files](#configuration-files)  

    [storage-authzdb](#storage-authzdb)  
    [The gplazmalite-vorole-mapping plug-in](#the-gplazmalite-vorole-mapping-plug-in)  
    [Authorizing a VO](#authorizing-a-vo)  
    [The kpwd plug-in](#the-kpwd-plug-in)  
    [The gridmap plug-in](#the-gridmap-plug-in)    

+ [gPlazma specific dCache configuration](#gplazma-specific-dcache-configuration)  

    [Enabling Username/Password Access for WebDAV](#enabling-username-password-access-for-webdav)    
    [gPlazma config example to work with authenticated webadmin](#gplazma-config-example-to-work-with-authenticated-webadmin)  

To limit access to data, dCache comes with an authentication and authorization interface called `gPlazma2`. gPlazma is an acronym for Grid-aware PLuggable AuthorZation Management. Earlier versions of dCache worked with `gPlazma1` which has now been completely removed from dCache. So if you are upgrading, you have to reconfigure `gPlazma` if you used `gPlazma1` until now. 

Basics
======

Though it is possible to allow anonymous access to dCache it is usually desirable to authenticate users. The user then has to connect to one of the different doors (e.g., `GridFTP door, dCap door`) and login with credentials that prove his identity. In Grid-World these credentials are very often `X.509` certificates, but dCache also supports other methods like username/password and kerberos authentication.

The door collects the credential information from the user and sends a login request to the configured authorization service (i.e., `gPlazma`) Within `gPlazma` the configured plug-ins try to verify the users identity and determine his access rights. From this a response is created that is then sent back to the door and added to the entity representing the user in dCache. This entity is called `subject`. While for authentication usually more global services (e.g., ARGUS) may be used, the mapping to site specific UIDs has to be configured on a per site basis. 

Configuration  
=============

`gPlazma2` is configured by the PAM-style configuration file **/etc/dcache/gplazma.conf**. Each line of the file is either a comment (i.e., starts with #, is empty, or defines a plugin. Plugin defining lines start with the plugin stack type (one of `auth, map, account, session identity`), followed by a PAM-style modifier (one of `optional, sufficient, required, requisite`), the plugin name and an optional list of key-value pairs of parameters. During the login process they will be executed in the order `auth, map, account` and `session`. The `identity` plugins are not used during login, but later on to map from UID+GID back to user names (e.g., for NFS). Within these groups they are used in the order they are specified. 

    auth|map|account|session|identity optional|required|requisite|sufficient plug-in ["key=value" ...]

A complete configuration file will look something like this:

Example:

    # Some comment
    auth    optional  x509
    auth    optional  voms
    map     requisite vorolemap
    map     requisite authzdb authzdb=/etc/grid-security/authzdb
    session requisite authzdb

**auth**  
`auth`-plug-ins are used to read the users public and private credentials and ask some authority, if those are valid for accessing the system. 

**map**  
`map`-plug-ins map the user information obtained in the `auth` step to UID and GIDs. This may also be done in several steps (e.g., the `vorolemap` plug-in maps the users DN+FQAN to a username which is then mapped to UID/GIDs by the `authzdb` plug-in. 

**account**  
`account`-plug-ins verify the validity of a possibly mapped identity of the user and may reject the login depending on information gathered within the map step. 

**session**  
`session` plug-ins usually enrich the session with additional attributes like the user’s home directory. 

**identity**  
`identity` plug-ins are responsible for mapping UID and GID to user names and vice versa during the work with dCache. 

The meaning of the modifiers follow the PAM specification:

Modifiers

**optional**  
The success or failure of this plug-in is only important if it is the only plug-in in the stack associated with this type.

**sufficient**   
Success of such a plug-in is enough to satisfy the authentication requirements of the stack of plug-ins (if a prior required plug-in has failed the success of this one is ignored). A failure of this plug-in is not deemed as fatal for the login attempt. If the plug-in succeeds `gPlazma2` immediately proceeds with the next plug-in type or returns control to the door if this was the last stack. 

**required**   
Failure of such a plug-in will ultimately lead to `gPlazma2` returning failure but only after the remaining plug-ins for this type have been invoked. 

**requisite**   
Like `required`, however, in the case that such a plug-in returns a failure, control is directly returned to the door. 

Plug-ins
--------

`gPlazma2` functionality is configured by combining different types of plug-ins to work together in a way that matches your requirements. For this purpose there are five different types of plug-ins. These types correspond to the keywords `auth, map, account, session` and `identity` as described in the previous section. The plug-ins can be configured via properties that may be set in **dcache.conf**, the layout-file or in **gplazma.conf**. 

### auth Plug-ins

#### kpwd

The `kpwd` plug-in authorizes users by username and password, by pairs of DN and FQAN and by `Kerberos` principals.


Properties 

**gplazma.kpwd.file** 

Path to   **dcache.kpwd**  
Default:  **/etc/dcache/dcache.kpwd**


#### voms

The `voms` plug-in is an `auth` plug-in. It can be used to verify `X.509` credentials. It takes the certificates and checks their validity by testing them against the trusted CAs. The verified certificates are then stored and passed on to the other plug-ins in the stack. 



Properties

**gplazma.vomsdir.ca**

   Path to ca certificates
   Default: **/etc/grid-security/certificates**



**gplazma.vomsdir.dir**

  Path to **vomsdir**
  Default: **/etc/grid-security/vomsdir**

#### X.509 plug-in

The X.509 is a auth plug-in that extracts X.509 certificate chains from the credentials of a user to be used by other plug-ins. 

### map Plug-ins

#### kpwd

As a `map` plug-in it maps usernames to UID and GID. And as a `session` plug-in it adds root and home path information to the session based on the user’s username. 

Properties

  **gplazma.kpwd.file**  

     Path to **dcache.kpwd**  
     Default: **/etc/dcache/dcache.kpwd**  



#### authzdb  

The GP2-AUTHZDB takes a username and maps it to UID+GID using the `storage-authzdb` file.



**gplazma.authzdb.file**   

   Path to **storage-authzdb**     
   Default: **/etc/grid-security/storage-authzdb**  



#### GridMap

The `authzdb` plug-in takes a username and maps it to UID+GID using the **storage-authzdb** file. 



Properties

**gplazma.gridmap.file**  

   Path to `grid-mapfile`  
   Default: **/etc/grid-security/grid-mapfile**



#### vorolemap

The `voms` plug-in maps pairs of DN and FQAN to usernames via a [vorolemap](config-gplazma.md#preparing-grid-vorolemap) file. 



Properties

**gplazma.vorolemap.file**

   Path to **grid-vorolemap**  
   **/etc/grid-security/grid-vorolemap**



#### krb5

 The `krb5` plug-in maps a kerberos principal to a username by removing the domain part from the principal.

Example:

                    user@KRB-DOMAIN.EXAMPLE.ORG to user
                  

#### nsswitch

The `nsswitch` plug-in uses the system’s `nsswitch` configuration to provide mapping.

Typically `nsswitch` plug-in will be combined with `vorolemap` plug-in, `gridmap` plug-in or `krb5` plug-in: 

Example:

    # Map grid users to local accounts
    auth    optional  x509 #1
    auth    optional  voms #2
    map     requisite vorolemap #3
    map     requisite nsswitch #4
    session requisite nsswitch #5

In this example following is happening: extract user's DN (1), extract and verify VOMS attributes (2), map DN+Role to a local account (3), extract uid and gids for a local account (4) and, finally, extract users home directory (5).

#### nis

The `nis` uses an existing `NIS` service to map username+password to a username.

Properties

**gplazma.nis.server**  

 `NIS` server host  
 Default: `nisserv.domain.com`
 
 
**gplazma.nis.domain**

`NIS` domain  
Default: `domain.com`

The result of `nis` can be used by other plug-ins:

Example:

    # Map grid or kerberos users to local accounts
    auth    optional  x509 #1
    auth    optional  voms #2
    map     requisite vorolemap #3
    map     optional  krb5 #4
    map     optional  nis #5
    session requisite nis #6

In this example two access methods are considered: grid based and kerberos based. If user comes with grid certificate and VOMS role: extract user’s DN (1), extract and verify VOMS attributes (2), map DN+Role to a local account (3). If user comes with `Kerberos` ticket: extract local account (4). After this point in both cases we talk to `NIS` to get uid and gids for a local account (5) and, finally, adding users home directory (6).



### account Plug-ins

#### argus

 The argus plug-in bans users by their DN. It talks to your site’s ARGUS system (see [https://twiki.cern.ch/twiki/bin/view/EGEE/AuthorizationFramework](https://twiki.cern.ch/twiki/bin/view/EGEE/AuthorizationFramework)) to check for banned users.

Properties

**gplazma.argus.hostcert**  

   Path to host certificate  
   Default: **/etc/grid-security/hostcert.pem**



**gplazma.argus.hostkey**

   Path to host key  
   Default:  **/etc/grid-security/hostkey.pem**



**gplazma.argus.hostkey.password**

   Password for host key  
   Default:



**gplazma.argus.ca**

   Path to CA certificates  
   Default:  **/etc/grid-security/certificates**



**gplazma.argus.endpoint**

   URL of PEP service  
   Default: **https://localhost:8154/authz**



#### banfile

The `banfile` plug-in bans users by their principal class and the associated name. It is configured via a simple plain text file. 

Example:

    # Ban users by principal
    alias dn=org.globus.gsi.jaas.GlobusPrincipal
    alias kerberos=javax.security.auth.kerberos.KerberosPrincipal
    alias fqan=org.dcache.auth.FQANPrincipal
    alias name=org.dcache.auth.LoginNamePrincipal

    ban name:ernie
    ban kerberos:BERT@EXAMPLE.COM
    ban com.example.SomePrincipal:Samson

In this example the first line is a comment. Lines 2 to 5 define aliases for principal class names that can then be used in the following banning section. The four aliases defined in this example are actually hard coded into CELL-GPLAZMA, therefore you can use these short names without explicitly defining them in your configuration file. Line 7 to 9 contain ban definitions. Line 9 directly uses the class name of a principal class instead of using an alias.

Please note that the plug-in only supports principals whose assiciated name is a single line of plain text. In programming terms this means the constructor of the principal class has to take exactly one single string parameter.

For the plugin to work, the configuration file has to exist even if it is empty.



Properties

**gplazma.banfile.path**

   Path to configuration file  
   Default: **/etc/dcache/ban.conf**



To activate the `banfile` it has to be added to **gplazma.conf**:

Example:

    # Map grid or kerberos users to local accounts
    auth    optional  x509
    auth    optional  voms
    map     requisite vorolemap
    map     optional  krb5
    map     optional  nis
    session requisite nis
    account requisite banfile

### session Plug-ins



#### kpwd

The `kpwd`plug-in adds root and home path information to the session, based on the username.



Properties

**gplazma.kpwd.file**

   Path to **dcache.kpwd**  
   Default: **/etc/dcache/dcache.kpwd**  



#### authzdb

The `authzdb` plug-in adds root and home path information to the session, based and username using the **storage-authzdb** file. 



Properties

**gplazma.authzdb.file** 

   Path to **storage-authzdb**  
   Default: **/etc/grid-security/storage-authzdb**



#### nsswitch

The `nsswitch` plug-in adds root and home path information to the session, based on the username using your system’s `nsswitch` service.

Typically `nsswitch` plug-in will be combined with `vorolemap` plug-in, `gridmap` plug-in or `krb5` plug-in: 



Example:

    # Map grid users to local accounts
    auth    optional  x509 #1
    auth    optional  voms #2
    map     requisite vorolemap #3
    map     requisite nsswitch #4
    session requisite nsswitch #5

In this example following is happening: extract user's DN (1), extract and verify VOMS attributes (2), map DN+Role to a local account (3), extract uid and gids for a local account (4) and, finally, extract users home directory (5).



#### nis

The `nis` plug-in adds root and home path information to the session, based on the username using your site’s `NIS` service. 



Properties

**gplazma.nis.server**

    `NIS` server host  
    Default: `nisserv.domain.com`
    
    
    
**gplazma.nis.domain**

    `NIS` domain  
    Default: `domain.com`
    
The result of `nis` can be used by other plug-ins:

Example:

    # Map grid or kerberos users to local accounts
    auth    optional  x509 #1
    auth    optional  voms #2
    map     requisite vorolemap #3
    map     optional  krb5 #4
    map     optional  nis #5
    session requisite nis #6

In this example two access methods are considered: grid based and kerberos based. If user comes with grid certificate and VOMS role: extract user's DN (1), extract and verify VOMS attributes (2), map DN+Role to a local account (3). If user comes with `Kerberos` ticket: extract local account (4). After this point in both cases we talk to NIS to get uid and gids for a local account (5) and, finally, adding users home directory (6).



#### ldap

The `ldap` is a map, session and identity plugin. As a map plugin it maps user names to UID and GID. As a session plugin it adds root and home path information to the session. As an identity plugin it supports reverse mapping of UID and GID to user and group names repectively.



Properties  

**gplazma.ldap.url**  

    `LDAP` server url. Use `ldap://` prefix to connect to plain `LDAP` and `ldaps://` for secured `LDAP`.  
    Example: `ldaps://example.org:389`



**gplazma.ldap.organization**  

    Top level (`base DN`) of the `LDAP` directory tree    
    Example: `o="Example, Inc.", c=DE`



**gplazma.ldap.tree.people**  

`LDAP` subtree containing user information. The path to the user records will be formed using the `base
                    DN` and the value of this property as a organizational unit (`ou`) subdirectory.

Default: `People`

Example: Setting `gplazma.ldap.organization=o="Example, Inc.", c=DE` and `gplazma.ldap.tree.people=People` will have the plugin looking in the LDAP directory `ou=People, o="Example, Inc.", c=DE` for user information.


**gplazma.ldap.tree.groups**

`LDAP` subtree containing group information. The path to the group records will be formed using the `base
                    DN` and the value of this property as a organizational unit (`ou`) subdirectory.

Default: `Groups`

Example: Setting `gplazma.ldap.organization=o="Example, Inc.",
                    c=DE` and `gplazma.ldap.tree.groups=Groups` will have the plugin looking in the LDAP directory `ou=Groups, o="Example, Inc.", c=DE` for group information.


**gplazma.ldap.userfilter**

`LDAP` filter expression to find user entries. The filter has to contain the `%s` exactly once. That occurence will be substituted with the user name before the filter is applied.

Default: `(uid=%s)`

**gplazma.ldap.home-dir**  

the user's home directory. `LDAP` attribute identifiers surrounded by `%` will be expanded to their corresponding value. You may also use a literal value or mix literal values and attributes.

Default: `%homeDirectory%`

**gplazma.ldap.root-dir**

the user's root directory. LDAP attribute identifiers surrounded by `%` will be expanded to their corresponding value. You may also use a literal value or mix literal values and attributes.

Default: `/`

As a session plugin the GP2-LDAP assigns two directories to the user's session: the root directory and the home directory. The root directory is the root of the directory hierarchy visible to the user, while the home directory is the directory the user starts his session in. In default mode, the root directory is set to `/` and the home directory is set to `%homeDirectory%`, thus the user starts his session in the home directory, as it is stored on the LDAP server, and is able to go up in the directory hierarchy to `/`. For a different use-case, for example if dCache is used as a cloud storage, it may be desireable for the users to see only their own storage space. For this use case `home-dir` can be set to `/` and `root-dir` be set to `%homeDirectory%`. In both path properties any `%val%` expression will be expanded to the the value of the attribute with the name `val` as it is stored in the user record on the LDAP server.

### identity Plug-ins

#### nsswitch

The `nsswitsch` provides forward and reverse mapping for `NFSv4.1` using your system's `nsswitch` service.

#### nis

The `nis` plug-in forward and reverse mapping for `NFSv4.1` using your site's NIS service.

Properties

**gplazma.nis.server**

   `NIS` server host   
   Default: `nisserv.domain.com`
    
**gplazma.nis.domain**

   `NIS` domain  
    Default: domain.com

Using X509 Certificates  
=======================  

Most plug-ins of `gPlazma` support `X.509` certificates for authentication and authorisation. `X.509` certificates are used to identify entities (e.g., persons, hosts) in the Internet. The certificates contain a DN (Distinguished Name) that uniquely describes the entity. To give the certificate credibility it is issued by a CA (Certificate Authority) which checks the identity upon request of the certificate (e.g., by checking the persons id). For the use of X.509 certificates with dCache your users will have to request a certificate from a CA you trust and you need host certificates for every host of your dCache instance. 


CA Certificates  
---------------  

To be able to locally verify the validity of the certificates, you need to store the CA certificates on your system. Most operating systems come with a number of commercial CA certificates, but for the *Grid* you will need the certificates of the Grid CAs. For this, CERN packages a number of CA certificates. These are deployed by most grid sites. By deploying these certificates, you state that you trust the CA's procedure for the identification of individuals and you agree to act promptly if there are any security issues.

To install the CERN CA certificates follow the following steps:

    [root] # cd /etc/yum.repos.d/
    [root] # wget http://grid-deployment.web.cern.ch/grid-deployment/glite/repos/3.2/lcg-CA.repo
    [root] # yum install lcg-CA

This will create the directory **/etc/grid-security/certificates** which contains the Grid CA certificates.

Certificates which have been revoked are collected in certificate revocation lists (CRLs). To get the CRLs install the **fetch-crl** command as described below.

    [root] # yum install fetch-crl
    [root] # /usr/sbin/fetch-crl

**fetch-crl** adds `X.509` CRLs  to **/etc/grid-security/certificates**. It is recommended to set up a cron job to periodically update the CRLs.

User Certificate
----------------

If you do not have a valid grid user certificate yet, you have to request one from your CA. Follow the instructions from your CA on how to get a certificate. After your request was accepted you will get a URL pointing to your new certificate. Install it into your browser to be able to access grid resources with it. Once you have the certificate in your browser, make a backup and name it **userCertificate.p12**. Copy the user certificate to the directory **~/.globus/** on your worker node and convert it to **usercert.pem** and **userkey.pem** as described below.

   [user] $ openssl pkcs12 -clcerts -nokeys -in <userCertificate>.p12 -out usercert.pem  
   Enter Import Password:  
   MAC verified OK  

During the backup your browser asked you for a password to encrypt the certificate. Enter this password here when asked for a password. This will create your user certificate.  

    [user] $ openssl pkcs12 -nocerts -in <userCertificate>.p12 -out userkey.pem  
    Enter Import Password:  
    MAC verified OK  
    Enter PEM pass phrase:  

In this step you need to again enter the backup password. When asked for the PEM pass phrase choose a secure password. If you want to use your key without having to type in the pass phrase every time, you can remove it by executing the following command.

    [root] # openssl rsa -in userkey.pem -out userkey.pem  
    Enter pass phrase for userkey.pem:  
    writing RSA key   

Now change the file permissions to make the key only readable by you and the certificate world readable and only writable by you.

    [root] # chmod 400 userkey.pem  
    [root] # chmod 644 usercert.pem  

Host Certificate   
----------------

To request a host certificate for your server host, follow again the instructions of your CA.

The conversion to **hostcert.pem** and **hostkey.pem** works analogous to the user certificate. For the hostkey you have to remove the pass phrase. How to do this is also explained in the previous section. Finally copy the **host*.pem** files to **/etc/grid-security/** as `root` and change the file permissions in favour of the user running the grid application.

VOMS Proxy Certificate
----------------------

For very large groups of people, it is often more convenient to authorise people based on their membership of some group. To identify that they are a member of some group, the certificate owner can create a new short-lived `X.509` certificate that includes their membership of various groups. This short-lived certificate is called a proxy-certificate and, if the membership information comes from a VOMS server, it is often referred to as a VOMS-proxy.

   [root] # cd /etc/yum.repos.d/
   [root] # wget http://grid-deployment.web.cern.ch/grid-deployment/glite/repos/3.2/glite-UI.repo
   [root] # yum install glite-security-voms-clients

### `Creating a VOMS proxy`

To create a VOMS proxy for your user certificate you need to execute the **voms-proxy-init** as a user.

Example:

    [user] $ export PATH=/opt/glite/bin/:$PATH
    [user] $ voms-proxy-init
    Enter GRID pass phrase:
    Your identity: /C=DE/O=GermanGrid/OU=DESY/CN=John Doe

    Creating proxy ........................................................................Done
    Your proxy is valid until Mon Mar  7 22:06:15 2011
    

#### Certifying your membership of a VO

 You can certify your membership of a VO by using the command **voms-proxy-init -voms <yourVO>**. This is useful as in dCache authorization can be done by VO (see [the section called “Authorizing a VO”](#authorizing-a-vo)). To be able to use the extension **-voms <yourVO>** you need to be able to access VOMS servers. To this end you need the the VOMS server’s and the CA’s DN. Create a file **/etc/grid-security/vomsdir/<VO>/<hostname>.lsc** per VOMS server containing on the 1st line the VOMS server’s DN and on the 2nd line, the corresponding CA’s DN. The name of this file should be the fully qualified hostname followed by an **.lsc** extension and the file must appear in a subdirectory **/etc/grid-security/vomsdir/<VO>** for each VO that is supported by that VOMS server and by the site.

At [http://operations-portal.egi.eu/vo](https://operations-portal.egi.eu/vo) you can search for a VO and find this information. 


Example:

For example, the file /etc/grid-security/vomsdir/desy/grid-voms.desy.de.lsc contains:

    /C=DE/O=GermanGrid/OU=DESY/CN=host/grid-voms.desy.de
    /C=DE/O=GermanGrid/CN=GridKa-CA

where the first entry is the DN of the DESY VOMS server and the second entry is the DN of the CA which signed the DESY VOMS server's certificate.

In addition, you need to have a file **/opt/glite/etc/vomses** containing your VO's VOMS server.

Example:

For DESY the file **/opt/glite/etc/vomses�**` should contain the entry

    "desy" "grid-voms.desy.de" "15104" "/C=DE/O=GermanGrid/OU=DESY/CN=host/grid-voms.desy.de" "desy" "24"

The first entry “desy” is the real name or a nickname of your VO. “grid-voms.desy.de” is the hostname of the VOMS server. The number “15104” is the port number the server is listening on. The forth entry is the DN of the server's VOMS certificate. The fifth entry, “desy”, is the VO name and the last entry is the globus version number which is not used anymore and can be omitted.


Example:

Use the command **voms-proxy-init -voms** to create a VOMS proxy with VO “desy”.  

    [user] $ voms-proxy-init -voms desy
    Enter GRID pass phrase:
    Your identity: /C=DE/O=GermanGrid/OU=DESY/CN=John Doe
    Creating temporary proxy ....................................................... Done
    Contacting  grid-voms.desy.de:15104 [/C=DE/O=GermanGrid/OU=DESY/CN=host/grid-voms.desy.de] "desy" Done
    Creating proxy .................................... Done
    Your proxy is valid until Mon Mar  7 23:52:13 2011

View the information about your VOMS proxy with **voms-proxy-info**

    [user] $ voms-proxy-info
    subject   : /C=DE/O=GermanGrid/OU=DESY/CN=John Doe/CN=proxy
    issuer    : /C=DE/O=GermanGrid/OU=DESY/CN=John Doe
    identity  : /C=DE/O=GermanGrid/OU=DESY/CN=John Doe
    type      : proxy
    strength  : 1024 bits
    path      : /tmp/x509up_u500
    timeleft  : 11:28:02

The last line tells you how much longer your proxy will be valid.

If your proxy is expired you will get

    [user] $ voms-proxy-info
    subject   : /C=DE/O=GermanGrid/OU=DESY/CN=John Doe/CN=proxy
    issuer    : /C=DE/O=GermanGrid/OU=DESY/CN=John Doe
    identity  : /C=DE/O=GermanGrid/OU=DESY/CN=John Doe
    type      : proxy
    strength  : 1024 bits
    path      : /tmp/x509up_u500
    timeleft  : 0:00:00

The command **voms-proxy-info -all** gives you information about the proxy and about the VO.

    [user] $ voms-proxy-info -all
    subject   : /C=DE/O=GermanGrid/OU=DESY/CN=John Doe/CN=proxy
    issuer    : /C=DE/O=GermanGrid/OU=DESY/CN=John Doe
    identity  : /C=DE/O=GermanGrid/OU=DESY/CN=John Doe
    type      : proxy
    strength  : 1024 bits
    path      : /tmp/x509up_u500
    timeleft  : 11:24:57
    === VO desy extension information ===
    VO        : desy
    subject   : /C=DE/O=GermanGrid/OU=DESY/CN=John Doe
    issuer    : /C=DE/O=GermanGrid/OU=DESY/CN=host/grid-voms.desy.de
    attribute : /desy/Role=NULL/Capability=NULL
    attribute : /desy/test/Role=NULL/Capability=NULL
    timeleft  : 11:24:57
    uri       : grid-voms.desy.de:15104

Use the command **voms-proxy-destroy** to destroy your VOMS proxy.

    [user] $ voms-proxy-destroy
    [user] $ voms-proxy-info

    Couldn't find a valid proxy.

Configuration files
===================

In this section we explain the format of the the **storage-authzdb, kpwd** and **vorolemap** files. They are used by the `authzdb` plug-in, `vorolemap` plug-in,and `kpwd` plug-in. 

`storage-authzdb`
-----------------

In `gPlazma`, except for the `kpwd` plug-in, authorization is a two-step process. First, a username is obtained from a mapping of the user’s DN or his DN and role, then a mapping of username to UID and GID with optional additional session parameters like the root path is performed. For the second mapping usually the file called **storage-authzdb** is used. 

### Preparing **storage-authzdb**

The default location of the **storage-authzdb** is **/etc/grid-security**. Before the mapping entries there has to be a line specifying the version of the used file format.

Example:

    version 2.1

dCache supports versions 2.1 and to some extend 2.2.

Except for empty lines and comments (lines start with `#`) the configuration lines have the following format:

      authorize <username> (read-only|read-write) <UID> <GID>[,<GID>]* <homedir> <rootdir> 

For legacy reasons there may be a third path entry which is ignored by dCache. The username here has to be the name the user has been mapped to in the first step (e.g., by his DN).

Example:

    authorize john read-write 1001 100 / /data/experiments /

In this example user <john> will be mapped to UID 1001 and GID 100 with read access on the directory `/data/experiments`. You may choose to set the user's root directory to `/`.

Example:

     authorize adm read-write 1000 100 / / /

In this case the user <adm> will be granted read/write access in any path, given that the file system permissions in CHIMERA also allow the transfer.

The first path is nearly always left as “`/`”, but it may be used as a home directory in interactive session, as a subdirectory of the root path. Upon login, the second path is used as the user's root, and a “cd” is performed to the first path. The first path is always defined as being relative to the second path.

Multiple GIDs can be assigned by using comma-separated values for the GID file, as in

Example:

    authorize john read-write 1001 100,101,200 / / /

The lines of the `storage-authzdb` file are similar to the “login” lines of the `dcache.kpwd` file. If you already have a `dcache.kwpd` file, you can easily create `storage-authzdb` by taking the lines from your `dcache.kpwd` file that start with the word `login`, for example,

    login john read-write 1001 100 / /data/experiments /

and replace the word `login` with `authorize`. The following line does this for you.

    [root] #  sed "s/^ *login/authorize/" dcache.kpwd|grep "^authorize" > storage-authzdb 

The gplazmalite-vorole-mapping plug-in
--------------------------------------

The second is the **storage-authzdb** used in other plug-ins. See the above documentation on [`storage-authdb`](config-gplazma.md#storage-authzdb) for how to create the file.

### Preparing `grid-vorolemap`

The file is similar in format to the `grid-mapfile`, however there is an additional field following the DN (Certificate Subject), containing the FQAN (Fully Qualified Attribute Name).

    "/C=DE/O=GermanGrid/OU=DESY/CN=John Doe" "/some-vo" doegroup
    "/C=DE/DC=GermanGrid/O=DESY/CN=John Doe" "/some-vo/Role=NULL" doegroup
    "/C=DE/DC=GermanGrid/O=DESY/CN=John Doe" "/some-vo/Role=NULL/Capability=NULL" doegroup 

Therefore each line has three fields: the user's DN, the user's FQAN, and the username that the DN and FQAN combination are to be mapped to.

The FQAN is sometimes semantically referred to as the “role”. The same user can be mapped to different usernames depending on what their FQAN is. The FQAN is determined by how the user creates their proxy, for example, using [`voms-proxy-init`](config-gplazma.md#voms-proxy-certificate). The FQAN contains the user's Group, Role (optional), and Capability (optional). The latter two may be set to the string “NULL”, in which case they will be ignored by the plug-in. Therefore the three lines in the example above are equivalent.

Example:

If a user is authorized in multiple roles, for example

    "/DC=org/DC=doegrids/OU=People/CN=John Doe" "/some-vo/sub-grp" vo_sub_grp_user
    "/DC=org/DC=doegrids/OU=People/CN=John Doe" "/some-vo/sub-grp/Role=user" vouser
    "/DC=org/DC=doegrids/OU=People/CN=John Doe" "/some-vo/sub-grp/Role=admin" voadmin
    "/DC=org/DC=doegrids/OU=People/CN=John Doe" "/some-vo/sub-grp/Role=prod" voprod

he will get the username corresponding to the FQAN found in the proxy that the user creates for use by the client software. If the user actually creates several roles in his proxy, authorization (and subsequent check of path and file system permissions) will be attempted for each role in the order that they are found in the proxy.

In a `GRIDFTP` URL, the user may also explicitly request a username.

    gsiftp://doeprod@ftp-door.example.org:2811/testfile1

in which case other roles will be disregarded.

Authorizing a VO
----------------

Instead of individual DNs, it is allowed to use `*` or `"*"` as the first field, such as

Example:

    "*" "/desy/Role=production/" desyprod 

In that case, any DN with the corresponding role will match. It should be noted that a match is first attempted with the explicit DN. Therefore if both DN and `"*"` matches can be made, the DN match will take precedence. This is true for the revocation matches as well (see below).

Thus a user with subject `/C=DE/O=GermanGrid/OU=DESY/CN=John Doe` and role `/desy/Role=production` will be mapped to username `desyprod` via the above **storage-authzdb** line with `"*"` for the DN, except if there is also a line such as

    "/C=DE/O=GermanGrid/OU=DESY/CN=John Doe" "/desy/Role=production" desyprod2

in which case the username will be `desyprod2`.

### Revocation Entries

To create a revocation entry, add a line with a dash (`-`) as the username, such as

    "/C=DE/O=GermanGrid/OU=DESY/CN=John Doe" "/desy/production" -

or modify the username of the entry if it already exists. The behaviour is undefined if there are two entries which differ only by username.

Since DN is matched first, if a user would be authorized by his VO membership through a `"*"` entry, but is matched according to his DN to a revocation entry, authorization would be denied. Likewise if a whole VO were denied in a revocation entry, but some user in that VO could be mapped to a username through his DN, then authorization would be granted.

### More Examples

Suppose that there are users in production roles that are expected to write into the storage system data which will be read by other users. In that case, to protect the data the non-production users would be given read-only access. Here in **/etc/grid-security/grid-vorolemap** the production role maps to username `cmsprod`, and the role which reads the data maps to `cmsuser`.

    "*" "/cms/uscms/Role=cmsprod" cmsprod "*" "/cms/uscms/Role=cmsuser" cmsuser

The read-write privilege is controlled by the third field in the lines of **/etc/grid-security/storage-authzdb**

    authorize cmsprod  read-write  9811 5063 / /data /
    authorize cmsuser  read-only  10001 6800 / /data /

Example:

Another use case is when users are to have their own directories within the storage system. This can be arranged within the CELL-GPLAZMA configuration files by mapping each user's DN to a unique username and then mapping each username to a unique root path. As an example, lines from **/etc/grid-security/grid-vorolemap** would therefore be written

    "/DC=org/DC=doegrids/OU=People/CN=Selby Booth" "/cms" cms821
    "/DC=org/DC=doegrids/OU=People/CN=Kenja Kassi" "/cms" cms822
    "/DC=org/DC=doegrids/OU=People/CN=Ameil Fauss" "/cms" cms823

and the corresponding lines from **/etc/grid-security/storage-authzdb** would be

    authorize cms821 read-write 10821 7000 / /data/cms821 /
    authorize cms822 read-write 10822 7000 / /data/cms822 /
    authorize cms823 read-write 10823 7000 / /data/cms823 /

The kpwd plug-in
----------------

The section in the `gPlazma` policy file for the kpwd plug-in specifies the location of the **dcache.kpwd** file, for example

Example:
    # dcache.kpwd
    kpwdPath="/etc/dcache/dcache.kpwd"

 To maintain only one such file, make sure that this is the same location as defined in **/usr/share/dcache/defaults/dcache.properties**.

Use **/usr/share/dcache/examples/gplazma/dcache.kpwd** to create this file.

To be able to alter entries in the **dcache.kpwd** file conveniantly the dcache script offers support for doing this. 

Example:
    [user] $dcache kpwd dcuseradd testuser -u 12345 -g 1000 -h / -r / -f / -w read-write -p password

adds this to the kpwd file:

    passwd testuser ae39aec3 read-write 12345 1000 / /

There are many more commands for altering the kpwd-file, see the dcache-script help for further commands available.

The gridmap plug-in
-------------------

Two file locations are defined in the policy file for this plug-in:

    # grid-mapfile
    gridMapFilePath="/etc/grid-security/grid-mapfile"
    storageAuthzPath="/etc/grid-security/storage-authzdb"

### Preparing the `grid-mapfile`

The `grid-mapfile` is the same as that used in other applications. It can be created in various ways, either by connecting directly to VOMS or GUMS servers, or by hand.

Each line contains two fields: a DN (Certificate Subject) in quotes, and the username it is to be mapped to.

Example:

    "/C=DE/O=GermanGrid/OU=DESY/CN=John Doe" johndoe

When using the `gridmap`, the **storage-authzdb** file must also be configured. See [the section called “storage-authzdb”](config-gplazma.md#storage-authzdb) for details. 

gPlazma specific dCache configuration
=====================================

dCache has many parameters that can be used to configure the systems behaviour. You can find all these parameters well documented and together with their default values in the properties files in **/usr/share/dcache/defaults/**. To use non-default values, you have to set the new values in **/etc/dcache/dcache.conf** or in the layout file. Do not change the defaults in the properties files! After changing a parameter you have to restart the concerned cells.

Refer to the file **gplazma.properties** for a full list of properties for `gPlazma` One commonly used property is `gplazma.cell.limits.threads`, which is used to set the maximum number of concurrent requests to gPlazma. The default value is `30`.

Setting the value for `gplazma.cell.limits.threads` too high may result in large spikes of CPU activity and the potential to run out of memory. Setting the number too low results in potentially slow login activity. 

Enabling Username/Password Access for WEBDAV
--------------------------------------------

This section describes how to activate the Username/Password access for `WebDAV`. It uses **dcache.kwpd** file as an example format for storing Username/Password information. First make sure `gPlazma2` is enabled in the **/etc/dcache/dcache.conf ** or in the layout file.

Example:

Check your `WebDAV` settings: enable the `HTTP` access, disallow the anonymous access, disable requesting and requiring the client authentication and activate basic authentication.

    webdav.authn.protocol=http
    webdav.authz.anonymous-operations=NONE
    webdav.authn.accept-client-cert=false
    webdav.authn.require-client-cert=false
    webdav.authn.basic=true

Adjust the **/etc/dcache/gplazma.conf** to use the `kpwd` plug-in (for more information see also [the section called “Plug-ins”](config-gplazma.md#plug-ins). 

It will look something like this:

    auth optional kpwd
    map requisite kpwd
    session requisite kpwd

The **/etc/dcache/dcache.kpwd** file is the place where you can specify the username/password record. It should contain the username and the password hash, as well as UID, GID, access mode and the home, root and fsroot directories:

    # set passwd
    passwd tanja 6a4cd089 read-write 500 100 / / /

The passwd-record could be automatically generated by the dCache kpwd-utility, for example:

    [root] # dcache kpwd dcuseradd -u 500 -g 100 -h / -r / -f / -w read-write -p dickerelch tanja

Some file access examples:

    curl -u tanja:dickerelch http://webdav-door.example.org:2880/pnfs/

    wget --user=tanja --password=dickerelch http://webdav-door.example.org:2880/pnfs/

gPlazma config example to work with authenticated webadmin
----------------------------------------------------------

This section describes how to configure gplazma to enable the webadmin servlet in authenticated mode with a grid certificate as well as with a username/password and how to give a user administrator access.

Example:
In this example for the **/etc/dcache/gplazma.conf ** file the GP2-X509 plugin is used for the authentication step with the grid certificate and the GP2-KPWD plugin is used for the authentication step with username/password.

    auth optional x509
    auth optional kpwd
    map requisite kpwd
    session requisite kpwd

The following example will show how to set up the **/etc/dcache/dcache.kpwd** file:

    version 2.1

    mapping "/C=DE/O=ExampleOrganisation/OU=EXAMPLE/CN=John Doe" john
    # the following are the user auth records
    login john read-write 1700 1000 / / /
    /C=DE/O=ExampleOrganisation/OU=EXAMPLE/CN=John Doe

    # set pwd
    passwd john 8402480 read-write 1700 1000 / / /

This maps the DN of a grid certificate `subject=/C=DE/O=ExampleOrganisation/OU=EXAMPLE/CN=John Doe` to the user john and the entry

    login john read-write 1700 1000 / / /
      /C=DE/O=GermanGrid/OU=DESY/CN=John Doe

applies unix-like values to john, most important is the `1000`, because it is the assigned GID. This must match the value of the `httpd.authz.admin-gid` configured in your webadmin. This is sufficient for login using a certificate. The entry:

    passwd john 8402480 read-write 1700 1000 / / /

enables username/password login, such as a valid login would be user `john` with some password. The password is encrypted with the kpwd-algorithm (also see [the section called “The kpwd plug-in”](#the-kpwd-plug-in) and then stored in the file. Again the 1000 here is the assigned GID. 

<!--  [vorolemap]: #cf-gplazma-plug-inconfig-vorolemap-gridvorolemap
  [section\_title]: #cf-gplazma-plug-inconfig-voauth
  []: http://operations-portal.egi.eu/vo
  [`storage-authdb`]: #cf-gplazma-plug-inconfig-authzdb
  [`voms-proxy-init`]: #cb-voms-proxy-glite
  [1]: #cf-gplazma-gp2-configuration-plug-ins
  [2]: #cf-gplazma-kpwd
