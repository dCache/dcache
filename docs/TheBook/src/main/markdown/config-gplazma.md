Chapter 9. Authorization in dCache
===================================

To limit access to data, dCache comes with an authentication and authorization interface called `gPlazma2`. gPlazma is an acronym for Grid-aware PLuggable AuthorZation Management. Earlier versions of dCache worked with `gPlazma1` which has now been completely removed from dCache. So if you are upgrading, you have to reconfigure `gPlazma` if you used `gPlazma1` until now.

-----
[TOC bullet hierarchy]
-----

## Basics

Though it is possible to allow anonymous access to dCache it is usually desirable to authenticate users. The user then has to connect to one of the different doors (e.g., `GridFTP door, dCap door`) and login with credentials that prove his identity. In Grid-World these credentials are very often `X.509` certificates, but dCache also supports other methods like username/password and kerberos authentication.

The door collects the credential information from the user and sends a login request to the configured authorization service (i.e., `gPlazma`) Within `gPlazma` the configured plug-ins try to verify the users identity and determine his access rights. From this a response is created that is then sent back to the door and added to the entity representing the user in dCache. This entity is called `subject`. While for authentication usually more global services (e.g., ARGUS) may be used, the mapping to site specific UIDs has to be configured on a per site basis.

## Configuration

`gPlazma2` is configured by the PAM-style configuration file
`/etc/dcache/gplazma.conf`. Each line of the file is either a comment
(i.e., starts with #, is empty, or defines a plugin. Plugin defining
lines start with the plugin stack type (one of `auth, map, account,
session identity`), followed by a PAM-style modifier (one of
`optional, sufficient, required, requisite`), the plugin name and an
optional list of key-value pairs of parameters. During the login
process they will be executed in the order `auth, map, account` and
`session`. The `identity` plugins are not used during login, but later
on to map from UID+GID back to user names (e.g., for NFS). Within
these groups they are used in the order they are specified.

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

### Plug-ins

`gPlazma2` functionality is configured by combining different types of plug-ins to work together in a way that matches your requirements. For this purpose there are five different types of plug-ins. These types correspond to the keywords `auth, map, account, session` and `identity` as described in the previous section. The plug-ins can be configured via properties that may be set in `dcache.conf`, the layout-file or in `gplazma.conf`.

#### auth Plug-ins

##### kpwd

The `kpwd` plug-in authorizes users by username and password, by pairs of DN and FQAN and by `Kerberos` principals.


Properties

**gplazma.kpwd.file**

Path to   **dcache.kpwd**
Default:  `/etc/dcache/dcache.kpwd`


##### voms

The `voms` plug-in is an `auth` plug-in. It can be used to verify `X.509` credentials. It takes the certificates and checks their validity by testing them against the trusted CAs. The verified certificates are then stored and passed on to the other plug-ins in the stack.



Properties

**gplazma.vomsdir.ca**

   Path to ca certificates
   Default: `/etc/grid-security/certificates`



**gplazma.vomsdir.dir**

  Path to **vomsdir**
  Default: `/etc/grid-security/vomsdir`

##### X.509 plug-in

The X.509 is a auth plug-in that extracts X.509 certificate chains from the credentials of a user to be used by other plug-ins.

##### jaas

The `jaas` uses _Java Authentication and Authorization Service_ and implements username and password based authentication against JAAS configured service. The typical usesage of `jaas` plugin is authentication with kerberos5. Though it's possible to use `jaas` plugin for ldap authentication it's recommended to use `ldap` plugin directly.

To configure `jaas` plugin an extra configuration file defined by **dcache.authn.jaas.config** property is required. For kerberos5 configuration:

```
Krb5Gplazma {
   com.sun.security.auth.module.Krb5LoginModule required debug=false useTicketCache=false;
};
```

where _Krb5Gplazma_ is the section name to be referenced by **gplazma.jaas.name** property. For other JAAS -based configurations check desired login module documentation. A single configuration file you may have multiple sections if required.

Properties:

**dcache.authn.jaas.config**

  Path to jass config file
  Default: `/etc/dcache/jgss.conf`

**gplazma.jaas.name**

  jass configuration section name

To complete authentication `jaas` plugin should be combined with `krb5` or `mutator` mapping plugin.

Example:

    # krb5 with ldap configuration
    auth    optional        jaas gplazma.jaas.name=Krb5Gplazma
    map     optional        krb5
    map     sufficient      ldap
    identity requisite      ldap
    session optional        ldap

#### map Plug-ins

##### kpwd

As a `map` plug-in it maps usernames to UID and GID. And as a `session` plug-in it adds root and home path information to the session based on the user’s username.

Properties

  **gplazma.kpwd.file**

     Path to **dcache.kpwd**
     Default: `/etc/dcache/dcache.kpwd`



##### authzdb

The GP2-AUTHZDB takes a username and maps it to UID+GID using the `storage-authzdb` file.



**gplazma.authzdb.file**

   Path to **storage-authzdb**
   Default: `/etc/grid-security/storage-authzdb`



##### GridMap

The `authzdb` plug-in takes a username and maps it to UID+GID using the **storage-authzdb** file.



Properties

**gplazma.gridmap.file**

   Path to `grid-mapfile`
   Default: `/etc/grid-security/grid-mapfile`



##### vorolemap

The `voms` plug-in maps pairs of DN and FQAN to usernames via a [vorolemap](config-gplazma.md#preparing-grid-vorolemap) file.



Properties

**gplazma.vorolemap.file**

   Path to **grid-vorolemap**
   Default: `/etc/grid-security/grid-vorolemap`



##### krb5

 The `krb5` plug-in maps a kerberos principal to a username by removing the domain part from the principal.

Example: the Kerberos principal `user@KRB-DOMAIN.EXAMPLE.ORG` is
mapped to the user `user`.


##### nsswitch

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

##### nis

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

##### mutator

The `mutator` plugin is used to convert principal returned by third-party plugin into a principal, which is understood by gplazma plugins. For example, when the `jaas` plugin is configured to be used with an _ActiveMQ_ server, then login module specific principal is returned.

Example:

    # use activemq + mutator + authdb
    auth jaas gplazma.jaas.name=ActiveMQ
    map optional mutator gplazma.mutator.accept=org.apache.activemq.jaas.UserPrincipal gplazma.mutator.produce=username
    map requisite authzdb

Properties

**gplazma.mutator.accept**

  The fully qualified java class names of principal that have to the converted.

  **gplazma.mutator.produce**

  The gplazma internal principal short-hand name into which provided principal should be converted. The supported short-hand names:

  | Short name| Produced Principal | Description |
  |-----------|--------------------|-------------|
  | dn | GlobusPrincipal| To be consumed by Grid specific plugins|
  |kerberos| KerberosPrincipal | To be consumed bu Kerberos specific plugins|
  |fqan| FQANPrincipal | To be consumed by Grid-VO specific plugins|
  |name| LoginNamePrincipal| Login name which requires an aditional mapping to username|
  |username| UserNamePrincipal|Principal which is associated with final login step|


#### account Plug-ins

##### argus

 The argus plug-in bans users by their DN. It talks to your site’s ARGUS system (see [https://twiki.cern.ch/twiki/bin/view/EGEE/AuthorizationFramework](https://twiki.cern.ch/twiki/bin/view/EGEE/AuthorizationFramework)) to check for banned users.

Properties

**gplazma.argus.hostcert**

   Path to host certificate
   Default: `/etc/grid-security/hostcert.pem`



**gplazma.argus.hostkey**

   Path to host key
   Default: `/etc/grid-security/hostkey.pem`



**gplazma.argus.hostkey.password**

   Password for host key
   Default:



**gplazma.argus.ca**

   Path to CA certificates
   Default: `/etc/grid-security/certificates`



**gplazma.argus.endpoint**

   URL of PEP service
   Default: `https://localhost:8154/authz`



##### banfile

The `banfile` plug-in bans users by their principal class and the associated name. It is configured via a simple plain text file.

Example:

    # Ban users by principal
    alias dn=org.globus.gsi.gssapi.jaas.GlobusPrincipal
    alias kerberos=javax.security.auth.kerberos.KerberosPrincipal
    alias fqan=org.dcache.auth.FQANPrincipal
    alias name=org.dcache.auth.LoginNamePrincipal

    ban name:ernie
    ban kerberos:BERT@EXAMPLE.COM
    ban com.example.SomePrincipal:Samson

In this example the first line is a comment. Lines 2 to 5 define aliases for principal class names that can then be used in the following banning section. The four aliases defined in this example are actually hard coded into CELL-GPLAZMA, therefore you can use these short names without explicitly defining them in your configuration file. Line 7 to 9 contain ban definitions. Line 9 directly uses the class name of a principal class instead of using an alias.

Please note that the plug-in only supports principals whose assiciated name is a single line of plain text. In programming terms this means the constructor of the principal class has to take exactly one single string parameter.

The plugin assumes that a missing file is equivalent to a file with no contents; i.e., that no one has been banned.

After modifying the banfile, you may want to update gplazma.conf to trigger a reload to activate the changes. You can test the result with the `explain login` command in the gPlazma cell.


Properties

**gplazma.banfile.path**

   Path to configuration file
   Default: `/etc/dcache/ban.conf`



To activate the `banfile` it has to be added to `gplazma.conf`:

Example:

    # Map grid or kerberos users to local accounts
    auth    optional  x509
    auth    optional  voms
    map     requisite vorolemap
    map     optional  krb5
    map     optional  nis
    session requisite nis
    account requisite banfile

#### session Plug-ins



##### kpwd

The `kpwd`plug-in adds root and home path information to the session, based on the username.



Properties

**gplazma.kpwd.file**

   Path to **dcache.kpwd**
   Default: `/etc/dcache/dcache.kpwd`



##### authzdb

The `authzdb` plug-in adds root and home path information to the
session, based and username using the `storage-authzdb` file.



Properties

**gplazma.authzdb.file**

   Path to **storage-authzdb**
   Default: `/etc/grid-security/storage-authzdb`



##### nsswitch

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



##### nis

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



##### ldap

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

**gplazma.ldap.try-uid-mapping**

Default: `false`

Allow the ldap plugin to use the user's (numerical) uid to identify the user if no username is known. If enabled, the plugin uses the `uidNumber` attribute in LDAP to establish the username for such login attempts.

##### omnisession

The omnisession plugin provides an easy and convenient way to provide
session information for dCache users.  It uses a single configuration
file to describe which login attributes (such as home directory, root
directory, read-only accounts, etc) are added for dCache users.

It provides a superset of functionality of some other session plugins;
specifically, the kpwd and storage-authzdb plugins when used as
session plugins: all deployments using a storage-authzdb or kpwd
plugin during the session phase may be updated to use the omnisession
plugin instead.

The omnisession plugin uses a single configuration file.  Without any
explicit configuration, this file is located at
`${dcache.paths.etc}/omnisession.conf` (which expands to
`/etc/dcache/omnisession.conf` by default) but the location may be
adjusted by modifying the `dcache.paths.etc` configuration property or
the `gplazma.omnisession.file` configuration property.

The configuration file may have any number of empty lines.  Comments
are also supported.  Any line that starts with a hash symbol `#` is
treated as a comment and ignored.

All other lines define login attributes that apply to one or more
users.

The general format of such lines is

```
PREDICATE ATTRIBUTE [ATTRIBUTE ...]
```

The `PREDICATE` term describes to which users the attributes apply.
These mostly follow a simple `TYPE:VALUE` format; for example, the
predicate `username:paul` matches all users with a username of `paul`,
similarly, the predicate `gid:1000` matches all users with GID of
1000.  A complete list of predicate types is given below.

In some cases, a predicate's value may have spaces.  To accomodate
such cases, the value may be placed in double-quotes; for example, the
predicate `dn:"/C=DE/O=GermanGrid/OU=DESY/CN=Alexander Paul Millar"`
matches all users with that specific distinguished name.

There is a special predicate `DEFAULT` that matches all users.  This
may be used to describe attributes that match all users.  A file may
have at most one `DEFAULT` line.

The remainder of a non-empty, non-comment line is a white-space
separated list of attribute terms.  There must be at least one
attribute term, with each attribute term describing a login attribute
that should be added when the user logs in.

An attribute term generally has the form `TYPE:VALUE` where `TYPE`
describes what kind of attribute is being defined and `VALUE`
describes the specific value of that type; for example, `root:/`
indicates that the user's root directory is `/` (the root of dCache's
namespace) and `home:/Users/paul` states that the user's home
directory is `/Users/paul`.  A complete list of attribute types is
given below.

Although a line may contain multiple login attributes, there are
certain restrictions on those attributes contained on a single line.
It is not legal to express the same information more than once; for
example, a line with the two terms `root:/ root:/data` is not valid.
This is also not valid even if the values are consistent (e.g.,
`root:/ root:/`).

Here is a complete example

```
username:paul  root:/ home:/Users/paul
```

In this example, line matches user `paul` and adds two login
attributes: a home directory (with value `/Users/paul`) and a root
directory (with value `/`).


###### Login attribute overriding

During the login process, a user is identitied by a set of principals.
These principals may match multiple lines in the configuration file.
When this happens, login attributes are added in file's order: a line
that matches the user's principals that appear nearer the top of the
file is processed before a matching line that appears further down the
file's contents.

This order matters if the multiple matching lines provide login
attributes of the same type; for example, one matching line has a
`home` attribute as does a subsequent matching line.

If this happens then the first attribute (the one nearest the top of
the file) is used.  Subsequent attributes are ignored for this login
attempt.

Here is an example of attribute overriding:

```
username:paul  home:/Users/paul
group:group-a  root:/ home:/Groups/group-a
```

In this example, if a user with username `paul` and who is a member of
group `group-a` logs in, the home directory will be `/Users/paul` and
the root directory will be `/`.  Other members of `group-a` will have
a home directory of `/Groups/group-a` and a root directory of `/`.

The `DEFAULT` is special.  It always matches last, irrespective of
where it appears in the configuration file.  Therefore, if a login
attempt matches any lines then those attributes will always take
precedence over the DEFAULT set of login attributes.

Although dCache doesn't care where in the file the DEFAULT predicate
is located, but it's recommended to put it at the end of the file.

The following is another example of attribute overriding, but this
time using DEFAULT.

```
username:paul  home:/Users/paul
group:group-a  home:/Groups/group-a
DEFAULT        root:/ home:/
```

In this example, the user with username `paul` has a home directory
`/Users/paul`, members of `group-a` have a home directory
`/groups/group-a` (except for user `paul`) and all dCache users have a
home directory of `/` (except for members of group-a and user `paul`).
All users have a root directory of `/`.


###### Combining with other session plugins

If the omnisession plugin is combined with other session plugins then
omniplugin will refrain from adding attributes that are already
defined.  Omnisession will add any other (undefined) attributes.

Here's an example.  gPlazma is configured to use both `ldap` and
`omnisession` as session plugins.  When user X logs in the ldap plugin
adding a home directory and the omnisession configuration file
indicates both a home directory and a root directory for user X.  This
user will receive the home directory from LDAP and the root directory
from omnisession.

The omnisession plugin is successful if it adds at least one login
attribute, and fails otherwise.  This affects gPlazma configuration
(e.g., optional vs sufficient vs requisite).  For details, see the
section of gPlazma configuration.

Note that users are required to have a home directory attribute and a
root directory attribute before their login will be successful.

###### Handling bad configuration

A line within the configuration file with bad attribute declarations
will be marked invalid.  The plugin will fail for any user who's
principals match that invalid line's predicate.  The plugin will be
successful for users with predicates that only match valid lines and
for whom at least one attribute is added.

Here is an example:

```
username:paul  home:/ home:/Users/paul
DEFAULT        root:/ home:/
```

The first line is invalid because the `home` attribute is defined
twice.  Therefore, the plugin will fail for a login attempt with
username `paul`, but will succeed for other users.

If a line has a badly written predicate then the entire file is
considered bad and the plugin will fail for all users.

Here is an example:

```
user:paul  home:/Users/paul
DEFAULT    root:/ home:/
```

The predicate `user:paul` is not valid (it should have be
`username:paul`).  Because of this, the plugin will fail for all
users.

###### Predicates catalogue

The omnisession plugin accepts the following predicates.

- **dn** The user's Distinguished Name, which is typically obtained by
   authenticating via X.509.  Example:
   `dn:"/C=DE/O=GermanGrid/OU=DESY/CN=Alexander Paul Millar"`
- **email** Predicate matches the user's email address, if known.
   Example: `email:user@example.org` matches users with email address
   `user@example.org`.
- **gid** Predicate matches the user's gid.  There is an optional
   field that describes whether the match should be limited to users
   with a matching primary gid, a matching nonprimary gid, or a match
   gid of either type.  Example: `gid:1000` matches a user that is a
   member of the gid 1000 group.  `gid:1000,primary` matches members
   of the group with gid 1000 if the user has this as their primary
   gid, `gid:1000,nonprimary` matches members of the group with gid
   1000 if the user has this as their non-primary gid.
- **group** Predicate that matches members of the named group. There
   is an optional field that describes whether the match should be
   limited to users with a matching primary group, a matching
   nonprimary group, or a matching group of either use.  Example:
   `group:group-a` matches a user that is a member of the `group-a`
   group.  `group:group-a,primary` matches members of the `group-a`
   group if the user has this as their primary group,
   `group:group-a,nonprimary` matches members of the `group-a` group
   if the user has this as their non-primary gid.
- **fqan** Predicate that matches a VOMS FQAN. There is an optional
   field that describes whether the match should be limited to users
   with a matching primary FQAN, a matching nonprimary FQAN, or a
   matching FQAN of either use.  Example: `fqan:/atlas` matches a user
   that is a member of the `/atlas` FQAN.  `fqan:/atlas,primary`
   matches members of the `/atlas` FQAN if the user has this as their
   primary FQAN, `fqan:/atlas,nonprimary` matches members of the
   `/atlas` FQAN if the user has this as their non-primary FQAN.
- **kerberos** Predicate that matches a Kerberos principal.  Example:
   `kerberos:paul@DESY.DE` matches users with the Kerberos principal
   `paul@DESY.DE`.
- **oidc** Predicate that matches an OpenID-Connect `sub` ("subject")
   claim from a specific OP.  Example:
   `oidc:83326983-68a3-4f2c-8f0b-385ecd3e97b2@OP` matches users
   identified by the `sub` claim with the value
   `83326983-68a3-4f2c-8f0b-385ecd3e97b2` who have authenticated with
   the OAuth2 Provider configured with the alias `OP`.
- **oidcgrp** Predicate that matches the OpenID-Connect `groups` claim
   values.  Example: `oidcgrp:/main-group` matches `groups` claim that
   contains a value `/main-group`.
- **uid** Predicate that matches the uid of a user.  Example:
   `uid:15691` matches the user with uid 15691.
- **username** Predicate that matches the username of a user.
   Example: `username:paul` matches the user with username `paul`.
- **entitlement** Predicate that matches an eduPersonEntitlement
   claim.  Example:
   `entitlement:urn:geant:example.org:group:EXAMPLE#example.org`
   matches users that have the eduPersonEntitlement claim
   `urn:geant:helmholtz.de:group:EXAMPLE#login.helmholtz.de`

###### Login attribute catalogue

This section provides a list of all login attributes that the
omnisession plugin supports.

The `root`, `home` and `prefix` attributes accept an absolute path as
a value.  This means those values must start with a `/`.

- **read-only** This attribute limits the user so that the may not
   modify dCache.  Example: `read-only`.
- **root** The user's root directory.  The user will see only a
   subtree of dCache's namespace.  This works similarly to the chroot
   command. Example: `root:/data/experiment-A` that limits the user's
   view of the namespace to the `/data/expriment-A` subtree.
- **home** The user's home directory. In general, this is a directory
   that is somehow special for the user, although the precise
   semantics of this attribute is protocol specific.  There are
   network protocols or clients that have no concept of a home
   directory. Example: `home:/Users/paul` indicates the user has a
   home directory of `/Users/paul`.
- **prefix** This attribute limits which part of the namespace a user
   may access.  Unlike the `root` attribute, the user sees parent
   directories; however, parent directories appear as if they have a
   single entry.  Example: `prefix:/data/experiment-A` allows the user
   access to a specific subtree while preserving paths.
- **max-upload** Limit the size of any single uploaded file.  The
   value is a file size that may optionally ISO symbols.  Examples:
   `max-upload:5GiB` limits uploads to a maximum of five gibibyte
   (~5.4e9 bytes), `max-upload:1TB` limits uploads to a maximum of one
   terabyte (1e12 bytes), `max-upload:1048576` limits users to one
   mibibyte files.

#### identity Plug-ins

##### nsswitch

The `nsswitsch` provides forward and reverse mapping for `NFSv4.1` using your system's `nsswitch` service.

##### nis

The `nis` plug-in forward and reverse mapping for `NFSv4.1` using your site's NIS service.

Properties

**gplazma.nis.server**

   `NIS` server host
   Default: `nisserv.domain.com`

**gplazma.nis.domain**

   `NIS` domain
    Default: domain.com

## Using X509 Certificates

Most plug-ins of `gPlazma` support `X.509` certificates for authentication and authorisation. `X.509` certificates are used to identify entities (e.g., persons, hosts) in the Internet. The certificates contain a DN (Distinguished Name) that uniquely describes the entity. To give the certificate credibility it is issued by a CA (Certificate Authority) which checks the identity upon request of the certificate (e.g., by checking the persons id). For the use of X.509 certificates with dCache your users will have to request a certificate from a CA you trust and you need host certificates for every host of your dCache instance.


### CA Certificates

To be able to locally verify the validity of the certificates, you need to store the CA certificates on your system. Most operating systems come with a number of commercial CA certificates, but for the *Grid* you will need the certificates of the Grid CAs. For this, CERN packages a number of CA certificates. These are deployed by most grid sites. By deploying these certificates, you state that you trust the CA's procedure for the identification of individuals and you agree to act promptly if there are any security issues.

To install the CERN CA certificates follow the following steps:

```console-root
cd /etc/yum.repos.d/
wget http://grid-deployment.web.cern.ch/grid-deployment/glite/repos/3.2/lcg-CA.repo
yum install lcg-CA
```

This will create the directory `/etc/grid-security/certificates` which
contains the Grid CA certificates.

Certificates which have been revoked are collected in certificate
revocation lists (CRLs). To get the CRLs install the `fetch-crl`
command as described below.

```console-root
yum install fetch-crl
fetch-crl
```

`fetch-crl` adds X.509 CRLs to `/etc/grid-security/certificates`. It
is recommended to set up a cron job to periodically update the CRLs.

### User Certificate

If you do not have a valid grid user certificate yet, you have to
request one from your CA. Follow the instructions from your CA on how
to get a certificate. After your request was accepted you will get a
URL pointing to your new certificate. Install it into your browser to
be able to access grid resources with it. Once you have the
certificate in your browser, make a backup and name it
`userCertificate.p12`. Copy the user certificate to the directory
`~/.globus/` on your worker node and convert it to `usercert.pem`
and `userkey.pem` as described below.

```console-user
openssl pkcs12 -clcerts -nokeys -in <userCertificate>.p12 -out usercert.pem
|Enter Import Password:
|MAC verified OK
```

During the backup your browser asked you for a password to encrypt the certificate. Enter this password here when asked for a password. This will create your user certificate.

```console-user
openssl pkcs12 -nocerts -in <userCertificate>.p12 -out userkey.pem
|Enter Import Password:
|MAC verified OK
|Enter PEM pass phrase:
```

In this step you need to again enter the backup password. When asked for the PEM pass phrase choose a secure password. If you want to use your key without having to type in the pass phrase every time, you can remove it by executing the following command.

```console-root
openssl rsa -in userkey.pem -out userkey.pem
|Enter pass phrase for userkey.pem:
|writing RSA key
```

Now change the file permissions to make the key only readable by you and the certificate world readable and only writable by you.

```console-root
chmod 400 userkey.pem
chmod 644 usercert.pem
```

### Host Certificate

To request a host certificate for your server host, follow again the instructions of your CA.

The conversion to `hostcert.pem` and `hostkey.pem` works analogous to
the user certificate. For the hostkey you have to remove the pass
phrase. How to do this is also explained in the previous
section. Finally copy the `host*.pem` files to `/etc/grid-security/`
as `root` and change the file permissions in favour of the user
running the grid application.

### VOMS Proxy Certificate

For very large groups of people, it is often more convenient to authorise people based on their membership of some group. To identify that they are a member of some group, the certificate owner can create a new short-lived `X.509` certificate that includes their membership of various groups. This short-lived certificate is called a proxy-certificate and, if the membership information comes from a VOMS server, it is often referred to as a VOMS-proxy.

```console-root
cd /etc/yum.repos.d/
wget http://grid-deployment.web.cern.ch/grid-deployment/glite/repos/3.2/glite-UI.repo
yum install glite-security-voms-clients
```

#### `Creating a VOMS proxy`

To create a VOMS proxy for your user certificate you need to execute
the `voms-proxy-init` as a user.

Example:

```console-user
export PATH=/opt/glite/bin/:$PATH
voms-proxy-init
|Enter GRID pass phrase:
|Your identity: /C=DE/O=GermanGrid/OU=DESY/CN=John Doe
|
|Creating proxy ..................................Done
|Your proxy is valid until Mon Mar  7 22:06:15 2011
```


##### Certifying your membership of a VO

 You can certify your membership of a VO by using the command
 `voms-proxy-init -voms <yourVO>`. This is useful as in dCache
 authorization can be done by VO (see [the section called “Authorizing
 a VO”](#authorizing-a-vo)). To be able to use the extension `-voms
 <yourVO>` you need to be able to access VOMS servers. To this end you
 need the the VOMS server’s and the CA’s DN. Create a file
 `/etc/grid-security/vomsdir/<VO>/<hostname>.lsc` per VOMS server
 containing on the 1st line the VOMS server’s DN and on the 2nd line,
 the corresponding CA’s DN. The name of this file should be the fully
 qualified hostname followed by an `.lsc` extension and the file must
 appear in a subdirectory `/etc/grid-security/vomsdir/<VO>` for each
 VO that is supported by that VOMS server and by the site.

At [http://operations-portal.egi.eu/vo](https://operations-portal.egi.eu/vo) you can search for a VO and find this information.


Example:

For example, the file `/etc/grid-security/vomsdir/desy/grid-voms.desy.de.lsc` contains:

    /C=DE/O=GermanGrid/OU=DESY/CN=host/grid-voms.desy.de
    /C=DE/O=GermanGrid/CN=GridKa-CA

where the first entry is the DN of the DESY VOMS server and the second entry is the DN of the CA which signed the DESY VOMS server's certificate.

In addition, you need to have a file `/opt/glite/etc/vomses`
containing your VO's VOMS server.

Example:

For DESY the file `/opt/glite/etc/vomses` should contain the entry

    "desy" "grid-voms.desy.de" "15104" "/C=DE/O=GermanGrid/OU=DESY/CN=host/grid-voms.desy.de" "desy" "24"

The first entry “desy” is the real name or a nickname of your VO. “grid-voms.desy.de” is the hostname of the VOMS server. The number “15104” is the port number the server is listening on. The forth entry is the DN of the server's VOMS certificate. The fifth entry, “desy”, is the VO name and the last entry is the globus version number which is not used anymore and can be omitted.


Example:

Use the command `voms-proxy-init -voms` to create a VOMS proxy with VO
“desy”.

```console-user
voms-proxy-init -voms desy
|Enter GRID pass phrase:
|Your identity: /C=DE/O=GermanGrid/OU=DESY/CN=John Doe
|Creating temporary proxy ....................................................... Done
|Contacting  grid-voms.desy.de:15104 [/C=DE/O=GermanGrid/OU=DESY/CN=host/grid-voms.desy.de] "desy" Done
|Creating proxy .................................... Done
|Your proxy is valid until Mon Mar  7 23:52:13 2011
```

View the information about your VOMS proxy with `voms-proxy-info`

```console-user
voms-proxy-info
|subject   : /C=DE/O=GermanGrid/OU=DESY/CN=John Doe/CN=proxy
|issuer    : /C=DE/O=GermanGrid/OU=DESY/CN=John Doe
|identity  : /C=DE/O=GermanGrid/OU=DESY/CN=John Doe
|type      : proxy
|strength  : 1024 bits
|path      : /tmp/x509up_u500
|timeleft  : 11:28:02
```

The last line tells you how much longer your proxy will be valid.

If your proxy is expired you will get

```console-user
voms-proxy-info
|subject   : /C=DE/O=GermanGrid/OU=DESY/CN=John Doe/CN=proxy
|issuer    : /C=DE/O=GermanGrid/OU=DESY/CN=John Doe
|identity  : /C=DE/O=GermanGrid/OU=DESY/CN=John Doe
|type      : proxy
|strength  : 1024 bits
|path      : /tmp/x509up_u500
|timeleft  : 0:00:00
```

The command `voms-proxy-info -all` gives you information about the
proxy and about the VO.

```console-user
voms-proxy-info -all
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
```

Use the command `voms-proxy-destroy` to destroy your VOMS proxy.

```console-user
voms-proxy-destroy
voms-proxy-info
|
|Couldn't find a valid proxy.
```


## Using OpenID Connect

dCache also supports the use of OpenID Connect bearer tokens as a means of authentication. 

OpenID Connect is a federated identity system.  The dCache users see federated identity as a way to use their existing username & password safely.  From a dCache admin's point-of-view, this involves "outsourcing" responsibility for checking users identities to some external service: you must trust that the service is doing a good job.

> OpenID Connect is a simple identity layer on top of the OAuth 2.0 protocol. It enables Clients to verify the identity of the End-User based on the authentication performed by an Authorization Server, as well as to obtain basic profile information about the End-User in an interoperable and REST-like manner.
>
> --<cite>http://openid.net/specs/openid-connect-core-1_0.html<cite>

Common examples of Authorisation servers are Google, Indigo-IAM, Cern-Single-Signon etc.

As of version 2.16, dCache is able to perform authentication based on [OpendID Connect](http://openid.net/specs/openid-connect-core-1_0.html) credentials on its HTTP end-points. In this document, we outline the configurations necessary to enable this support for OpenID Connect. 

OpenID Connect credentials are sent to dCache with Authorisation HTTP Header as follows 
`Authorization: Bearer  <yaMMeexxx........>`. This bearer token is extracted, validated and verified against a **Trusted Authorisation Server** (Issue of the bearer token) and is used later to fetch additional user identity information from the corresponding Authorisation Server.





  This configuration property is a map.  Each entry of the map
  associates a nickname with information about that provider.  The
  nickname is used when logging problems with the provider.  The
  information is the URI of the issuer endpoint.  This must be a
  valid URL that starts 'https://'.

  The following example associates the nickname 'google' with
  Google's issuer endpoint.

```ini
      gplazma.oidc.provider!google = https://accounts.google.com/
```

  The process for validating an access token has been improved with
  newer versions of dCache.  Sometimes these improvements, while
  correct, risk introducing incompatibilities when upgrading from an
  earlier versions of dCache.  To support backwards compatibility the
  `-suppress` option may be specified on a specific OP; for example,

```ini
  gplazma.oidc.provider!EXAMPLE = https://op.example.org/ -suppress=foo
```

 The `-suppress option's` value is a comma-separated list; in
  addition, the -suppress option may be repeated.

  The following suppress keywords are supported:

**audience**  Suppress audience ("aud") claim verification.  By
                default, dCache will check that the "aud" claim, if
                present, matches one of the identities configured via
               the `gplazma.oidc.audience-targets` configuration
                property.  The token is rejected if there is no
               match.  This option suppresses this check, resulting
                in dCache accepting tokens with an arbitrary "aud"
                claim value.In general, audience verification provides a form of
                "damage limitation" if a token is misappropriated.
                The stolen token may only be used against the token's
               intended target service and not against any other
                service that the user is authorised. 
                Suppressing audience verification may be needed as
                earlier versions of dCache lacked audience
                verification.  Existing clients may be obtaining
                tokens with inappropriate audience fields.  It is
                recommended that this option is used sparingly and
                only for a short time; i.e., a strategy is devised
                through which clients are updated and this
                suppression may be disabled.             
**offline**   Suppress offline validation.  By default, dCache will
                attempt to validate a JWT using the OP's public keys.
                If the token is valid then the claims stored within
                the JWT are used by dCache.  This option suppresses
                this behaviour, resulting in dCache making an HTTP
                request to the OP's userinfo endpoint to check the
                validity of the token and obtain the claims that
                describe the user. In general, offline verification is preferred as it
                is faster and avoids making an HTTP request (one per
                token) against the OP.  Suppressing offline validation is needed if OP
                includes less information in the JWT than is
                available from the userinfo endpoint and that
                additional information is needed (e.g., group
                membership).




Since there is no universal token schema and token scopes are different the `-profile` option is used to set explicitly define the type of the token to be used. By default it is OIDC and WLCG token has a different schema (`-profile=wlcg`).

  The `-prefix` This attribute limits which part of the namespace a user may access. 


### Steps for configuration 

In order to configure the OpenID Connect support, we need to 

1. configure the gplazma plugins providing the support for authentication using OpenID credentials and mapping a verified OpenID credential to dCache specific `username`, `uid` and `gid`.
2. enabling the plugins in gplazma 

### Gplazma Plugins for OpenId Connect

The support for OpenID Connect in Cache is achieved with the help of two gplazma plugins.

#### OpenID Authenticate Plugin (oidc)
It takes the extracted OpenID connect credentials (Bearer Token) from the HTTP requests and validates it against a OpenID Provider end-point. The admins need to obtain this information from their trusted OpenID Provider such as Google.

In case of Google, the provider end-point can be obtained from the url of its [Discovery Document](http://openid.net/specs/openid-connect-core-1_0.html#OpenID.Discovery), e.g. https://accounts.google.com/.well-known/openid-configuration. Hence, the provider end-point in this case would be **accounts.google.com**. 

This end-point has to be appended to the gplazma property **gplazma.oidc.hostnames**, which should be added to the layouts file. Multiple trusted OpenID providers can be added with space separated list as below.

`gplazma.oidc.hostnames = accounts.google.com iam-test.indigo-datacloud.eu`

#### MultiMap plugin (multimap)

dCache requires that authenticated credentials be mapped to posix style `username`, `uid` and `gid`. In case of OpenID credentials, it can be achieved through the new gplazma multimap plugin. This plugin is able to take a verified OpenID credentials in the form of OpenID Subject or the corresponding Email address, and map it to a username, uid, gid etc.

For example,

> oidc:9889-1231-2999-12312@GOOGLE    username:kermit
>
> email:kermit.the.frog@email.com     username:thefrog

In this example, the first line matches users with `sub` claim
`9889-1231-2999-12312` from the OAuth2 Provider `GOOGLE` and adds the
username `kermit`.  The second example matches the email address
`kermit.the.frog@email.com` and adds the username `thefrog`.  In both
cases, it is assumed there is an additional mapping from username to
uid, gid etc in files like storage-autzdb.

This mapping as shown above can be stored in a gplazma multi-map configuration file. The location of the multimap configuration file can be specified with another gplazma property **gplazma.multimap.file**. By default it is configured to be located in /etc/dcache/multi-mapfile.

#### Enable the gplazma plugins

The two plugins above must be enabled in the gplazma.conf.

> auth optional oidc

> map optional  multimap 

Restart dCache and check that there are no errors in loading these gplazma plugins.

#### Third-Party Transfer with OpenID Connect Credentials

Third-party transfer with OpenID Connect Credentials are also possible. dCache performs a token-exchange with the OpenID provider in order and obtain a new delegated bearer token for itself, which it can use (and refresh) to perform Third-party transfer.

In order to perform a token-exchange, it requires the id and secret of a client registered to the OpenID Provider same as that of the bearer token (that was received with the COPY request).

The client-id and client-secret of such a client can be set in the webdav properties as follows,

> webdav.oidc.client.ids!provider.hostname : id of a client registered to an OpenId Provider

> webdav.oidc.client.secrets!provider.hostname : client-secret corresponding the client-id specified above

Here, **provider.hostname** must be replaced with a supported OpenID provider like accounts.google.com.

## Configuration files

In this section we explain the format of the the **storage-authzdb, kpwd** and **vorolemap** files. They are used by the `authzdb` plug-in, `vorolemap` plug-in,and `kpwd` plug-in.

### `storage-authzdb`

In `gPlazma`, except for the `kpwd` plug-in, authorization is a two-step process. First, a username is obtained from a mapping of the user’s DN or his DN and role, then a mapping of username to UID and GID with optional additional session parameters like the root path is performed. For the second mapping usually the file called **storage-authzdb** is used.

#### Preparing **storage-authzdb**

The default location of the **storage-authzdb** is
`/etc/grid-security`. Before the mapping entries there has to be a
line specifying the version of the used file format.

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

```console-root
sed "s/^ *login/authorize/" dcache.kpwd|grep "^authorize" > storage-authzdb
```

### The gplazmalite-vorole-mapping plug-in

The second is the **storage-authzdb** used in other plug-ins. See the above documentation on [`storage-authdb`](config-gplazma.md#storage-authzdb) for how to create the file.

#### Preparing `grid-vorolemap`

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

### Authorizing a VO

Instead of individual DNs, it is allowed to use `*` or `"*"` as the first field, such as

Example:

    "*" "/desy/Role=production/" desyprod

In that case, any DN with the corresponding role will match. It should be noted that a match is first attempted with the explicit DN. Therefore if both DN and `"*"` matches can be made, the DN match will take precedence.

Thus a user with subject `/C=DE/O=GermanGrid/OU=DESY/CN=John Doe` and
role `/desy/Role=production` will be mapped to username `desyprod` via
the above `storage-authzdb` line with `"*"` for the DN, except if
there is also a line such as

    "/C=DE/O=GermanGrid/OU=DESY/CN=John Doe" "/desy/Role=production" desyprod2

in which case the username will be `desyprod2`.

#### More Examples

Suppose that there are users in production roles that are expected to
write into the storage system data which will be read by other
users. In that case, to protect the data the non-production users
would be given read-only access. Here in
`/etc/grid-security/grid-vorolemap` the production role maps to
username `cmsprod`, and the role which reads the data maps to
`cmsuser`.

    "*" "/cms/uscms/Role=cmsprod" cmsprod "*" "/cms/uscms/Role=cmsuser" cmsuser

The read-write privilege is controlled by the third field in the lines
of `/etc/grid-security/storage-authzdb`

    authorize cmsprod  read-write  9811 5063 / /data /
    authorize cmsuser  read-only  10001 6800 / /data /

Example:

Another use case is when users are to have their own directories
within the storage system. This can be arranged within the
CELL-GPLAZMA configuration files by mapping each user's DN to a unique
username and then mapping each username to a unique root path. As an
example, lines from `/etc/grid-security/grid-vorolemap` would
therefore be written

    "/DC=org/DC=doegrids/OU=People/CN=Selby Booth" "/cms" cms821
    "/DC=org/DC=doegrids/OU=People/CN=Kenja Kassi" "/cms" cms822
    "/DC=org/DC=doegrids/OU=People/CN=Ameil Fauss" "/cms" cms823

and the corresponding lines from `/etc/grid-security/storage-authzdb`
would be

    authorize cms821 read-write 10821 7000 / /data/cms821 /
    authorize cms822 read-write 10822 7000 / /data/cms822 /
    authorize cms823 read-write 10823 7000 / /data/cms823 /

### The kpwd plug-in

The section in the `gPlazma` policy file for the kpwd plug-in
specifies the location of the `dcache.kpwd` file, for example

Example:
    # dcache.kpwd
    kpwdPath="/etc/dcache/dcache.kpwd"

To maintain only one such file, make sure that this is the same
location as defined in `/usr/share/dcache/defaults/dcache.properties`.

Use `/usr/share/dcache/examples/gplazma/dcache.kpwd` to create this
file.

To be able to alter entries in the `dcache.kpwd` file conveniantly the
dcache script offers support for doing this.

Example:

```console-root
dcache kpwd dcuseradd testuser -u 12345 -g 1000 -h / -r / -f / -w read-write -p password
```

adds this to the kpwd file:

```
passwd testuser ae39aec3 read-write 12345 1000 / /
```

There are many more commands for altering the kpwd-file, see the dcache-script help for further commands available.

### The gridmap plug-in

Two file locations are defined in the policy file for this plug-in:

    # grid-mapfile
    gridMapFilePath="/etc/grid-security/grid-mapfile"
    storageAuthzPath="/etc/grid-security/storage-authzdb"

#### Preparing the `grid-mapfile`

The `grid-mapfile` is the same as that used in other applications. It can be created in various ways, either by connecting directly to VOMS or GUMS servers, or by hand.

Each line contains two fields: a DN (Certificate Subject) in quotes, and the username it is to be mapped to.

Example:

    "/C=DE/O=GermanGrid/OU=DESY/CN=John Doe" johndoe

When using the `gridmap`, the `storage-authzdb` file must also be
configured. See [the section called
“storage-authzdb”](config-gplazma.md#storage-authzdb) for details.

## gPlazma specific dCache configuration

dCache has many parameters that can be used to configure the systems
behaviour. You can find all these parameters well documented and
together with their default values in the properties files in
`/usr/share/dcache/defaults/`. To use non-default values, you have to
set the new values in `/etc/dcache/dcache.conf` or in the layout
file. Do not change the defaults in the properties files! After
changing a parameter you have to restart the concerned cells.

Refer to the file `gplazma.properties` for a full list of properties
for `gPlazma` One commonly used property is
`gplazma.cell.limits.threads`, which is used to set the maximum number
of concurrent requests to gPlazma. The default value is `30`.

Setting the value for `gplazma.cell.limits.threads` too high may result in large spikes of CPU activity and the potential to run out of memory. Setting the number too low results in potentially slow login activity.

### Enabling Username/Password Access for WEBDAV

This section describes how to activate the Username/Password access
for `WebDAV`. It uses `dcache.kwpd` file as an example format for
storing Username/Password information. First make sure `gPlazma2` is
enabled in the `/etc/dcache/dcache.conf` or in the layout file.

Example:

Check your WebDAV settings: enable the HTTP access, disallow the
anonymous access, disable requesting and requiring the client
authentication and activate basic authentication.

```ini
webdav.authn.protocol=http
webdav.authz.anonymous-operations=NONE
webdav.authn.accept-client-cert=false
webdav.authn.require-client-cert=false
webdav.authn.basic=true
```

Adjust the `/etc/dcache/gplazma.conf` to use the `kpwd` plug-in (for
more information see also [the section called
“Plug-ins”](config-gplazma.md#plug-ins).

It will look something like this:

    auth optional kpwd
    map requisite kpwd
    session requisite kpwd

The `/etc/dcache/dcache.kpwd` file is the place where you can specify
the username/password record. It should contain the username and the
password hash, as well as UID, GID, access mode and the home, root and
fsroot directories:

    # set passwd
    passwd tanja 6a4cd089 read-write 500 100 / / /

The passwd-record could be automatically generated by the dCache kpwd-utility, for example:

```console-root
dcache kpwd dcuseradd -u 500 -g 100 -h / -r / -f / -w read-write -p dickerelch tanja
```

Some file access examples:

    curl -u tanja:dickerelch http://webdav-door.example.org:2880/pnfs/

    wget --user=tanja --password=dickerelch http://webdav-door.example.org:2880/pnfs/

### Roles

Roles are a way of describing what capabilities a given user has.  They constitute
a set of operations defined either explicitly or implicitly which the user who
is assigned that role is permitted to exercise.  

Roles further allow users to act in more than one capacity without having to 
change their basic identity. For instance, a "superuser" may wish to act as _janedoe_ 
for some things, but as an administrator for others, without having to reauthenticate.

While the role framework in dCache is designed to be extensible, there 
currently exists only one recognized role, that of _admin_.  

To activate the use of the _admin_ role, the following steps are necessary.

1) Define the admin role using the property:  

```ini
gplazma.roles.admin-gid=<gid>
```
    
2) Add the _admin_ gid to the set of gids for any user who should have this capability.

3) Add the roles plugin to your gPlazma configuration (usually 'requisite' is sufficient):  

    session requisite roles

Roles are currently used by the [dCache Frontend Service](config-frontend.md)
to distinguish between regular and admin access.  Please refer to that section of
this document for further information.

<!--  [vorolemap]: #cf-gplazma-plug-inconfig-vorolemap-gridvorolemap
  [section\_title]: #cf-gplazma-plug-inconfig-voauth
  []: http://operations-portal.egi.eu/vo
  [`storage-authdb`]: #cf-gplazma-plug-inconfig-authzdb
  [`voms-proxy-init`]: #cb-voms-proxy-glite
  [1]: #cf-gplazma-gp2-configuration-plug-ins
  [2]: #cf-gplazma-kpwd
