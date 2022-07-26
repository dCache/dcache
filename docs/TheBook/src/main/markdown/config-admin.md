THE ADMIN SERVICE
=================

> **JUST USE COMMANDS THAT ARE DOCUMENTED HERE**
>
> Only commands described in this documentation should be used for the administration of a dCache system.

-----
[TOC bullet hierarchy]
-----

## First Steps

dCache has a powerful administration interface.  Administration protocol is implemented as `admin` cell that
embeds `ssh` server. Once logged to admin interface an administrator can connect or send commands to other cells
in the system.

It is useful to run the admin service in its own separate domain.
In the example of [the section called “Installing a dCache instance”](install.md)
this domain is called  adminDoorDomain:

```ini
[adminDoorDomain]
[adminDoorDomain/admin]
```

> **Note**
>
> All configurable values of the ssh admin interface can be found in
> the `/usr/share/dcache/defaults/admin.properties` file. Please do
> NOT change any value in this file. Instead enter the key value
> combination in the `/etc/dcache/dcache.conf`.


## Access with SSH

The `admin` service embeds `ssh` server listening on port 22224 (configurable) and supports the following authentication mechanisms :

- kerberos
- password
- public key authentication.

The mechanisms can be enabled by setting the following variable:

```ini
admin.ssh.authn.enabled = password,publickey,kerberos
```

(that is comma separated mechanism names). By default `publickey` and `password` are enabled.
To enable `kerberos` it needs to be added to the list.
To complete `kerberos` setup the following variable needs to be defined:

```ini
dcache.authn.kerberos.realm=EXAMPLE.ORG
```

and `admin.ssh.authn.kerberos.keytab-file` should point existing keytab file. Default is `/etc/krb5.keytab`.

There are two ways of authorizing administrators to access the dCache `ssh` admin interface - public key based
authorization and `gPlazma` based authorization.  The configuration of both authorization mechanisms is
described below.


### Public Key Authorization

To authorize administrators by their public key insert the key into
the file `authorized_keys2` which should be placed in the directory
`/etc/dcache/admin` as specified in the file
`/usr/share/dcache/defaults/admin.properties` under
`admin.paths.authorized-keys`. Each key has to be one line (no line
breaks) and should have a standard format, such as:


    ssh-rsa AAAAB3....GWvM= admin@localhost


> **IMPORTANT**
>
> Please make sure that the copied key is still in one line. Any line-break will prevent the key from being read.
>
> DSA keys are deprecated and will not work.
>
> The key's comment (the part behind the equal sign) should match the admin user's name, e.g., admin@localhost


Now you can login to the admin interface by

```console-user
ssh -p 22224 -l admin headnode.example.org
|dCache (##VERSION##)
|Type "\?" for help.
```

Public key based authorization is default with a fallback to `gPlazma` `kpwd` plugin.


### Access via gPlazma and the `dcache.kpwd` file

To use `gPlazma` make sure that you added it to your layout file :

```ini
[gplazmaDomain]
[gplazmaDomain/gplazma]
```

The `gPlazma` configuration file `/etc/dcache/gplazma.conf` has to
look like:

```
auth    sufficient      kpwd  "kpwd=/etc/dcache/dcache.kpwd"
map     sufficient      kpwd  "kpwd=/etc/dcache/dcache.kpwd"
session sufficient      kpwd  "kpwd=/etc/dcache/dcache.kpwd"
```

Add a user `admin` to the `/etc/dcache/dcache.kpwd` file using the
`dcache` script.

>    Example:
>    ```console-user
>    dcache kpwd dcuseradd admin -u 12345 -g 1000 -h / -r / -f / -w read-write -p password
>    |writing to /etc/dcache/dcache.kpwd :
>    |
>    |done writing to /etc/dcache/dcache.kpwd :
>    ```

After you ran the above command the following like appears in
`/etc/dcache/dcache.kpwd` file:

```
passwd admin 4091aba7 read-write 12345 1000 / /
```

For more information about `gPlazma` see [Chapter 10, Authorization in dCache](config-gplazma.md).

Now the user `admin` can login to the admin interface with his password `password` by:

```console-user
ssh -l admin -p 22224 headnode.example.org
|admin@headnode.example.org's password:
|dCache (##VERSION##)
|Type "\?" for help.
```

To utilize kerberos authentication mechanism the following lines need
to be added to `/etc/dcache/dcache.kpwd` file:

```
mapping "johndoe@EXAMPLE.ORG" admin

login admin read-write 0 0 / / /
  johndoe@EXAMPLE.ORG
```

Then, you can access dCache having obtained kerberos ticket:

```console-user
kinit johndoe@EXAMPLE.ORG
ssh -l admin -p 22224 headnode.example.org
|dCache (##VERSION##)
|Type "\?" for help.
```


To allow other users access to the admin interface add them to the `/etc/dcache/dcache.kpwd` file as described above.

Just adding a user in the `dcache.kpwd` file is not sufficient. The generated user also needs access priileges that can only be set within the admin interface itself.

See [the section called “Creating a new user”](#creating-a-new-user) to learn how to create the user in the admin interface and set the rights.


## How to use the Admin Interface

Admin interface allows you to execute shell commands, connect to other cells and execute their supported commands or
send supported cell commands to other cells. Once logged in you are prompted to use help `Type "\?" for help`.

```
[headnode] (local) admin > \?
\? [command]...  # display help for shell commands
\c cell[@domain] [user]  # connect to cell
\exception [trace]  # controls display of stack traces
\h [command]...  # display help for cell commands
\l [cell[@domain]|pool/poolgroup]...  # list cells
\q # quit
\s [OPTIONS] (cell[@domain]|pool/poolgroup)[,(cell[@domain]|pool/poolgroup)]... command...  # send command
\sl [options] pnfsid|path command...  # send to locations
\sn [options] command...  # send pnfsmanager command
\sp [options] command...  # send poolmanager command
\timeout [seconds]  # sets the command timeout

[headnode] (local) admin >
```

Shell commands are always available at command prompt, whereas in order to execute cell commands you have to either connect to the cell
using `\c cell[@domain]` and execute command or send command to the cell using `\s [OPTIONS] (cell[@domain]|pool/poolgroup)[,(cell[@domain]|pool/poolgroup)]... command...`. For instance:

```
[headnode] (local) enstore > \? \c
NAME
       \c -- connect to cell

SYNOPSIS
       \c cell[@domain] [user]

DESCRIPTION
       Connect to new cell. May optionally switch to another user.

ARGUMENTS
       cell[@domain]
              Well known or fully qualified cell name.
       user
              Account to connect with.

[headnode] (local) enstore >
```

The `\l` command executed without arguments lists all well-known cells in the system. In general cells are
addressed by their full name `<name>@<domainName>`. For well-known cells the `@<domainName>` part can be omitted.
Executing `\l *@*` will list everything running in your dCache system.

> **NOTE**
>
> If the cells are well-known, they can be accessed without adding the domain-scope. See [Cell Message passing](config-config-message-passing.md) for more information.


Each cell implements `help [command]` (also aliased as `\h [command]`) which, when executed
without parameters,  displays a set of commands supported by the cell. When provided with command name as an argument
it shows that command syntax like so:

```
[headnode] (local) admin > \s pool_1 help log set
Sets the log level of <appender>.
```

> **NOTE**
>
> You can send (`\s`) `help` command to a cell but you can't send `\h` command to a cell. The `\h` command can be executed only after connecting to a cell using `\c` command:

```
[headnode] (local) admin > \c pool_1
[headnode] (pool_1@poolDomain) admin > \h log set
Sets the log level of <appender>.
```

> **WARNING**
>
> Some commands are dangerous. Executing them without understanding what they do may lead to data loss.


The command `\q` exits the admin shell.

If you want to find out which cells are running on a certain domain, you can issue the command `ps` in the System `cell` of the domain.

Example:
For example, if you want to list the cells running on the `poolDomain`, `\c` to its `System` cell and issue the `ps` command.

      (local) admin > \c System@poolDomain
      (System@poolDomain) admin > ps
        Cell List
      ------------------
      c-dCacheDomain-101-102
      System
      pool_2
      c-dCacheDomain-101
      pool_1
      RoutingMgr
      lm

The cells in the domain can be accessed using `\c` together with the cell-name scoped by the domain-name. So first, one has to   get back to the local prompt, as the `\c` command will not work otherwise.

> **NOTE**
>
> Note that `\c` only works from the local prompt. If the cell you are trying to access does not exist, the `\c` command will   complain.
>
>     Example:
>     (local) admin > \c nonsense
>     Cell as it doesn't exist
>


Connect to the routing manager of the `dCacheDomain` and use `ls` command to get a list of all well-known cells, running in
each domain:

    Example:
      (local) admin > \c RoutingMgr@dCacheDomain
      (RoutingMgr@dCacheDoorDomain) admin > ls
      Our routing knowledge :
       Local : [PoolManager, topo, LoginBroker, info]
       adminDoorDomain : [pam]
      gsidcapDomain : [DCap-gsi-example.dcache.org]
      dcapDomain : [DCap-example.dcache.org]
      utilityDomain : [gsi-pam, PinManager]
      gPlazmaDomain : [gPlazma]
      webdavDomain : [WebDAV-example.dcache.org]
       gridftpDomain : [GFTP-example.dcache.org]
      srmDomain : [RemoteTransferManager, CopyManager, SrmSpaceManager, SRM-example.dcache.org]
       httpdDomain : [billing, srm-LoginBroker, TransferObserver]
      poolDomain : [pool_2, pool_1]
       namespaceDomain : [PnfsManager, dirLookupPool, cleaner]

All cells implement the `info` command to display  general information about the cell and `show pinboard` command
for listing the last lines of the [pinboard](rf-glossary.md#pinboard) of the cell. The output of these commands contains useful information
for troubleshooting.

The most useful command of the pool cells is [rep ls](reference.md#rep-ls). It lists the file replicas
 which are stored in the pool by their `pnfs` IDs:

```
(RoutingMgr@dCacheDoorDomain) admin >  \s pool_1  rep ls
000100000000000000001120 <-P---------(0)[0]> 485212 si={myStore:STRING}
000100000000000000001230 <C----------(0)[0]> 1222287360 si={myStore:STRING}
(RoutingMgr@dCacheDoorDomain) admin >


(RoutingMgr@dCacheDoorDomain) admin > \c pool_1
(pool_1) admin > rep ls
000100000000000000001120 <-P---------(0)[0]> 485212 si={myStore:STRING}
000100000000000000001230 <C----------(0)[0]> 1222287360 si={myStore:STRING}
```

Each file replica  in a pool has one of the 4 primary states: “cached” (<C---), “precious” (<-P--), “from client” (<--C-), and        “from store” (<---S).


See [the section called “How to Store-/Restore files via the Admin Interface”](config-hsm.md#how-to-store-restore-files-via-the-admin-interface) for more information about `rep ls`.

The most important commands in the `PoolManager` are: `rc ls` and `cm ls -r`.

`rc ls` lists the requests currently handled by the `PoolManager`. A typical line of output for a read request with an error condition is (all in one line):

```
(pool_1) admin > \c PoolManger
(PoolManager) admin > rc ls
000100000000000000001230@0.0.0.0/0.0.0.0 m=1 r=1 [<unknown>]
[Waiting 08.28 19:14:16]
{149,No pool candidates available or configured for 'staging'}
```

As the error message at the end of the line indicates, no pool was found containing the file replica
 and no pool could be used for staging the file from a tertiary storage system.



See [the section called “Obtain information via the dCache Command Line Admin Interface”](config-hsm.md#how-to-monitor-whats-going-on) for more information about the command `rc ls`

Finally, [cm ls](reference.md#cm-ls) with the option `-r` gives the information about the pools currently stored in the cost module of the pool manager. A typical output is:

```
(PoolManager) admin > cm ls -r
pool_1={R={a=0;m=2;q=0};S={a=0;m=2;q=0};M={a=0;m=100;q=0};PS={a=0;m=20;q=0};PC={a=0;m=20;q=0};
    (...continues...)   SP={t=2147483648;f=924711076;p=1222772572;r=0;lru=0;{g=20000000;b=0.5}}}
pool_1={Tag={{hostname=example.org}};size=0;SC=0.16221282938326134;CC=0.0;}
pool_2={R={a=0;m=2;q=0};S={a=0;m=2;q=0};M={a=0;m=100;q=0};PS={a=0;m=20;q=0};PC={a=0;m=20;q=0};
    (...continues...)   SP={t=2147483648;f=2147483648;p=0;r=0;lru=0;{g=4294967296;b=250.0}}}
pool_2={Tag={{hostname=example.org}};size=0;SC=2.7939677238464355E-4;CC=0.0;}
```


While the first line for each pool gives the information stored in the cache of the cost module, the second line gives the    costs (SC: [space cost](rf-glossary.md#space-cost), CC: [performance cost](rf-glossary.md#performance-cost)) calculated for a (hypothetical) file of zero size. For details on how these are calculated and their meaning, see [the section called “Classic Partitions”](#config-poolmanager.md#classic-partitions).

## Creating a new user

To create a new user, <new-user> and set a new password for the user `\c` from the local prompt `((local) admin >)` to the acm, the access control manager, and run following command sequence:

```
(local) admin > \c acm
(acm) admin > create user <new-user>
(acm) admin > set passwd -user=<new-user> <newPasswd> <newPasswd>
```

For the newly created users there will be an entry in the directory
`/etc/dcache/admin/users/meta`.

> **NOTE**
>
> As the initial user `admin` has not been created with the above
> command you will not find him in the directory
> `/etc/dcache/admin/users/meta`.

Give the new user access to the PnfsManager.

```
(acm) admin > create acl cell.<cellName>.execute
(acm) admin > add access -allowed cell.<cellName>.execute <new-user>
```

Example:
Give the new user access to the PnfsManager.

```
(acm) admin > create acl cell.PnfsManager.execute
(acm) admin > add access -allowed cell.PnfsManager.execute <new-user>
```

Now you can check the permissions by:

```
(acm) admin > check cell.PnfsManager.execute <new-user>
Allowed
(acm) admin > show acl cell.PnfsManager.execute
<noinheritance>
<new-user> -> true
```

The following commands allow access to every cell for a user <new-user>:

```
(acm) admin > create acl cell.*.execute
(acm) admin > add access -allowed cell.*.execute <new-user>
```


The following command makes a user as powerful as admin (dCache’s equivalent to the root user):

```
(acm) admin > create acl *.*.*
(acm) admin > add access -allowed *.*.* <new-user>
```

## Direct command execution

Admin ssh server allows direct command execution like so:

```console-user
ssh -p 22224 admin@adminNode  "command1; command2; command3"
```

That is it accepts semicolon (';') separated list of commands. Spaces between commands and command
separators do not matter.


## Use of the SSH Admin Interface by scripts

In scripts, one can use a “Here Document” to list the commands, or supply them to `ssh` as standard-input (stdin). The following demonstrates using a Here Document:

```shell
#!/bin/sh
#
#  Script to automate dCache administrative activity

outfile=/tmp/$(basename $0).$$.out

ssh -p 22224 admin@adminNode  > $outfile << EOF
\c PoolManager
cm ls -r
\q
EOF
```

or, the equivalent as stdin.

```shell
#!/bin/bash
#
#   Script to automate dCache administrative activity.

echo -e '\c pool_1\nrep ls\n(more commands here)\n\q' \
  | ssh -p 22224 admin@adminNode \
  | tr -d '\r' > rep_ls.out
```
