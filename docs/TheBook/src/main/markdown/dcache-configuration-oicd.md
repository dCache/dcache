


### Introduction to multimap and OIDC Plug-ins in dCache

- Dedicated OIDC plugin for authenticating 

- For Mapping use multimapplug-in: mapmany different credentialsto dCache uid and gid

- Changes to gPlazma.conffile

### Enable oidc authentication plugin

### Replacing usual vorolemap plug-in

> map     optional    vorolemap #2.1

> map      sufficient   multimap gplazma.multimap.file=/etc/dcache/multi-mapfile.wlcg_jwt



we will have now 

# /etc/dcache/gplazma.conf
...
auth     optional     oidc
map      sufficient   multimap gplazma.multimap.file=/etc/dcache/multi-mapfile.wlcg_jwt
...


 

Lets have a look on a complete configuration example and go through the each phase.


>vi etc/dcache/gplazma.conf
                              
 ```ini

cat >/etc/dcache/gplazma.conf <<EOF
auth    optional    x509 #1.1
auth    optional    voms #1.2
auth    optional  htpasswd #1

map     optional    vorolemap #2.1
map     optional    gridmap #2.2
map     requisite   authzdb #2.3

session requisite   roles #3.2
session requisite   authzdb #3.2
EOF                            
```


During the login process they will be executed in the order **auth**, **map**, **account** and
**session**. The identity plugins are not used during login, but later on to map from UID+GID back to user. Within these groups they are used in the order they are specified.



  **auth**  phase - verifies user’s identity. auth-plug-ins are used to read the users public and private credentials and ask some authority, if those are valid for accessing the system.

**#1.1** This configuration tells gPlazma to use the **x.509** plugin used to extracts X.509 certificate chains from the credentials of a user to be used by other plug-ins
If user comes with grid
certificate and VOMS role: extract user’s DN (**#1.2**), checks if the username and password exist in database (**#1.3**), which should be added to

password file **/etc/dcache/htpasswd**.
The htpasswd plugin uses the Apache HTTPD server’s file format to record username and passwords. This
file may be maintained by the htpasswd command.
Let us create a new password file (/etc/dcache/htpasswd) and add these two users (”tester” and ”admin”)
with passwords TooManySecrets and dickerelch respectively:

> touch /etc/dcache/htpasswd
> 
> 
> htpasswd -bm /etc/dcache/htpasswd admin admin
>


**optional** here means, the success or failure of this plug-in is only important if it is the only plug-in in the stack associated
with this type.

 **#2** **map** - converts this identity to some dCache user.
                                              
 **#2.1** the “grid-mapfile”-file, the client-certificate’s DN is mapped to a
virtual user-name.                      

                                              
 ```ini
cat >/etc/grid-security/grid-mapfile <<EOF
"/C=DE/ST=Hamburg/O=dCache.ORG/CN=Kermit the frog" kermit
EOF 
 ```
 
**#2.2** the vorolemap plug-in maps the users DN+FQAN to a username which is then
mapped to UID/GIDs by the authzdb plug-in.
                                          
  ```ini

cat >/etc/grid-security/grid-vorolemap <<EOF
"*" "/desy" desyuser
EOF
 ```
 
 
 
 
 **#2.3** Using the “storage-authzdb-style”-file, this virtual user-name is then mapped to
the actual UNIX user-ID 4 and group-IDs 4


```ini
cat >/etc/grid-security/storage-authzdb <<EOF
version 2.1

authorize admin    read-write    0    0 / / /
authorize desyuser read-write 1000 2000 / / /
authorize kermit   read-write 1000 1000 / / /
EOF
```


 **suﬀicient** Success of such a plug-in is enough to satisfy the authentication requirements of the stack of
plug-ins (if a prior required plug-in has failed the success of this one is ignored). A failure of this plug-in is
not deemed as fatal for the login attempt. If the plug-in succeeds gPlazma2 immediately proceeds with the
next plug-in type or returns control to the door if this was the last stack.

Here is an example of the output of this 3 phases.

```console-root
[centos@os-46-install1 ~]$ sudo journalctl -f -u dcache@dCacheDomain.service
Jan 05 13:44:47 os-46-install1.novalocal dcache@dCacheDomain[25977]:  |   +--gridmap OPTIONAL:FAIL (no mapping) => OK
Jan 05 13:44:47 os-46-install1.novalocal dcache@dCacheDomain[25977]:  |   |
Jan 05 13:44:47 os-46-install1.novalocal dcache@dCacheDomain[25977]:  |   +--authzdb REQUISITE:FAIL (no mappable principal) => FAIL (ends the phase)
Jan 05 13:44:47 os-46-install1.novalocal dcache@dCacheDomain[25977]:  |
Jan 05 13:44:47 os-46-install1.novalocal dcache@dCacheDomain[25977]:  +--(ACCOUNT) skipped
Jan 05 13:44:47 os-46-install1.novalocal dcache@dCacheDomain[25977]:  |
Jan 05 13:44:47 os-46-install1.novalocal dcache@dCacheDomain[25977]:  +--(SESSION) skipped
Jan 05 13:44:47 os-46-install1.novalocal dcache@dCacheDomain[25977]:  |
Jan 05 13:44:47 os-46-install1.novalocal dcache@dCacheDomain[25977]:  +--(VALIDATION) skipped
Jan 05 13:45:15 os-46-install1.novalocal dcache@dCacheDomain[25977]: 05 Jan 2023 13:45:15 (pool1) [] The file system containing the data files appears to have less free space (40,453,738,496 bytes) than expected (40,453,779,120 bytes); reducing the pool size to 40,455,127,376 bytes to compensate. Notice that this does not leave any space for the meta data. If such data is stored on the same file system, then it is paramount that the pool size is reconfigured to leave enough space for the meta data.
 ```

 
 


Finally, **session** adds some additional information, for example the user’s home directory.


This ability to split login steps between different plugins may make the process seem complicated; however,
it is also very powerful and allows dCache to work with many different authentication schemes.






