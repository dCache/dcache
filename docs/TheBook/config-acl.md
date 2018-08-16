CHAPTER 18.  ACLS IN dCache
===========================

Table of Contents
-----------------

* [Introduction](#introduction)
* [Database configuration](#database-configuration)
* [Configuring ACL support](#configuring-acl-support)
        
  [Enabling ACL support](#enabling-acl-support)
        
* [Administrating ACLs](#administrating-acls)

  [How to set ACLs](#how-to-set-acls)

* [Viewing configured ACLs](#viewing-configured-acls)

dCache includes support for Access Control Lists (ACLs). This support is conforming to the [NFS version 4 Protocol specification](http://www.nfsv4-editor.org/).

This chapter provides some background information and details on configuring dCache to use ACLs and how to administer the resulting system.

> **ACLS AND PNFS**
>
> ACLs are only supported with the Chimera name space backend. Versions before 1.9.12 had partial support for ACLs with the pnfs backend, however due to the limitations of that implementation ACLs were practically useless with pnfs.

INTRODUCTION
============

dCache allows control over namespace operations (e.g., creating new files and directories, deleting items, renaming items) and data operations (reading data, writing data) using the standard Unix permission model. In this model, files and directories have both owner and group-owner attributes and a set of permissions that apply to the owner, permissions for users that are members of the group-owner group and permissions for other users.

Although Unix permission model is flexible enough for many deployment scenarios there are configurations that either cannot configured easily or are impossible. To satisfy these more complex permission handling dCache has support for ACL-based permission handling.

An Access Control List (ACL) is a set of rules for determining whether an end-user is allowed to undertake some specific operation. Each ACL is tied to a specific namespace entry: a file or directory. When an end-user wishes to undertake some operation then the ACL for that namespace entry is checked to see if that user is authorised. If the operation is to create a new file or directory then the ACL of the parent directory is checked.

> **FILE- AND DIRECTORY- ACLS**
>
> Each ACL is associated with a specific file or directory in dCache. Although the general form is the same whether the ACL is associated with a file or directory, some aspects of an ACL may change. Because of this, we introduce the terms file-ACL and directory-ACL when taking about ACLs associated with a file or a directory respectively. If the term ACL is used then it refers to both file-ACLs and directory-ACLs.

Each ACL contains a list of one or more Access Control Entries (ACEs). The ACEs describe how dCache determines whether an end-user is authorised. Each ACE contains information about which group of end users it applies to and describes whether this group is authorised for some subset of possible operations.

The order of the ACEs within an ACL is significant. When checking whether an end-user is authorised each ACE is checked in turn to see if it applies to the end-user and the requested operation. If it does then that ACE determines whether that end-user is authorised. If not then the next ACE is checked. Thus an ACL can have several ACEs and the first matched ACE “wins”.

One of the problems with traditional Unix-based permission model is its inflexible handling of newly created files and directories. With transitional filesystems, the permissions that are set are under the control of the user-process creating the file. The sysadmin has no direct control over the permissions that newly files or directories will have. The ACL permission model solves this problem by allowing explicit configuration using inheritance.

ACL inheritance is when a new file or directory is created with an ACL containing a set of ACEs from the parent directory's ACL. The inherited ACEs are specially marked so that only those that are intended will be inherited.

Inheritance only happens when a new file or directory is created. After creation, the ACL of the new file or directory is completely decoupled from the parent directory's ACL: the ACL of the parent directory may be altered without affecting the ACL of the new file or directory and visa versa.

Inheritance is optional. Within a directory's ACL some ACEs may be inherited whilst others are not. New files or directories will receive only those ACEs that are configured; the remaining ACEs will not be copied.

DATABASE CONFIGURATION
======================

ACL support requires database tables to store ACL and ACE information. These tables are part of the CHIMERA name space backend and for a new installation no additional steps are needed to prepare the database.

Early versions of Chimera (before dCache 1.9.3) did not create the ACL table during installation. If the database is lacking the extra table then it has to be created before enabling ACL support. This is achieved by applying two SQL files:

    [root] # psql chimera < /usr/share/dcache/chimera/sql/addACLtoChimeraDB.sql
    [root] # psql chimera < /usr/share/dcache/chimera/sql/pgsql-procedures.sql

CONFIGURING ACL SUPPORT
=======================

The **dcache.conf** and layout files contain a number of settings that may be adjusted to configure dCache's permission settings. These settings are are described in this section.

ENABLING ACL SUPPORT
--------------------

To enable ACL support set `pnfsmanager.enable.acl`=`true` in the layout file.

    ..
    [<domainName>/pnfsmanager]
    pnfsmanager.enable.acl=true
    ..

ADMINISTRATING ACLS
===================

Altering dCache ACL behaviour is achieved by connecting to the `PnfsManager` [well-known cell](rf-glossary.md#well-known-cell) using the administrator interface. For further details about how to use the administrator interface, see [the section called “The Admin Interface”](intouch.md#the-admin-interface).

The `info` and `help` commands are available within `PnfsManager` and fulfil their usual functions.

HOW TO SET ACLS
---------------

The `setfacl` command is used to set a new ACL. This command accepts arguments with the following form:  

setfacl <ID> <ACE> [<ACE>...]  

The <ID> argument is either a pnfs-ID or the absolute path of some file or directory in dCache. The `setfacl` command requires one or more <ACE> arguments seperated by spaces.  

The `setfacl` command creates a new ACL for the file or directory represented by <ID>. This new ACL replaces any existing ACEs for <ID>.  

An ACL has one or more ACEs. Each ACE defines permissions to access this resource for some [Subject](#the-subject). The ACEs are space-separated and the ordering is significant. The format and description of these ACE values are described below.  

### Description of the ACE structure  

The <ACE> arguments to the `setfacl` command have a specific format. This format is described below in Extended Backus-Naur Form (EBNF).

[1]	ACE	::=	Subject ':' Access |   
                        Subject ':' Access ':' Inheritance	   
[2]	Subject	::=	'USER:' UserID |   
'GROUP:' GroupID |   
'OWNER@' |   
'GROUP@' |   
'EVERYONE@' |   
'ANONYMOUS@' |   
'AUTHENTICATED@'	  
[3]	Access	::=	'+' Mask |  
'-' Mask	  
[4]	Mask	::=	Mask MaskItem |   
MaskItem	   
[5]	MaskItem	::=	'r' | 'l' | 'w' | 'f' | 's' | 'a' | 'n' | 'N' | 'x' | 'd' | 'D' | 't' | 'T' | 'c' | 'C' | 'o'	 
[6]	Inheritance	::=	Inheritance Flag |   
Flag	 
[7]	Flag	::=	'f' | 'd' | 'o'	   
[8]	UserID	::=	INTEGER	   
[9]	GroupID	::=	INTEGER    
The various options are described below.  

#### The Subject

The [Subject](#the-subject) defines to which user or group of users the ACE will apply. It acts as a filter so that only those users that match the Subject will have their access rights affected.

As indicated by the EBNF above, the Subject of an ACE can take one of several forms. These are described below:
 
**USER:**<id>  
The `USER:` prefix indicates that the ACE applies only to the specific end-user: the dCache user with ID <id>. For example, `USER:0:+w` is an ACE that allows user 0 to write over a file's existing data.

**GROUP:**<id>  
The `GROUP:` prefix indicates that the ACE applies only to those end-users who are a member of the specific group: the dCache group with ID <id>. For example, `GROUP:20:+a` is an ACE that allows any user who is a member of group 20 to append data to the end of a file.

**OWNER@**    
The `OWNER@` subject indicates that the ACE applies only to whichever end-user owns the file or directory. For example, `OWNER@:+d` is an ACE that allows the file's or directory's owner to delete it.

**GROUP@**   
The `GROUP@` subject indicates that the ACE applies only to all users that are members of the group-owner of the file or directory. For example, `GROUP@:+l` is an ACE that allows any user that is in a directory's group-owner to list the directory's contents.

**EVERYONE@**   
The `EVERYONE@` subject indicates that the ACE applies to all users. For example, `EVERYONE@:+r` is an ACE that makes a file world-readable.

**ANONYMOUS@**   
The `ANONYMOUS@` Subject indicates that the ACE applies to all users who have not authenticated themselves. For example, `ANONYMOUS@:-l` is an ACE that prevents unauthenticated users from listing the contents of a directory.

`AUTHENTICATED@`  
The `AUTHENTICATED@` Subject indicates that an ACE applies to all authenticated users. For example, `AUTHENTICATED@:+r` is an ACE that allows any authenticated user to read a file's contents.

> **AUTHENTICATED OR ANONYMOUS**
>
> An end user of dCache is either authenticated or is unauthenticated, but never both. Because of this, an end user operation will either match ACEs with `ANONYMOUS@` Subjects or `AUTHENTICATED@` Subjects but the request will never match both at the same time.

#### Access mask

[Access](#description-of-the-ace-structure) (defined in the [ACE EBNF](#description-of-the-ace-structure) above) describes what kind of operations are being described by the ACE and whether the ACE is granting permission or denying it.

An individual ACE can either grant permissions or deny them, but never both. However, an ACL may be composed of any mixture of authorising- and denying- ACEs. The first character of [Access](##description-of-the-ace-structure) describes whether the ACE is authorising or denying.

If [Access](#description-of-the-ace-structure) begins with a plus symbol (`+`) then the ACE authorises the [Subject](#description-of-the-ace-structure) some operations. The ACE `EVERYONE@:+r` authorises all users to read a file since the ACE-ACCESS begins with a `+`.

If the [Access](#description-of-the-ace-structure) begins with a minus symbol (`-`) then the ACE denies the [Subject](#description-of-the-ace-structure) some operations. The ACE `EVERYONE@:-r` prevents any user from reading a file since the Access begins with a `-`.

The first character of [Access](#description-of-the-ace-structure) must be `+` or `-`, no other possibility is allowed. The initial `+` or `-` of [Access](#description-of-the-ace-structure) is followed by one or more operation letters. These letters form the ACE's *access mask* ([Mask](#description-of-the-ace-structure) in [ACE EBNF](#description-of-the-ace-structure) above).

The access mask describes which operations may be allowed or denied by the ACE. Each type of operation has a corresponding letter; for example, obtaining a directory listing has a corresponding letter `l`. If a user attempts an operation of a type corresponding to a letter present in the access mask then the ACE may affect whether the operation is authorised. If the corresponding letter is absent from the access mask then the ACE will be ignored for this operation.

The following table describes the access mask letters and the corresponding operations:

> **FILE- AND DIRECTORY- SPECIFIC OPERATIONS**
>
> Some operations and, correspondingly, some access mask letters only make sense for ACLs attached to certain types of items. Some operations only apply to directories, some operations are only for files and some operations apply to both files and directories.
>
> When configuring an ACL, if an ACE has an operation letter in the access mask that is not applicable to whatever the ACL is associated with then the letter is converted to an equivalent. For example, if `l` (list directory) is in the access mask of an ACE that is part of a file-ACL then it is converted to `r`. These mappings are described in the following table.

**r**   
reading data from a file. Specifying `r` in an ACE's access mask controls whether end-users are allowed to read a file's contents. If the ACE is part of a directory-ACL then the letter is converted to `l`.

**l**   
listing the contents of a directory. Specifying `l` in an ACE's access mask controls whether end-users are allowed to list a directory's contents. If the ACE is part of a file-ACL then the letter is converted to `r`.

**w**   
overwriting a file's existing contents. Specifying `w` in an ACE's access mask controls whether end-users are allowed to write data anywhere within the file's current offset range. This includes the ability to write to any arbitrary offset and, as a result, to grow the file. If the ACE is part of a directory-ACL then the letter is converted to `f`.

**f** 
creating a new file within a directory. Specifying `f` in an ACE's access mask controls whether end-users are allowed to create a new file. If the ACE is part of an file-ACL then then the letter is converted to `w`.

**s**  
creating a subdirectory within a directory. Specifying `s` in an ACE's access mask controls whether end-users are allowed to create new subdirectories. If the ACE is part of a file-ACL then the letter is converted to `a`.

**a**  
appending data to the end of a file. Specifying `a` in an ACE's access mask controls whether end-users are allowed to add data to the end of a file. If the ACE is part of a directory-ACL then the letter is converted to `s`.

**n**    
reading attributes. Specifying `n` in an ACE's access mask controls whether end-users are allowed to read attributes. This letter may be specified in ACEs that are part of a file-ACL and those that are part of a directory-ACL.

**N**   
write attributes. Specifying `N` in an ACE's access mask controls whether end-users are allowed to write attributes. This letter may be specified in ACEs that are part of a file-ACL and those that are part of a directory-ACL.

**x**   
executing a file or entering a directory. `x` may be specified in an ACE that is part of a file-ACL or a directory-ACL; however, the operation that is authorised will be different.

Specifying **x** in an ACEs access mask that is part of a file-ACL will control whether end users matching the ACE Subject are allowed to execute that file.

Specifying **x** in an ACEs access mask that is part of a directory-ACL will control whether end users matching ACE Subject are allowed to search a directory for a named file or subdirectory. This operation is needed for end users to change their current working directory.

**d**   
deleting a namespace entry. Specifying **d** in an ACE's access mask controls whether end-users are allowed to delete the file or directory the ACL is attached. The end user must be also authorised for the parent directory (see `D`).

**D**  
deleting a child of a directory. Specifying **D** in the access mask of an ACE that is part of a directory-ACL controls whether end-users are allowed to delete items within that directory. The end user must be also authorised for the existing item (see **d**).
 
**t**   
reading basic attributes. Specifying `t` in the access mask of an ACE controls whether end users are allowed to read basic (i.e., non-ACL) attributes of that item.

**T**   
altering basic attributes. Specifying `T` in an ACE's access mask controls whether end users are allowed to alter timestamps of the item the ACE's ACL is attached.

**c**    
reading ACL information. Specifying `c` in an ACE's access mask controls whether end users are allowed to read the ACL information of the item to which the ACE's ACL is attached.

**C**   
writing ACL information. Specifying `C` in an ACE's access mask controls whether end users are allowed to update ACL information of the item to which the ACE's ACL is attached.

**o**  
altering owner and owner-group information. Specifying `o` controls whether end users are allowed to change ownership information of the item to which the ACE's ACL is attached.

#### ACL inheritance

To enable ACL inheritance, the optional [inheritance flags](#description-of-the-ace-structure) must be defined. The flag is a list of letters. There are three possible letters that may be included and the order doesn't matter.

**f**   
This inheritance flag only affects those ACEs that form part of an directory-ACL. If the ACE is part of a file-ACL then specifying `f` has no effect.

If a file is created in a directory with an ACE with `f` in inheritance flags then the ACE is copied to the newly created file's ACL. This ACE copy will not have the `f` inheritance flag.

Specifying `f` in an ACE's inheritance flags does not affect whether this ACE is inherited by a newly created subdirectory. See `d` for more details.

**d**    
This inheritance flag only affect those ACEs that form part of an directory-ACL. If the ACE is part of a file-ACL then specifying `d` has no effect.

Specifying `d` in an ACE's inheritance flags does not affect whether this ACE is inherited by a newly created file. See `f` for more details.

If a subdirectory is created in a directory with an ACE with `d` in the ACE's inheritance flag then the ACE is copied to the newly created subdirectory's ACL. This ACE copy will have the `d` inheritance flag specified. If the `f` inheritance flag is specified then this, too, will be copied.

**o**   
The `o` flag may only be used when the ACE also has the `f`, `d` or both `f` and `d` inheritance flags.

Specifying `o` in the inheritance flag will suppress the ACE. No user operations will be authorised or denied as a result of such an ACE.

When a file or directory inherits from an ACE with `o` in the inheritance flags then the `o` is *not* present in the newly created file or directory's ACE. Since the newly created file or directory will not have the `o` in it's inheritance flags the ACE will take effect.

An `o` in the inheritance flag allows child files or directories to inherit authorisation behaviour that is different from the parent directory.

## Examples  

This section gives some specific examples of how to set ACLs to achieve some specific behaviour.  

Example 18.1.   

ACL allowing specific user to delete files in a directory 

This example demonstrates how to configure a directory-ACL so user 3750 can delete any file within the directory **/pnfs/example.org/data/exampleDir**.

    (PnfsManager) admin > setfacl /pnfs/example.org/data/exampleDir EVERYONE@:+l USER:3750:D
        (...line continues...)   USER:3750:+d:of
    (PnfsManager) admin > setfacl /pnfs/example.org/data/exampleDir/existingFile1
        (...line continues...)   USER:3750:+d:f
    (PnfsManager) admin > setfacl /pnfs/example.org/data/exampleDir/existingFile2
        (...line continues...)   USER:3750:+d:f

The first command creates an ACL for the directory. This ACL has three ACEs. The first ACE allows anyone to list the contents of the directory. The second ACE allows user 3750 to delete content within the directory in general. The third ACE is inherited by all newly created files and specifies that user 3750 is authorised to delete the file independent of that file's ownership.

The second and third commands creates an ACL for files that already exists within the directory. Since ACL inheritance only applies to newly created files or directories, any existing files must have an ACL explicitly set.


Example 18.2.

ACL to deny a group  #

The following example demonstrates authorising all end users to list a directory. Members of group 1000 can also create subdirectories. However, any member of group 2000 can do neither.

    (PnfsManager) admin > setfacl /pnfs/example.org/data/exampleDir GROUP:2000:-sl
        (...line continues...)    EVERYONE@:+l GROUP:1000:+s

The first ACE denies any member of group 2000 the ability to create subdirectories or list the directory contents. As this ACE is first, it takes precedence over other ACEs.

The second ACE allows everyone to list the directory's content. If an end user who is a member of group 2000 attempts to list a directory then their request will match the first ACE so will be denied. End users attempting to list a directory that are not a member of group 2000 will not match the first ACE but will match the second ACE and will be authorised.

The final ACE authorises members of group 1000 to create subdirectories. If an end user who is a member of group 1000 and group 2000 attempts to create a subdirectory then their request will match the first ACE and be denied.


Example 18.3.

ACL to allow a user to delete all files and subdirectories  

This example is an extension to [Example 18.1, “ACL allowing specific user to delete files in a directory”](#example-18.1.-acl-allowing-specific-user-to-delete-files-in-a-directory). The previous example allowed deletion of the contents of a directory] but not the contents of any subdirectories. This example allows user 3750 to delete all files and subdirectories within the directory.


   (PnfsManager) admin > setfacl /pnfs/example.org/data/exampleDir USER:3750:+D:d
        (...line continues...)    USER:3750:+d:odf

The first ACE is `USER:3750:+D:d`. This authorises user 3750 to delete any contents of directory **/pnfs/example.org/data/exampleDir** that has an ACL authorising them with `d` operation.

The first ACE also contains the inheritance flag `d` so newly created subdirectories will inherit this ACE. Since the inherited ACE will also contain the `d` inheritance flag, this ACE will be copied to all subdirectories when they are created.

The second ACE is `USER:3750:+d:odf`. The ACE authorises user 3750 to delete whichever item the ACL containing this ACE is associated with. However, since the ACE contains the `o` in the inheritance flags, user 3750 is *not* authorised to delete the directory **/pnfs/example.org/data/exampleDir**

Since the second ACE has both the `d` and `f` inheritance flags, it will be inherited by all files and subdirectories of **/pnfs/example.org/data/exampleDir**, but without the `o` flag. This authorises user 3750 to delete these items.

Subdirectories (and files) will inherit the second ACE with both `d` and `f` inheritance flags. This implies that all files and sub-subdirecties within a subdirectory of **/pnfs/example.org/data/exampleDir** will also inherit this ACE, so will also be deletable by user 3750.

VIEWING CONFIGURED ACLS  
-----------------------
The `getfacl` is used to obtain the current ACL for some item in dCache namespace. It takes the following arguments.  

getfacl [pnfsId] | [globalPath]    

The `getfacl` command fetches the ACL information of a namespace item (a file or directory). The item may be specified by its PNFS-ID or its absolute path.

Example 18.4.  

Obtain ACL information by absolute path  
       (PnfsManager) admin > getfacl /pnfs/example.org/data/exampleDir  
        ACL: rsId = 00004EEFE7E59A3441198E7EB744B0D8BA54, rsType = DIR  
        order = 0, type = A, accessMsk = lfsD, who = USER, whoID = 12457  
        order = 1, type = A, flags = f, accessMsk = lfd, who = USER, whoID = 87552  
        In extra format:  
        USER:12457:+lfsD  
        USER:87552:+lfd:f  

The information is provided twice. The first part gives detailed information about the ACL. The second part, after the `In extra format:` heading, provides a list of ACEs that may be used when updating the ACL using the `setfacl` command.

 <!--   [NFS version 4 Protocol specification]: http://www.nfsv4-editor.org/draft-25/draft-ietf-nfsv4-minorversion1-25.html
  [???]: #intouch-admin
  [Subject]: #ebnf.ace.subject
  [ACE EBNF]: #ebnf.ace
  [inheritance flags]: #ebnf.ace.inheritance
  [example\_title]: #cf-acl-eg-delete
--!>
