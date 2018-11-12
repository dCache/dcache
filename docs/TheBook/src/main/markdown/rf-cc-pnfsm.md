PnfsManager Commands
======================

pnfsidof
--------
pnfsidof — Print the pnfs id of a file given by its global path.

synopsis
--------
pnfsidof <globalPath> 

Description
-----------
Print the PNFS id of a file given by its global path. The global path always starts with the “VirtualGlobalPath” as given by the “`info`”-command.


flags remove
-------------

flags remove - Remove a flag from a file.

synopsis
---------
flags remove <pnfsId> <key> ... 

Arguments
pnfsIs

The PNFS id of the file of which a flag will be removed.

key  
flags which will be removed.

Description
-----------

flags ls
---------

flags ls - List the flags of a file.

synopsis
---------
flags ls

flags ls <pnfsId> 

pnfsId  
The PNFS id of the file of which a flag will be listed.

Description
-----------

flags set
----------

flags set <pnfsId> <key>=<value> ... 

Arguments

pnfsId

pnfsId  

The PNFS id of the file of which flags will be set.

key  
The flag which will be set.

value  
The value to which the flag will be set.

Description
-----------

metadataof
-----------

metadataof - Print the meta-data of a file.

synopsis
---------
metadataof [ <pnfsId> ] | [ <globalPath> ] [-v] [-n] [-se]

Arguments

pnfsId  
The PNFS id of the file.

globalPath  
The global path of the file.

Description
-----------

pathfinder
-----------

pathfinder - Print the global or local path of a file from its PNFS id.

synopsis
---------
pathfinder <pnfsId> [[-global] | [-local]]

Arguments

pnfsId  
The PNFS Id of the file.

-global  
Print the global path of the file.

-local  
Print the local path of the file.

Description
-----------

set meta
--------

set meta - Set the meta-data of a file.

synopsis
---------
set meta [<pnfsId>] | [<globalPath>] <uid> <gid> <perm> <levelInfo>... 

Arguments

pnfsId  
The PNFS id of the file.

globalPath  
The global path oa the file.

uid  
The user id of the new owner of the file.

gid  
The new group id of the file.

perm  
The new file permitions.

levelInfo  
The new level information of the file.

Description
-----------

storageinfoof
--------------

storageinfoof - Print the storage info of a file.

synopsis
---------
storageinfoof [<pnfsId>] | [<globalPath>] [-v] [-n] [-se]

Arguments

pnfsId  
The PNFS id of the file.

globalPath  
The global path oa the file.

Description
-----------

cacheinfoof
------------

cacheinfoof - Print the cache info of a file.

synopsis
----------
cacheinfoof [<pnfsId>] | [<globalPath>] 

Arguments
pnfsId

The PNFS id of the file.

globalPath  
The global path oa the file.

Description
-----------
