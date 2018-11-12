Chapter 28. Common Cell Commands
=================================

Table of Contents
------------------
+ [Common Cell Commands](rf-cc-common.md)    
[pin](rf-cc-common.md#pin) — Adds a comment to the pinboard.  
[info](rf-cc-common.md#info) — Print info about the cell.  
[dump pinboard](rf-cc-common.md#dump-inboard) — Dump the full pinboard of the cell to a file.  
[show pinboard](rf-cc-common.md#show-pinboard) — Print a part of the pinboard of the cell to STDOUT.   

+ [PnfsManager Commands](rf-cc-pnfsm.md)     
[pnfsidof](rf-cc-pnfsm.md#pnfsidof)  — Print the pnfs id of a file given by its global path.   
[flags remove](rf-cc-pnfsm.md#flags-remove)  — Remove a flag from a file.   
[flags ls](rf-cc-pnfsm.md#flags-ls)  — List the flags of a file.   
[flags set](rf-cc-pnfsm.md#flags-set) — Set a flag for a file.   
[metadataof](rf-cc-pnfsm.md#metadata-of)  — Print the meta-data of a file.   
[pathfinder](rf-cc-pnfsm.md#pathfinder)  — Print the global or local path of a file from its PNFS id.    
[set meta](rf-cc-pnfsm.md#set-meta)  — Set the meta-data of a file.  
[storageinfoof](rf-cc-pnfsm.md#storageinfoof)  — Print the storage info of a file.     
[cacheinfoof](rf-cc-pnfsm.md#cacheinfoof)  — Print the cache info of a file. 

+ [Pool Commands](rf-cc-pool.md)
[rep ls](rf-cc-pool.md#rep-ls) — List the files currently in the repository of the pool.  
[st set max active](rf-cc-pool.md#st-set-max-active) — Set the maximum number of active store transfers.    
[rh set max active](rf-cc-pool.md#rh-set-max-active) — Set the maximum number of active restore transfers.      
[mover set max active](rf-cc-pool.md#mover-set-max-active) — Set the maximum number of active client transfers.     
[mover set max active -queue=p2p](rf-cc-pool.md#mover-set-max-active-queuep2p) — Set the maximum number of active pool-to-pool server transfers.  
[pp set max active](rf-cc-pool.md#pp-set-max-active) — Set the value used for scaling the performance cost of pool-to-pool client transfers analogous to the other  
[set gap](rf-cc-pool.md#set-gap)    — Set the gap parameter - the size of free space below which it will be assumed that the pool is full within the cost calculations.     
[set breakeven](rf-cc-pool.md#set-breakeven)   — Set the breakeven parameter - used within the cost calculations.  
[mover ls](rf-cc-pool.md#mover-ls)    — List the active and waiting client transfer requests.    
[migration cache](rf-cc-pool.md#migration-cache)    — Caches replicas on other pools.  
[migration cancel](rf-cc-pool.md#migration-cancel)    — Cancels a migration job    
[migration clear](rf-cc-pool.md#migration-clear)    — Removes completed migration jobs.    
[migration concurrency](rf-cc-pool.md#migration-concurrency)   — Adjusts the concurrency of a job.     
[migration copy](rf-cc-pool.md#migration-copy)    — Copies files to other pools.    
[migration info](rf-cc-pool.md#migration-info)   — Shows detailed information about a migration job.    
[migration ls](rf-cc-pool.md#migration-ls)   — Lists all migration jobs.       
[migration move](rf-cc-pool.md#migration-move)   — Moves replicas to other pools.   
[migration suspend](rf-cc-pool.md#migration-suspend)   — Suspends a migration job.  
[migration resume](rf-cc-pool.md#migration-resume)   — Resumes a suspended migration job.  

+ [PoolManager Commands](rf-cc-pm.md)
[rc ls](rf-cc-pm.md#rc-ls) — List the requests currently handled by the PoolManager  
[cm ls](rf-cc-pm.md#cm-ls) — List information about the pools in the cost module cache.   
[set pool decision](rf-cc-pm.md#set-pool-decision) — Set the factors for the calculation of the total costs of the pools. 


pin
-----

pin - Adds a comment to the pinboard.   

synopsis
---------

pin <comment>

Arguments

comment

    A string which is added to the pinboard. 

Description
-----------

info
-----

info -Print info about the cell.

info [-a] [-l]

Arguments

-a

    Display more information. 
-l

    Display long information. 

Description
-----------

The info printed by `info` depends on the cell class.

dump pinboard
--------------

dump pinboard - Dump the full pinboard of the cell to a file.

synopsis
---------
dump pinboard <filename>

Arguments

filename

    The file the current content of the pinboard is stored in. 
The file the current content of the pinboard is stored in.

Description
-----------

show pinboard
--------------

show pinboard - Print a part of the pinboard of the cell to STDOUT.

show pinboard [ <lines> ]

Arguments

lines

    The number of lines which are displayed. Default: all.

Description
-----------
