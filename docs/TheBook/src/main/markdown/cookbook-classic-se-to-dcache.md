Migration of classic SE ( nfs, disk ) to dCache
===============================================

This chapter contains a guide how-to migrate a classic SE to dCache without
coping the data.

The conversion of a classic SE is straightforward, but has to be done carefully to prevent data losses.

We assume, that dCache is [installed](install.md), configured and NFS mounted on the classic SE node.

>`NOTICE`: There is no easy way to switch back to classic SE once migrated.

- create a new pool, See [Creating and configuring pools](install.md#creating-and-configuring-pools) for details.

- for each file in the classic SE an entry in the mounted dCache has to be created. The owner, group and size must be adjusted. Then the file has to be moved to pools' data directory. To avoid mistakes we recommend to use a script developed and tested by the dCache developers. Run the script for each file which goes into dCache:

```sh
find . -type f -exec file2dcache.sh {} /dcache/data/fromSE /pool/pool1 \;
```

- start the pool. Since the pool has to recreate the inventory, the start up time will be longer than usually..

- connect to the dCache via admin interface and register the newly created files:

```
[dcache] (local) admin > \c pool1
[dcache] (pool1@poolDomain) admin > pnfs register
....
[dcache] (pool1@poolDomain) admin > \q
```

After a while, a newly migrated files shall be available and already to be accessed.

```sh
#!/bin/sh

if [ $# -ne 3 ]
then
   echo "Usage: $0 <file> <pnfs path> <pool base>"
   exit 1;
fi

SRC=$1
FILE=`basename $1`
DIR=`dirname $1`
PNFS_PRFIX=$2
POOL_BASE=$3

PNFS_FILE="${PNFS_PRFIX}/${DIR}/${FILE}"

if [ ! -f "${SRC}" ]
then
   echo "File ${SRC} do not exist."
   exit 1
fi

if [  -f "${PNFS_FILE}" ]
then
   echo "File ${PNFS_FILE}  already exist."
   exit 2
fi

if [ ! -d "${POOL_BASE}/control" ]
then
   echo "Creating directory [control]"
   mkdir ${POOL_BASE}/control
fi

if [ ! -d "${POOL_BASE}/data" ]
then
   echo "Creating directory [data]"
   mkdir ${POOL_BASE}/data
fi

if [ ! -f "setup" ]
then
   echo "Creating dummy [setup] file"
   touch  ${POOL_BASE}/setup
fi

echo "Creating file in pnfs"
if [ ! -d ${PNFS_PRFIX}/${DIR} ]
then
   mkdir -p ${PNFS_PRFIX}/${DIR} > /dev/null 2>&1
   if [ $? -ne 0 ]
   then
      echo "Failed to create directory ${PNFS_PRFIX}/${DIR}"
      exit 3;
   fi
fi

touch ${PNFS_FILE}
FILE_SIZE=`stat -c "%s" ${SRC}`
touch "${PNFS_PRFIX}/${DIR}/.(fset)(${FILE})(size)(${FILE_SIZE})"
chmod --reference=${SRC} ${PNFS_FILE}
chown --reference=${SRC} ${PNFS_FILE}

echo "Creating control file for pnfsID $PNFS_ID"
PNFS_ID=`cat "${PNFS_PRFIX}/${DIR}/.(id)(${FILE})"`
echo "precious" >  ${POOL_BASE}/control/${PNFS_ID}
echo "Copy ${SRC} to  ${POOL_BASE}/data/${PNFS_ID}"
cp  ${SRC}  ${POOL_BASE}/data/${PNFS_ID}

exit 0
```
