Migration of classic SE ( nfs, disk ) to dCache
===============================================

This chapter contains a guide to migrate a classic SE to dCache.

The convertion of a classic SE to dCache is not complicated, but has to be done very carefully to prevent data losses.

We assume, that dCache is installed and configured.( [???]). To be on the safe side, we recommend to install a new pool on a different host, since there is no easy way to switch back to classic SE.

-   create a new pool.

-   for each file in the classic SE an entry in PNFS has to be created. then the file has to be moved to data directory in the pool control directory and the owner, group and size must be set in PNFS. To avoid mistakes we recomend to use a script developed and tested by the dCache developers. Run the script for each file which goes into dCache:

        PROMPT-ROOTfind . -type f -exec file2dcache.sh {} /pnfs/desy.de/data/fromSE /pool/pool1 \; 

-   start the pool. Since the pool has to recreate the inventory, the start up time will be longer than usually..

-   connect to the dCache via admin interface and register the newly created files:

        cd pool1
              pnfs register
              ..
              logoff
              

The newly migrated files shall be available already.

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

  [???]: #in-install
