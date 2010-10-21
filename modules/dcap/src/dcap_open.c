/*
 *   DCAP - dCache Access Protocol client interface
 *
 *   Copyright (C) 2000,2004 DESY Hamburg DMG-Division.
 *
 *   AUTHOR: Tigran Mkrtchayn (tigran.mkrtchyan@desy.de)
 *
 *   This program can be distributed under the terms of the GNU LGPL.
 *   See the file COPYING.LIB
 *
 */


/*
 * $Id: dcap_open.c,v 1.26 2007-07-06 16:09:08 tigran Exp $
 */

#include <sys/times.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stdarg.h>

#include "dcap.h"
#include "dcap_functions.h"
#include "dcap_checksum.h"
#include "dcap_mqueue.h"
#include "dcap_url.h"
#include "dcap_lcb.h"
#include "gettrace.h"
#include "links.h"
#include "node_plays.h"
#include "pnfs.h"
#include "debug_level.h"
#include "system_io.h"
#include "dcap_protocol.h"
#include "xutil.h"


#define DC_STAGE (O_RDONLY | O_NONBLOCK)

int
dc_open(const char *fname, int flags,...)
{

	struct vsp_node *node;
	char           *path;
	dcap_url       *url;
	va_list         args;
	mode_t          mode = 0;
	time_t          atime = 0;
	char           *location = NULL;
	int             isNew = 1; /* flag to monitor file creation */
#ifndef WIN32
	clock_t         rtime; /* timestamp */
	struct tms      dumm;
#else
	unsigned int    rtime;
#endif /* WIN32 */

	time_t          timestamp;
	char             *stamp;

	int             isTrunc = 0;
	int enableReadAhead;
	int enableLocalCache = 0;
	char *tmpName = NULL;
	int tmpIndex;
#ifdef WIN32
	struct _stati64 sbuf;
#else
	struct stat64 sbuf;
#endif

#ifdef DC_CALL_TRACE
	showTraceBack();
#endif


	/* nothing wrong ... yet */
	dc_errno = DEOK;
	errno = 0;

	if (fname == NULL) {
		errno = EFAULT;	/* path points to an illegal address. */
		dc_errno = DEEVAL;
		return -1;
	}

	if (flags & O_CREAT) {
		va_start(args, flags);
		mode = va_arg(args, mode_t);
		va_end(args);
		isTrunc = ( (flags & O_TRUNC) == O_TRUNC );
	}

	if(flags & DC_STAGE) {
		va_start(args, flags);
		atime = va_arg(args, time_t);
		location = va_arg(args, char* );
		va_end(args);
	}
	url = (dcap_url *)dc_getURL(fname);
	if(url != NULL) {
		path = strdup(url->file);
	}else{

		if(flags & O_CREAT) {
			path = strdup(fname);
		}else{
			path = followLink(fname);
		}

		if( path == NULL ) {
			/* errno set by other oprations */
			dc_errno = DEEVAL;
			return -1;
		}

		dc_debug(DC_INFO, "Real file name: %s.", path);

		if (!isPnfs(path)) {	/* if file is not on Pnfs, then nothing to do
								 * with Disk Cache */
				free(path);
				dc_debug(DC_INFO, "Using system native open for %s.", fname);
			    flags |= O_LARGEFILE ;
				return system_open(fname, flags, mode);
		}

		dc_debug(DC_INFO, "Using dCache open for %s.", path);
		if (system_access(path, F_OK) < 0) {	/* file not exist */
			if ((flags & O_RDONLY) || (flags == 0) || (flags == DC_STAGE) ) {
				/* trying to read non existing file */
				dc_debug(DC_ERROR, "Trying to read non existing file.");
				dc_errno = DENE;
				free(path);
				return -1;
			}
			/* we must create entry in pnfs */
			if (create_pnfs_entry(path, mode) < 0) {
				dc_debug(DC_ERROR, "Failed create entry in pNFS.");
				free(path);
				return -1;
			}
		} else {		/* file exist */
			isNew = 0;
			if( isTrunc ) {

				/*
				 * truncate triggered ; we need to check for file size. If file size == 0
				 * door will use this entry as a destination. As a result:
				 * we overwrite empty file and up to door setup to allow to overwrite
				 * existing non zero size files.
				*/


				if( (system_stat64(path, &sbuf) == 0) && (sbuf.st_size > 0) ) {

					tmpIndex = strlen(path);


					/* FIXME: we need a better way to create unic temporary file */
					tmpName = malloc(tmpIndex + 14); /* path + ';<uid>(6)_<pid>(6)' +'\0' */
					tmpName[0] = '\0';
#ifdef WIN32
					sprintf(tmpName, "%s;WINDCAP_%d", path, getpid() );
#else
					sprintf(tmpName, "%s;%d_%d", path, getuid(), getpid() );
#endif
					dc_debug(DC_INFO, "TRUNC: new file %s", tmpName );

					if (create_pnfs_entry(tmpName, mode) < 0) {
						dc_debug(DC_ERROR, "Failed create entry in pNFS.");
						free(path);
						free(tmpName);
						return -1;
					}

				}

			}
		}

	}

	if(path == NULL) {
		dc_debug(DC_ERROR, "Can not resolve path to %s.", fname);
		if(url != NULL) {
			free(url->file);
			free(url->host);
			free(url);
		}
		return -1;
	}


	node = new_vsp_node(tmpName == NULL ? path : tmpName );
	if (node == NULL) {
		dc_debug(DC_ERROR, "Failed to create new node.");
		free(path);
		if(url != NULL) {
			free(url->file);
			free(url->host);
			if( url->prefix != NULL ) free(url->prefix);
			free(url);
		}
		return -1;
	}
	/*
	 * dCache always has LARGE FILE SUPPORT,
	 * remove extra flag, while it makes trouble in ascii open
	 */
	node->flags =  flags & ~O_LARGEFILE;
	node->mode = mode;

	if(url == NULL) {
		if (getPnfsID(node) < 0) {
			if (flags & O_CREAT) {
				/* FIXME: node also should be deleted */
				unlink(path);
			}
			dc_debug(DC_ERROR, "Unable to get pNFS ID.");
			free(path);
			if( tmpName != NULL ) {
				unlink(tmpName);
				free(tmpName);
			}
			return -1;
		}
	}else{
		node->url = url;
		if(url->type == URL_PNFS) {
			node->pnfsId = (char *)strdup(url->file);
		}else{
			node->pnfsId = (char *)strdup(fname);
		}
	}

	node->asciiCommand = flags & DC_STAGE ?
			( atime < 0 ? DCAP_CMD_CHECK : DCAP_CMD_STAGE ) :
				((tmpName || ( ( url != NULL) && isTrunc ) ) ? DCAP_CMD_TRUNC : DCAP_CMD_OPEN ) ;

	if( tmpName != NULL ) {
		node->ipc = getPnfsIDbyPath( path );
	}

	if( ( url != NULL) && isTrunc ) {
		node->ipc = strdup( node->pnfsId );
	}

	node->atime = atime;
	node->stagelocation = location == NULL ? NULL : (char *) strdup(location);

	/* do some timng here */
#ifndef WIN32
	rtime = times(&dumm);
#else
	rtime = timeGetTime();
#endif /* WIN32 */

	time(&timestamp);

#ifdef _REENTRANT
	stamp = malloc(27);
#ifdef sun
	ctime_r(&timestamp, stamp, 26);
#else /* POSIX */
	ctime_r(&timestamp, stamp);
#endif	/* _sun */
#else
	stamp = strdup( ctime(&timestamp) );
#endif /* _REENTRANT */

	/* remove \n at the end of line */
	stamp[strlen(stamp) -1] = '\0';

	dc_debug(DC_TIME, "[%s] Going to open file %s in cache.", stamp, fname);
	free(stamp);
	/* open file in diks cache */
	if ( (cache_open(node) != 0) || (flags & DC_STAGE) ) {
		if ( (flags & O_CREAT) && isNew ) {
			if( node->url == NULL ) {
				unlink(path);
			}
		}

		free(path);

		if( tmpName != NULL ) {
			unlink(tmpName);
			free(tmpName);
		}

		/* node cleanup procedure */
		node_unplug( node );

		deleteQueue(node->queueID);
		node_destroy(node);

		if((flags & DC_STAGE) && ((dc_errno == DEOK) || (dc_errno == DENCACHED)) ) {
			errno = EAGAIN; /* indicate to user that stage in progress */
		}else{
			dc_debug(DC_ERROR, "Failed open file in the dCache.");
		}

		return -1;
	}

	/* to be fast on the read use read-ahead,  so enable it
	   the actual magick done in the dc_read */
	enableReadAhead =
#ifdef WIN32
		flags == O_BINARY ||
#endif
		flags == O_RDONLY;

	if(enableReadAhead && !(flags & DC_STAGE) &&
	   getenv("DC_LOCAL_CACHE_BUFFER") != NULL) {
			enableReadAhead = 0;
			enableLocalCache = 1;
			dc_debug(DC_INFO, "Switching off read-ahead; switching on local-cache buffer.");
	}


	if(enableReadAhead) {
			dc_debug(DC_INFO, "Switching on read ahead.");
			node->ahead = (ioBuffer *)malloc(sizeof(ioBuffer));
			if(node->ahead == NULL) {
				dc_debug(DC_ERROR, "Failed allocate memory for read-ahead, so skipping");
			}else{
				node->ahead->buffer = NULL;
				node->ahead->cur = 0;
				node->ahead->base = 0;
				node->ahead->used = 0;
				node->ahead->size = 0;
				node->ahead->isDirty = 0;
			}
	}


	if( flags & O_WRONLY ) {
		dc_debug(DC_INFO, "Enabling checksumming on write.");
		node->sum = (checkSum *)malloc( sizeof(checkSum) );
		if(node->sum == NULL) {
			dc_debug(DC_ERROR, "Failed to allocate memory for checksummiong, skipping");
		}else{
			node->sum->sum = initialSum(DCAP_DEFAULT_SUM);
			node->sum->type = DCAP_DEFAULT_SUM;
			node->sum->isOk = 1;
		}
	}



	if( tmpName != NULL ) {


		unlink ( path );

		free(node->file_name);
		node->file_name = strdup( xbasename(path) );

		rename(tmpName, path );
		dc_debug(DC_INFO, "moved %s to %s", tmpName, path );

		free(tmpName);
	}

	m_unlock(&node->mux);

	if( enableLocalCache) {

	  /* REVISIT: Gerd, I see the cache is allocated per open
	     file. It would be nice to have a shared cache for open
	     files. That would

	     a) limit total memory usage rather than allocating a huge
	        amount of memory for applications have many open
	        files.

	     b) improve utilization of the memory by providing LRU
	        semantics across all open files rather than per
	        file. */
	  dc_lcb_init( node );
	}

	dc_debug(DC_TIME, "Cache open succeeded in %2.4fs.",
#ifndef WIN32
	(double)(times(&dumm) - rtime)/(double)sysconf(_SC_CLK_TCK ));
#else
	(double)(timeGetTime() - rtime)/(double)1000);
#endif /* WIN32 */

	free(path);
	return node->dataFd;
}

int
dc_creat(const char *path, mode_t mode)
{
	return dc_open(path, O_WRONLY | O_CREAT | O_TRUNC, mode);
}

int
dc_stage(const char *path, time_t atime, const char *location)
{
	int rc = -1;

	dc_open(path, DC_STAGE, atime, location);
	if( (errno == EAGAIN) && (dc_errno == DEOK) ) {
		errno = 0;
		rc = 0;
	}

	return rc;
}

int
dc_check(const char *path, const char *location)
{
	return dc_stage(path, -1, location);
}
