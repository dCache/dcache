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
 * $Id: dcap_stat.c,v 1.22 2007-02-22 10:28:32 tigran Exp $
 */

#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include "dcap.h"
#include "dcap_functions.h"
#include "dcap_lseek.h"
#include "dcap_mqueue.h"
#include "dcap_url.h"
#include "gettrace.h"
#include "node_plays.h"
#include "pnfs.h"
#include "debug_level.h"
#include "dcap_protocol.h"
#include "system_io.h"

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

/*
   to support large files on current pnfs ( nfs v2 ), implementes small
   workaround. If file size is 1, than perform a stat operation over
   control line to get a real file size ( wich located in pnfs level 2 layer.
*/

static char * getNodePath(struct vsp_node *node);

/* FIXME: stat64to32 is duplicated in system_io.c */
#ifdef WIN32
static void stat64to32(struct stat *st32, const struct _stati64 *st64)
#else
static void stat64to32(struct stat *st32, const struct stat64 *st64)
#endif
{
	memset(st32, 0, sizeof(struct stat) );

	st32->st_dev     = st64->st_dev;
	st32->st_ino     = st64->st_ino;
	st32->st_mode    = st64->st_mode;
	st32->st_nlink   = st64->st_nlink;
	st32->st_uid     = st64->st_uid;
	st32->st_gid     = st64->st_gid;
	st32->st_rdev    = st64->st_rdev;
	st32->st_size    = (off_t) st64->st_size;
#ifndef WIN32
	st32->st_blksize = st64->st_blksize;
	st32->st_blocks  = (blkcnt_t)st64->st_blocks;
#endif
	st32->st_atime   = st64->st_atime;
	st32->st_mtime   = st64->st_mtime;
	st32->st_ctime   = st64->st_ctime;

}


char * getNodePath(struct vsp_node *node)
{

	char *path;

	if( node == NULL ) {
		return NULL;
	}

	path = malloc( PATH_MAX + 1);
	if(path == NULL ) {
		return NULL;
	}

	path[PATH_MAX] = '\0';

	if( node->url != NULL ) {
		if( node->url->prefix != NULL ) {
			sprintf(path, "%s%s://%s/%s", node->url->prefix , node->url->type == 1 ? "dcap" : "pnfs" , node->url->host, node->url->file);
		}else{
			sprintf(path, "%s://%s/%s", node->url->type == 1 ? "dcap" : "pnfs" , node->url->host, node->url->file);
		}
	}else{
		sprintf(path, "%s/%s", node->directory, node->file_name);
	}

	return path;
}


int
dc_stat(const char *path, struct stat *buf)
{
	int rc;
#ifdef WIN32
	struct _stati64 buf64;
#else
	struct stat64 buf64;
#endif

#ifdef WIN32
	memset(&buf64, 0, sizeof(struct _stati64) );
#else
	memset(&buf64, 0, sizeof(struct stat64) );
#endif

	rc = dc_stat64( path, &buf64);
	if( rc == 0 ) {
		stat64to32(buf, &buf64);
	}

	return rc;

}


int
dc_lstat(const char *path, struct stat *buf)
{
	int rc;
#ifdef WIN32
	struct _stati64 buf64;
#else
	struct stat64 buf64;
#endif

#ifdef WIN32
	memset(&buf64, 0, sizeof(struct _stati64) );
#else
	memset(&buf64, 0, sizeof(struct stat64) );
#endif

	rc = dc_lstat64( path, &buf64);
	if( rc == 0 ) {
		stat64to32(buf, &buf64);
	}

	return rc;
}

int
dc_fstat(int fd, struct stat *buf)
{
	int rc;
#ifdef WIN32
	struct _stati64 buf64;
#else
	struct stat64 buf64;
#endif

#ifdef WIN32
	memset(&buf64, 0, sizeof(struct _stati64) );
#else
	memset(&buf64, 0, sizeof(struct stat64) );
#endif

	rc = dc_fstat64( fd, &buf64);
	if( rc == 0 ) {
		stat64to32(buf, &buf64);
	}

	return rc;
}

#ifdef WIN32
int dc_stat64(const char *path, struct _stati64 *buf)
#else
int dc_stat64(const char *path, struct stat64 *buf)
#endif
{
	dcap_url *url;
	struct vsp_node *node;
#ifdef WIN32
	struct _stati64 *s;
#else
	struct stat64 *s;
#endif
	int rc;
	int old_errno;


#ifdef DC_CALL_TRACE
	showTraceBack();
#endif

	/* nothing wrong ... yet */
	dc_errno = DEOK;

	url = (dcap_url *)dc_getURL(path);


	/* let system to do the work where it's possible */
	if(url == NULL) {
		dc_debug(DC_INFO, "Using system native stat64 for %s.", path);
		rc = system_stat64(path, buf);
		/* isPnfs overwrite errno */
		old_errno = errno;
		if( (buf->st_size != 1) ||  !isPnfs(path) ) {
			errno = old_errno;
			return rc;
		}
	}

	node = new_vsp_node(path);
	if (node == NULL) {
		dc_debug(DC_ERROR, "dc_stat: Failed to create new node.");
		free(url->file);
		free(url->host);
		free(url);
		return -1;
	}

	node->url = url;
	if (url == NULL ) {
		getPnfsID(node);
	}else{
		if (url->type == URL_PNFS) {
			node->pnfsId = (char *)strdup(url->file);
		}else{
			node->pnfsId = (char *)strdup(path);
		}
	}

	node->asciiCommand = DCAP_CMD_STAT;

	rc = cache_open(node);

	if(node->ipc != NULL) {

#ifdef WIN32
		s = (struct _stati64*)node->ipc;
		memcpy(buf, s, sizeof(struct _stati64) );
#else
		s = (struct stat64*)node->ipc;
		memcpy(buf, s, sizeof(struct stat64) );
#endif
		free(node->ipc);
		node->ipc = NULL;
	}

	/* node cleanup procedure */
	node_unplug( node );

	deleteQueue(node->queueID);
	node_destroy(node);

	if( rc != 0 ) {
		errno = ENOENT;
	}
	return rc;
}

#ifdef WIN32
int dc_lstat64(const char *path, struct _stati64 *buf)
#else
int dc_lstat64(const char *path, struct stat64 *buf)
#endif
{
	dcap_url *url;
	struct vsp_node *node;
#ifdef WIN32
	struct _stati64 *s;
#else
	struct stat64 *s;
#endif
	int rc;
	int old_errno;

#ifdef DC_CALL_TRACE
	showTraceBack();
#endif

	/* nothing wrong ... yet */
	dc_errno = DEOK;

	url = (dcap_url *)dc_getURL(path);

	/* let system to do the work where it's possible */
	if(url == NULL) {
		dc_debug(DC_INFO, "Using system native lstat64 for %s.", path);
		rc = system_lstat64(path, buf);
		/* isPnfs overwrite errno */
		old_errno = errno;
		if( (buf->st_size != 1) ||  !isPnfs(path) ) {
			errno = old_errno;
			return rc;
		}
	}

	node = new_vsp_node(path);
	if (node == NULL) {
		dc_debug(DC_ERROR, "dc_stat: Failed to create new node.");
		free(url->file);
		free(url->host);
		free(url);
		return -1;
	}

	node->url = url;
	if (url == NULL ) {
		getPnfsID(node);
	}else{
		if (url->type == URL_PNFS) {
			node->pnfsId = (char *)strdup(url->file);
		}else{
			node->pnfsId = (char *)strdup(path);
		}
	}

	node->asciiCommand = DCAP_CMD_LSTAT;

	rc = cache_open(node);

	if(node->ipc != NULL) {
#ifdef WIN32
		s = (struct _stati64*)node->ipc;
		memcpy(buf, s, sizeof(struct _stati64) );
#else
		s = (struct stat64*)node->ipc;
		memcpy(buf, s, sizeof(struct stat64) );
#endif
		free(node->ipc);
		node->ipc = NULL;
	}

	/* node cleanup procedure */
	node_unplug( node );

	deleteQueue(node->queueID);
	node_destroy(node);

	if( rc != 0 ) {
		errno = ENOENT;
	}
	return rc;
}

#ifdef WIN32
int dc_fstat64(int fd, struct _stati64 *buf)
#else
int dc_fstat64(int fd, struct stat64 *buf)
#endif
{

	struct vsp_node *node;
	int rc;
	char *path;
	off64_t size;

#ifdef DC_CALL_TRACE
	showTraceBack();
#endif

	node = get_vsp_node( fd );
	if( node == NULL ) {
		dc_debug(DC_INFO, "Using system native fstat64 for %d.", fd);
		return system_fstat64(fd, buf);
	}

	/* pnfs can not show file size if file opened for write and not closed */
	if( node->flags & O_WRONLY ) {
		size = dc_real_lseek( node, 0, SEEK_CUR );
	}
	path = getNodePath(node);

	m_unlock(&node->mux);
	rc = dc_stat64( (const char *) path , buf);
	free(path);

	if( node->flags & O_WRONLY ) {
		buf->st_size = size;
	}

	return rc;
}
