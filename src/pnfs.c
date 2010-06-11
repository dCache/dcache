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
 * $Id: pnfs.c,v 1.16 2006-01-09 14:28:15 tigran Exp $
 */

#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#ifndef WIN32
#    include <unistd.h>
#else
#include "dcap_win32.h"
#endif

#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "dcap_shared.h"

#ifdef WIN32
#    define PATH_SEPARATOR '\\'
#else
#    define PATH_SEPARATOR '/'
#endif /* WIN32 */

/*
 * isPnfs: checks for pnfs file system return value: 1: Pnfs 0: regular file
 * system -1: error
 *
 */

int
isPnfs(const char *path)
{

	char           *directory;
	char           *fname;
	char           *pnfsLayer;
	int             ds;


	if (!(fname = (char *) strrchr(path, PATH_SEPARATOR))) {
		directory = (char *) strdup(".");
		ds = 1;
	} else {

		ds = fname - path;
		directory = (char *) malloc(ds + 1);

		if (directory == NULL) {
			dc_errno = DEMALLOC;
			return -1;
		}
		strncpy(directory, path, ds);
		directory[ds] = '\0';
	}

	pnfsLayer = (char *) malloc(strlen("/.(get)(cursor)") + 1 + ds);

	if (pnfsLayer == NULL) {
		free(directory);
		dc_errno = DEMALLOC;
		return -1;
	}
	sprintf(pnfsLayer, "%s%c.(get)(cursor)", directory, PATH_SEPARATOR);
	free(directory);


	if (system_access(pnfsLayer, F_OK) < 0) {
		/* not pnfs */
		free(pnfsLayer);
		dc_errno = DENPNFS;
		return 0;
	}
	free(pnfsLayer);
	dc_errno = DEOK;
	return 1;
}


int
create_pnfs_entry(const char *path, mode_t mode)
{

	int             fd;
	mode_t          my_mode = 0600;

	if (mode)
		my_mode = mode;
	fd = system_open(path, O_CREAT, my_mode);

	if (fd < 0) {
		dc_errno = DEEPNFS;
		return fd;
	}
	fd = system_close(fd);
	if (fd < 0) {
		dc_errno = DEEPNFS;
		return fd;
	}
	dc_errno = DEOK;
	return 0;
}



int
getPnfsID(struct vsp_node * node)
{

	char           *pnfsLayer;
	int             tmp;
	struct          stat buf;

	pnfsLayer = (char *) malloc(strlen(node->file_name) + strlen("/.(id)()") + 1 + strlen(node->directory));

	if (pnfsLayer == NULL) {
		dc_errno = DEPNFSID;
		return -1;
	}

	sprintf(pnfsLayer, "%s%c.(id)(%s)", node->directory, PATH_SEPARATOR, node->file_name);

	dc_debug(DC_TRACE, "Looking for pnfsID in %s\n", pnfsLayer);
	tmp = system_open(pnfsLayer, O_RDONLY, 0);

	/* we does not need pnfsLayer anymore, so free it */
	free(pnfsLayer);

	if (tmp < 0) {
		dc_errno = DEPNFSID;
		return -1;
	}

	if(system_fstat(tmp, &buf) < 0) {
		system_close(tmp);
		dc_errno = DEPNFSID;
		return -1;

	}

	/* pNfs id always have '\n' at the end, which we does not needed */
	/* that is why we allocate one byte less memory */
	node->pnfsId = (char *) malloc(buf.st_size);

	if (!node->pnfsId) {
		system_close(tmp);
		dc_errno = DEPNFSID;
		return -1;
	}

	if(system_read(tmp, node->pnfsId, buf.st_size) != buf.st_size ) {
		system_close(tmp);
		free(node->pnfsId);
		node->pnfsId = NULL;
		dc_errno = DEPNFSID;
		return -1;
	}

	node->pnfsId[buf.st_size - 1] = '\0';

	system_close(tmp);

	dc_errno = DEOK;
	return 0;
}


char *
getPnfsIDbyPath(const char *path)
{

	char           *pnfsLayer;
	int             tmp;
	struct          stat buf;
	char *dir;
	char *file;
	char *pnfsId;

	dir = xdirname(path);
	file = xbasename(path);


	pnfsLayer = (char *) malloc(strlen(file) + strlen("/.(id)()") + 1 + strlen(dir));

	if (pnfsLayer == NULL) {
		dc_errno = DEPNFSID;
		return NULL;
	}

	sprintf(pnfsLayer, "%s%c.(id)(%s)", dir, PATH_SEPARATOR, file);
	free(dir);
	free(file);

	dc_debug(DC_TRACE, "Looking for pnfsID in %s\n", pnfsLayer);
	tmp = system_open(pnfsLayer, O_RDONLY, 0);

	/* we does not need pnfsLayer anymore, so free it */
	free(pnfsLayer);

	if (tmp < 0) {
		dc_errno = DEPNFSID;
		return NULL;
	}

	if(system_fstat(tmp, &buf) < 0) {
		system_close(tmp);
		dc_errno = DEPNFSID;
		return NULL;

	}

	/* pNfs id always have '\n' at the end, which we does not needed */
	/* that is why we allocate one byte less memory */
	pnfsId = (char *) malloc(buf.st_size);

	if (pnfsId == NULL) {
		system_close(tmp);
		dc_errno = DEPNFSID;
		return NULL;
	}

	if(system_read(tmp, pnfsId, buf.st_size) != buf.st_size ) {
		system_close(tmp);
		free(pnfsId);
		dc_errno = DEPNFSID;
		return NULL;
	}

	pnfsId[buf.st_size - 1] = '\0';

	system_close(tmp);

	dc_errno = DEOK;
	return pnfsId;
}
