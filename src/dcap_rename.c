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
 * $Id: dcap_rename.c,v 1.2 2005-04-25 07:56:37 tigran Exp $
 */
#include <stdlib.h>
#include <errno.h>
#include <string.h>

#include "dcap.h"
#include "dcap_functions.h"
#include "dcap_mqueue.h"
#include "dcap_url.h"
#include "gettrace.h"
#include "node_plays.h"
#include "pnfs.h"
#include "debug_level.h"
#include "system_io.h"
#include "dcap_protocol.h"

int dc_rename( const char *oldPath, const char *newPath )
{

	struct vsp_node *node;
	dcap_url *url;
	int rc;

#ifdef DC_CALL_TRACE
	showTraceBack();
#endif

	/* nothing wrong ... yet */
	dc_errno = DEOK;
	errno = 0;

	url = (dcap_url *)dc_getURL(oldPath);

	if( url == NULL ) {
		dc_debug(DC_INFO, "Using system native rename for %s to %s.", oldPath, newPath);
		return system_rename(oldPath, newPath);
	}



	node = new_vsp_node(oldPath);
	if (node == NULL) {
		dc_debug(DC_ERROR, "dc_rename: Failed to create new node.");
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
			node->pnfsId = (char *)strdup(oldPath);
		}
	}

	node->asciiCommand = DCAP_CMD_RENAME;
	node->ipc = (char *)newPath;
	rc = cache_open(node);

	/* node cleanup procedure */
	node_unplug( node );

	deleteQueue(node->queueID);
	node_destroy(node);

	return rc;

}
