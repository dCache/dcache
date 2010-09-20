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
 * $Id: dcap_chown.c,v 1.1 2006-03-28 11:42:49 tigran Exp $
 */

#include <stdlib.h>
#include <string.h>

#include "dcap.h"
#include "dcap_functions.h"
#include "dcap_protocol.h"
#include "dcap_url.h"
#include "node_plays.h"
#include "debug_level.h"
#include "dcap_mqueue.h"
#include "system_io.h"


int dc_chown( const char *path, uid_t uid, gid_t gid)
 {


	dcap_url *url;
	struct vsp_node *node;
	int rc;

	url = (dcap_url *)dc_getURL(path);
 	if( url == NULL ) {
		dc_debug(DC_INFO, "Using system native chown for %s.", path);
		return system_chown(path, uid, gid);

	}

 	node = new_vsp_node( path );
	if( node == NULL ) {
		free(url->file);
		free(url->host);
		if( url->prefix != NULL ) free(url->prefix);
		free(url);
		return 1;
	}

	node->url = url;
	if (url->type == URL_PNFS) {
		node->pnfsId = (char *)strdup(url->file);
	}else{
		node->pnfsId = (char *)strdup(path);
	}
	node->asciiCommand = DCAP_CMD_CHOWN;

	node->uid = uid;
	node->gid = gid;
	rc = cache_open(node);

	/* node cleanup procedure */
	node_unplug( node );

	deleteQueue(node->queueID);
	node_destroy(node);

	return rc;

}
