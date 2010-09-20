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
 * $Id: dcap_unlink.c,v 1.4 2004-11-01 19:33:29 tigran Exp $
 */

#include <stdlib.h>
#include <string.h>

#include "dcap.h"
#include "dcap_types.h"
#include "dcap_functions.h"
#include "dcap_mqueue.h"
#include "dcap_url.h"
#include "node_plays.h"
#include "debug_level.h"
#include "dcap_protocol.h"
#include "system_io.h"

 int dc_unlink( const char *path)
 {


	dcap_url *url;
	struct vsp_node *node;
	int rc;


	url = (dcap_url *)dc_getURL(path);
 	if( url == NULL ) {
		dc_debug(DC_INFO, "Using system native unlink for %s.", path);
		return system_unlink(path);

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
	node->asciiCommand = DCAP_CMD_UNLINK;

	rc = cache_open(node);

	/* node cleanup procedure */
	node_unplug( node );

	deleteQueue(node->queueID);
	node_destroy(node);

	return rc;

}


int dc_rmdir( const char *path)
 {


	dcap_url *url;
	struct vsp_node *node;
	int rc;


	url = (dcap_url *)dc_getURL(path);
 	if( url == NULL ) {
		dc_debug(DC_INFO, "Using system native rmdir for %s.", path);
		return system_rmdir(path);

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
	node->asciiCommand = DCAP_CMD_RMDIR;

	rc = cache_open(node);

	/* node cleanup procedure */
	node_unplug( node );

	deleteQueue(node->queueID);
	node_destroy(node);

	return rc;

}
