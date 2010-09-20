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
 * $Id: dcap_fsync.c,v 1.5 2004-11-01 19:33:29 tigran Exp $
 */

#include "dcap.h"
#include "dcap_fsync.h"
#include "dcap_write.h"
#include "gettrace.h"
#include "node_plays.h"
#include "system_io.h"
#include "debug_level.h"

int dc_fsync(int fd)
{

	struct vsp_node *node;
	int rc;

#ifdef DC_CALL_TRACE
	showTraceBack();
#endif

	node = get_vsp_node(fd);
	if( node == NULL ) {
#ifdef WIN32
		return _commit(fd);
#else
		return system_fsync(fd);
#endif /* WIN32 */
	}

	rc = dc_real_fsync(node);

	m_unlock(&node->mux);
	return rc;
}



int dc_real_fsync( struct vsp_node *node)
{


	int rc = 0;


	if(  (node->ahead != NULL) && ( node->ahead->buffer != NULL ) && node->ahead->isDirty ) {

		dc_debug(DC_IO, "[%d] Syncing %ld bytes.", node->dataFd, node->ahead->used);

		if( dc_real_write( node, NULL, 0) < 0 ) {
			rc = -1;
		}
	}

	return rc;
}




