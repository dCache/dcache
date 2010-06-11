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
 * $Id: dcap_dup.c,v 1.4 2004-11-01 19:33:29 tigran Exp $
 */

#include "dcap_shared.h"


int dc_dup(int fd)
{

	struct vsp_node *node;
	int ret;

#ifdef DC_CALL_TRACE
	showTraceBack();
#endif

	node = get_vsp_node(fd);

	if( node == NULL ) {
		dc_debug(DC_INFO, "System native dup for [%d]", fd);
		return system_dup(fd);
	}


	ret = system_dup(fd);

	if(ret > 0 ) {
		node_attach_fd( node, ret );
		dc_debug(DC_INFO, "dc_dup: [%d](original) duplicated to [%d]", fd, ret);
	}else{
		dc_debug(DC_ERROR, "dc_dup: system dup failed for [%d]", fd);
	}

	m_unlock(&node->mux);
	return ret;


}
