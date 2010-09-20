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
 * $Id: dcap_checksum.c,v 1.4 2004-11-01 19:33:29 tigran Exp $
 */

#include <zlib.h>
#include <stdlib.h>

#include "dcap.h"
#include "dcap_checksum.h"
#include "node_plays.h"
#include "debug_level.h"

/*
 * reserved for future
 */
#if 0
typedef struct {
	int sumType;
	unsigned long initialValue;
} sumInitialValueTable;


static sumInitialValueTable csm[] = {
	{ 1, 1}, /* ADLER32 */
	{ -1, 0}
};
#endif


void update_checkSum(checkSum *sum, unsigned char *buf, size_t len)
{
	sum->sum =  adler32(sum->sum, buf, len);
}


unsigned long initialSum( int sumType )
{

	/*
	 *  in reality, we have to look for proper check sum type,
	 *  but up to now, we use only one alogorithm - adler32.
	 */

	return 1L;

}


void dc_noCheckSum( int fd )
{
	struct vsp_node *node;

	node = get_vsp_node(fd);
	if (node == NULL) {
		return ;
	}

	if( node->sum != NULL ) {
		free( node->sum );
		node->sum = NULL;
		dc_debug(DC_INFO, "[%d] Checksum calculation disabled.", node->dataFd );
	}

	m_unlock(&node->mux);

	return;
}
