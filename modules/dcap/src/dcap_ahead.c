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
 * $Id: dcap_ahead.c,v 1.18 2004-11-01 19:33:29 tigran Exp $
 */


#include "dcap_types.h"
#include "dcap_debug.h"
#include "dcap_nodes.h"
#include "dcap_reconnect.h"
#include "sysdep.h"
#include <stdlib.h>
#include <string.h>
#ifdef WIN32
#    include "dcap_win32.h"
#    include <stdio.h> /* for SEEK_X */
#else
#    include <unistd.h>
#endif

extern int dc_set_pos(struct vsp_node *, int, int64_t);

void dc_noBuffering(int fd)
{
	struct vsp_node *node;
	node = get_vsp_node(fd);

	if(node == NULL) return;
	if(node->ahead != NULL) {
		/* check for DCACHE_RAHEAD */
		if(getenv("DCACHE_RAHEAD") != NULL ) {
			dc_debug(DC_INFO, "Read ahead disabling skipped for fd = %d.", fd);
		}else{
			free(node->ahead);
			node->ahead=NULL;
			dc_debug(DC_INFO, "No Read ahead for fd = %d.", fd);
		}
	}

	m_unlock(&node->mux);
}

void dc_setNodeBufferSize(struct vsp_node *node, size_t newSize)
{

	char *tmpBuffer;

	if(node == NULL) return;

	if( node->ahead == NULL ) {

		node->ahead = (ioBuffer *)malloc( sizeof(ioBuffer) );

		if(node->ahead == NULL) {
			dc_debug(DC_ERROR, "Failed allocate memory for read-ahead, so skipping");
		}else{
			node->ahead->buffer = NULL;
			node->ahead->cur  = 0;
			node->ahead->base = 0;
			node->ahead->used = 0;
			node->ahead->size = 0;
			node->ahead->isDirty = 0;
		}

	}


	if(node->ahead != NULL) {

		if( node->ahead->buffer == NULL ) {

			/* allocating the buffer first time */

			dc_debug(DC_INFO, "[%d] Allocating %d bytes as Read-ahead buffer.", node->dataFd, newSize);
			node->ahead->buffer = (char *)malloc(newSize);

			if( node->ahead->buffer == NULL ) {
				dc_debug(DC_ERROR, "[%d] Failed to allocate %ld bytes for Read-ahead buffer.", node->dataFd, newSize);
			}else{
				node->ahead->size = newSize;
				node->ahead->used = 0;
				node->ahead->cur = 0;
				node->ahead->isDirty = 0;
			}
		}else{
			/* changing the buffer size */

			if( newSize == node->ahead->size) {
				/* no changes are needed */
				return;
			}

			dc_debug(DC_INFO, "[%d] Changing Read-ahead buffer size from %ld to %ld.",
 				node->dataFd, node->ahead->size, newSize);

			tmpBuffer = (char *)realloc(node->ahead->buffer, newSize);
			if( tmpBuffer == NULL ) {
				dc_debug(DC_INFO, "[%d] Failed to change read-ahead buffer size.", node->queueID);
				return;
			}


			node->ahead->buffer = tmpBuffer;

			if( newSize < node->ahead->size ) {

				/* if newSize < oldSize, make a corrections
					of current position and buffer used size.
					NOTE, that if used < newSize, no changes needed for
					current position as it can not be bigger than used. */

				if(node->ahead->used > newSize) {

					node->seek = node->ahead->base + newSize;
					node->whence = SEEK_SET;

					dc_set_pos(node, DCFT_POSITION, -1);

					node->ahead->used = newSize;

					if(node->ahead->cur > newSize) {
						node->ahead->cur = newSize;
					}
				}
			}

			node->ahead->size = newSize;
		}

    }

   return;

}

void dc_setBufferSize(int fd, size_t newSize)
{

    struct vsp_node *node;

    node = get_vsp_node(fd);

	if( node == NULL ){
		return;
	}

	dc_setNodeBufferSize(node, newSize);

	m_unlock(&node->mux);

}
