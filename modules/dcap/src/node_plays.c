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
 * $Id: node_plays.c,v 1.34 2006-09-22 13:25:46 tigran Exp $
 */

#include <stdlib.h>
#include <string.h>

#include "dcap.h"
#include "dcap_types.h"
#include "debug_level.h"
#include "xutil.h"

#ifdef WIN32
#    define PATH_SEPARATOR '\\'
#else
#    define PATH_SEPARATOR '/'
#endif /* WIN32 */

static RDLOCK(nodeRWlock);


/* Local function prototypes */
static void real_node_unplug( struct vsp_node *);
static int node_init(struct vsp_node *node, const char *path);
static void real_node_unplug( struct vsp_node *node);


static struct vsp_node *vspNode = NULL;
static struct vsp_node *lastNode = NULL;



void node_dupToAll(struct vsp_node *node, int fd)
{
	unsigned int i;

	for( i = 0; i < node->reference; i++) {
		if( node->fd_set[i] != fd ) {
			node->fd_set[i] = dup2(fd, node->fd_set[i] );
		}
	}
}

void node_attach_fd( struct vsp_node *node, int fd)
{
	node->fd_set[node->reference] = fd;
	++(node->reference);
	node->dataFd = fd;
}

void node_detach_fd( struct vsp_node *node, int fd)
{


	unsigned int i;

	for( i = 0; i < node->reference; i++) {

		if( node->fd_set[i] == fd ) {
			--node->reference;

			if( node->reference != 0 ) {
				node->fd_set[i] = node->fd_set[node->reference];
			}

			node->dataFd = fd;
		}

	}


}


int node_init(struct vsp_node *node, const char *path)
{
	node->next = NULL;
	node->pnfsId = NULL;
	node->ahead = NULL;
	node->fd = -1;
	node->dataFd = -1;

	node->asciiCommand = 0;

	node->pos = 0;
	node->whence = -1;
	node->seek = 0;

	node->stagelocation = NULL;

	node->unsafeWrite = getenv("DCACHE_USE_UNSAFE") != NULL ? 1 : 0;
	node->url = NULL;
	node->tunnel = NULL;
	node->sndBuf = 0;
	node->rcvBuf = 0;
	node->ipc = NULL;

	node->uid = -1;
	node->gid = -1;

	node->reference = 0;

	node->sum = NULL;

	node->file_name = xbasename(path);
	node->directory = xdirname(path);

	node->isPassive = 0;

	return 0;

}

struct vsp_node *
new_vsp_node(const char *path)
{

	struct vsp_node *node;

	/* allocate and clear memory */
	node = (struct vsp_node *) calloc(1,sizeof(struct vsp_node));

	if (node == NULL) {
		dc_errno = DENODE;
		return NULL;
	}

	if(node_init(node, path) < 0) {
		dc_errno = DENODE;
		free(node);
		return NULL;
	}

	rw_wrlock(&nodeRWlock);

	if (vspNode == NULL) {
		vspNode = node;
		node->prev = NULL;
	} else {
		node->prev = lastNode;
		lastNode->next = node;
	}

	lastNode = node;

	m_init(&node->mux);
	m_lock(&node->mux);
	rw_unlock(&nodeRWlock);

	return node;
}

struct vsp_node *
delete_vsp_node(int fd)
{

	struct vsp_node *node;
	unsigned int i;

	rw_wrlock(&nodeRWlock);

	node = vspNode;

	while (node != NULL) {

			for( i = 0; i < node->reference; i++ ) {
				if (node->fd_set[i] == fd) {

				node_detach_fd(node, fd);

				real_node_unplug(node);
				m_lock(&node->mux);
				rw_unlock(&nodeRWlock);

				return node;
			}
		}
		node = node->next;
	}

	/* node not exist */
	rw_unlock(&nodeRWlock);

	return NULL;
}

struct vsp_node *
get_vsp_node(int fd)
{

	struct vsp_node *node;
	unsigned int i;

	rw_rdlock(&nodeRWlock);

	node = vspNode;

	while (node != NULL) {

		for( i = 0; i < node->reference; i++ ) {
			if (node->fd_set[i] == fd) {

				node->dataFd = fd;
				m_lock(&node->mux);
				rw_unlock(&nodeRWlock);

				return node;
			}
		}
		node = node->next;
	}

	rw_unlock(&nodeRWlock);

	return NULL;

}



void node_destroy( struct vsp_node *node)
{

	if(node == NULL) return;


	if( node->reference > 0 ) {
		dc_debug(DC_INFO, "[%d] reference %d destroy canceled", node->dataFd, node->reference);
		m_unlock(&node->mux);
		return;
	}

	dc_debug(DC_INFO, "[%d] destroing node", node->dataFd);


	free(node->pnfsId);
	free(node->directory);
	free(node->file_name);

	if(node->url != NULL) {
		free(node->url->file);
		free(node->url->host);
		if( node->url->prefix != NULL ) {
			free( node->url->prefix);
		}
		free(node->url);
	}

	if(node->ipc != NULL) {
		free(node->ipc);
	}

	if( node->stagelocation != NULL ) {
		free(node->stagelocation);
	}

	if(node->ahead != NULL) {
		if(node->ahead->buffer != NULL) {
			free(node->ahead->buffer);
		}

		free(node->ahead);
	}


	if( node->sum != NULL ) {
		free( node->sum );
	}

	m_unlock(&node->mux);
	free(node);

}


void node_unplug( struct vsp_node *node)
{


	rw_wrlock(&nodeRWlock);

	real_node_unplug(node);

	rw_unlock(&nodeRWlock);

	return;

}


void real_node_unplug( struct vsp_node *node)
{


	if(node == NULL) return;


	if( node->reference > 0 ) {
		dc_debug(DC_INFO, "[%d] reference %d unplug canceled", node->dataFd, node->reference);
		return;
	}

	dc_debug(DC_INFO, "[%d] unpluging node", node->dataFd);


	if (node->next != NULL) {
		/* we are not a last in the chain */
		node->next->prev = node->prev;
	}else{
		/* we are last in the chain */
		lastNode = node->prev;
	}

	if (node->prev != NULL) {
		/* we are not a first in the chain */
		node->prev->next = node->next;
	}else{
		/* we are first in the chain */
		vspNode = node->next;
	}

}



fdList getAllFD()
{

	struct vsp_node *node;
	int *all = NULL;
	int count;
	int nc;
	int i;
	fdList list;

	rw_wrlock(&nodeRWlock);


	/* find out houw many fd's at all */
	node = vspNode;

	count = 0;
	while (node != NULL) {
		count +=  node->reference;
		node = node->next;
	}


	if( count > 0 ) {
		all = (int *)malloc( count*sizeof(int) );
		if( all != NULL ) {

			node = vspNode;
			nc = 0;
			while( node != NULL ) {
				for( i = 0; i < node->reference; i++) {
					all[nc] = node->fd_set[i];
					++nc;
				}
				node = node->next;
			}
		}
	}

	rw_unlock(&nodeRWlock);

	list.len = count;
	list.fds = all;

	return list;
}
