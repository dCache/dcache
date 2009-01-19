/*
 *       Copyright (c) 2000,2001,2002 DESY Hamburg DMG-Division
 *               All rights reserved.
 *
 *       THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF
 *                 DESY Hamburg DMG-Division
 */
#include "tunnelQueue.h"


#define MAX_GSS_CONTEXT 8192

/*
 * just a static array for all context.
 * The file descriptor of a connections is a array index.
 * FIXME: current limitation MAX_GSS_CONTEXT connections to handle.
 */
static tunnel_ctx_t* allTunnels[MAX_GSS_CONTEXT];



tunnel_ctx_t* createGssContext(int fd)
{

	if( fd < 0 || fd > MAX_GSS_CONTEXT) {
		errno = EINVAL;
#ifdef SHOW_ERROR
		perror("invalid file descriptor");
#endif
		return NULL;
	}

	tunnel_ctx_t *ctx = malloc( sizeof(tunnel_ctx_t) );

	if( ctx == NULL ) {
		errno = EINVAL;
#ifdef SHOW_ERROR
		perror("invalid file descriptor");
#endif
		return NULL;
	}

	ctx->context_hdl = GSS_C_NO_CONTEXT;
	ctx->isAuthentificated = 0;
	allTunnels[fd] = ctx;

	return ctx;

}


void setGssContext(int fd, gss_ctx_id_t ctx)
{
	if( fd < 0 || fd > MAX_GSS_CONTEXT) {
		errno = EINVAL;
#ifdef SHOW_ERROR
		perror("invalid file descriptor");
#endif
		return;
	}

	allTunnels[fd]->context_hdl = ctx;
}

tunnel_ctx_t* getGssContext(int fd)
{

	if( fd < 0 || fd > MAX_GSS_CONTEXT || allTunnels[fd] == NULL) {
		errno = EINVAL;
#ifdef SHOW_ERROR
		perror("invalid file descriptor");
#endif
		return NULL;
	}


	return allTunnels[fd];
}


void destroyGssContext(int fd)
{
	if( fd < 0 || fd > MAX_GSS_CONTEXT) {
		errno = EINVAL;
#ifdef SHOW_ERROR
		perror("invalid file descriptor");
#endif
		return;
	}

	free(allTunnels[fd]);
	allTunnels[fd] = NULL;
}
