/*
 *       Copyright (c) 2000,2001,2002 DESY Hamburg DMG-Division
 *               All rights reserved.
 *
 *       THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF
 *                 DESY Hamburg DMG-Division
 */
#ifndef TUNNEL_QUEUE_H
#define TUNNEL_QUEUE_H
#include <errno.h>
#include <gssapi.h>

typedef struct {
	gss_ctx_id_t context_hdl;
	int isAuthentificated;
} tunnel_ctx_t;


extern tunnel_ctx_t* createGssContext(int fd);
extern void setGssContext(int fd, gss_ctx_id_t ctx);
extern tunnel_ctx_t* getGssContext(int fd);
extern void destroyGssContext(int fd);

#endif
