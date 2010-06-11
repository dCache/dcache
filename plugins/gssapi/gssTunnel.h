/*
 *       Copyright (c) 2000,2001,2002 DESY Hamburg DMG-Division
 *               All rights reserved.
 *
 *       THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF
 *                 DESY Hamburg DMG-Division
 */

#ifndef GSS_IO_TUNNEL_H
#define GSS_IO_TUNNEL_H

extern int eInit(int);
extern int eDestroy(int);
extern ssize_t eRead(int fd, void *buf, size_t size);
extern ssize_t eWrite(int fd, const void *buf, size_t size);

extern int gss_check(int sock);



#endif
