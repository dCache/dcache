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
 * $Id: ioTunnel.h,v 1.4 2004-11-01 19:33:30 tigran Exp $
 */

#ifndef DCAP_IO_TUNNEL_H
#define DCAP_IO_TUNNEL_H

#ifndef WIN32
#   include <unistd.h>
#else
#   include "dcap_unix2win.h"
#endif

typedef struct {
	ssize_t (*eRead)(int, void *, size_t);              /* read heandler     */
	ssize_t (*eWrite)(int, const void *, size_t);       /* write heandler    */
	int (*eInit)(int);                                  /* mapping  manager  */
	int (*eDestroy)(int);                               /* demapping manager */
} ioTunnel;


#endif /* DCAP_IO_TUNNEL_H */
