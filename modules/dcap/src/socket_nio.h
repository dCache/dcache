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
 * $Id: socket_nio.h,v 1.5 2004-11-01 19:33:30 tigran Exp $
 */

#ifndef DCAP_SOCKET_NIO
#define DCAP_SOCKET_NIO
#include <sys/types.h>

#ifndef WIN32
#    include <sys/socket.h>
#else
#    include <Winsock2.h>
#endif

/* connect with timeout in seconds */

extern int nio_connect(int, const struct sockaddr *, int, unsigned int);
extern int clearNonBlocking(int);
extern int setNonBlocking(int);

#endif
