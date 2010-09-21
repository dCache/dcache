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
 * $Id: io.c,v 1.7 2004-11-01 19:33:30 tigran Exp $
 */

#include <stdlib.h>
#include "system_io.h"
#include "ioTunnel.h"
#ifdef WIN32
#    include  <Winsock2.h>
#endif

int
writen(int fd, const char *buf, int bufsize, ioTunnel *en)
{
	int             nleft, nwritten;
	nleft = bufsize;

	while (nleft > 0) {
#ifdef WIN32
		nwritten = send(fd, buf, nleft, 0);
#else
		nwritten = en == NULL ? system_write(fd, buf, nleft) : en->eWrite(fd, buf, nleft);
#endif /* WIN32 */

		if (nwritten <= 0)
			return (nwritten);
		nleft -= nwritten;
		buf += nwritten;
	}

	return (bufsize - nleft);
}

int
readn(int fd, char *buf, int bufsize, ioTunnel *en)
{
	int             nleft, nread;

	nleft = bufsize;

	while (nleft > 0) {

#ifdef WIN32
		nread = recv(fd, buf, nleft, 0);
#else
		nread = en == NULL ? system_read(fd, buf, nleft) : en->eRead(fd, buf, nleft);
#endif /* WIN32 */

		if (nread < 0)
			return (nread);
		else if (nread == 0)
			break;
		nleft -= nread;
		buf += nread;
	}

	return (bufsize - nleft);
}
