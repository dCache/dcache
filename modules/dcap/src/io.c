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
writeln(int fd, const char *buf, int bufsize, ioTunnel *en)
{
	int             nleft, nwritten;
	char            c = '\n';
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

#ifdef WIN32
	send(fd, &c, 1, 0);
#else
	en == NULL ? system_write(fd, &c, 1) : en->eWrite(fd, &c, 1);
#endif /* WIN32 */

	return (bufsize - nleft);
}

int
readln(int fd, char *str, int bufsize, ioTunnel *en)
{
	char            c;
	int             rc, i;

	for (i = 0; i < bufsize - 1; i++) {

#ifdef WIN32
		rc = recv(fd, &c, 1, 0);
#else
		rc = en == NULL ? system_read(fd, &c, 1) : en->eRead(fd, &c, 1);
#endif /* WIN32 */

		if (rc == 1) {
			str[i] = c;
			if (c == '\n')
				break;
		} else if (rc == 0) {
			if (i == 0) {
				str[0] = '\0';
				return 0;
			} else {
				break;
			}
		} else {
			return -1;
		}
	}

	str[i] = '\0';
	return (i);
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
