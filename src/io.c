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
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdlib.h>
#include <string.h>
#include <netdb.h>
#include "dcap.h"
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

/**
 * Connects to specified host and service. The service can be service name as
 * defined in /etc/services or port number in string form.
 *
 * returns non negative socket descriptor or -1 on error.
 */
int
socket_connect(const char *host, const char *service) {
    int fd = -1;
    struct addrinfo hints, *res_addrinfo, *ai;
    int rc;

#ifdef WIN32
    initWinSock();
#endif /* WIN32 */

    memset(&hints, 0, sizeof (hints));
    hints.ai_family = AF_UNSPEC; /* IPv6 + IPv4 */
    hints.ai_socktype = SOCK_STREAM; /* TCP */

    rc = getaddrinfo(host, service, &hints, &res_addrinfo);
    if (rc) {
        dc_errno = DESOCKET;
        return -1;
    }

    for (ai = res_addrinfo; ai != NULL; ai = ai->ai_next) {
        fd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (fd < 0) continue;

        if (nio_connect(fd, ai->ai_addr, ai->ai_addrlen, 20) == 0) break;
        system_close(fd);
        fd = -1;
    }

    freeaddrinfo(res_addrinfo);

    if (fd < 0) {
        dc_errno = DECONNECT;
    }

    return fd;
}