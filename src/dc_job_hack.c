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
 * $Id: dc_job_hack.c,v 1.2 2004-11-01 19:33:28 tigran Exp $
 */

#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/time.h>
#include <fcntl.h>
#include <unistd.h>
#include <limits.h>
#include <stdlib.h>
#include <errno.h>
#include <stdarg.h>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>


main(int argc, char *argv[])
{
	int32_t         sessionId, challengeSize;

	int             fd;
	struct sockaddr_in serv_addr;
	struct hostent *hp;

	if(argc != 4) {
		printf("Usage: %s <host> <port> <session id>\n", argv[0]);
		exit(1);
	}

	fd = socket(AF_INET, SOCK_STREAM, 0);
	if (fd < 0) {
		exit(1);
	}
	bzero((char *) &serv_addr, sizeof(serv_addr));


	serv_addr.sin_family = AF_INET;
	serv_addr.sin_port = htons(atoi(argv[2]));


	hp = (struct hostent *) gethostbyname(argv[1]);
	if (hp == NULL) {
		if ((serv_addr.sin_addr.s_addr = inet_addr(argv[1])) < 0) {
			close(fd);
			exit(1);
		}
	} else {
		bcopy(hp->h_addr_list[0], &serv_addr.sin_addr.s_addr, hp->h_length);
	}

	if (connect(fd, (struct sockaddr *) & serv_addr, sizeof(serv_addr)) < 0) {
		close(fd);
		exit(1);
	}
	sessionId = htonl(atoi(argv[3]));

	write(fd, &sessionId, sizeof(sessionId));
	write(fd, &sessionId, sizeof(sessionId));

	close(fd);
}
