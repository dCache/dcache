/*
 * $Id: server.c,v 1.1 2002-10-14 10:31:36 cvs Exp $
 */

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <string.h>
#include <unistd.h>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>


#include "gssTunnel.h"

#ifndef MAXHOSTNAMELEN
#define MAXHOSTNAMELEN 64
#endif

int
create_socket(int *dataFd, int *cbPort)
{

	struct sockaddr_in me, him;
#if defined(sun) || defined(sgi) || defined (__alpha)
	size_t          addrSize;
#else
	socklen_t       addrSize;
#endif
	int             bindResult;
	int             newFd;
	struct hostent *he;

	char           *hostName;



	hostName = (char *) malloc(MAXHOSTNAMELEN + 1);
	if (hostName == NULL) {
		*dataFd = -1;
		return -1;

	}
	hostName[MAXHOSTNAMELEN] = '\0';

	if (gethostname(hostName, MAXHOSTNAMELEN) < 0) {
		printf("Failed to get local host name.\n");
		*dataFd = -1;
		return -1;
	}
	/* trying to get full fully-qualified domain name. */
	he = (struct hostent *) gethostbyname((const char *) hostName);

	if (he != NULL) {
		/* if we successed to get it, let use it */
		free(hostName);
		hostName = (char *) strdup(he->h_name);
	} else {
		printf("Unable to get FQDN for host %s.\n", hostName);
	}

	printf("Setting hostname to %s.\n", hostName);


	*dataFd = socket(AF_INET, SOCK_STREAM, 0);
	if (*dataFd < 0) {
		return *dataFd;
	}
	memset((char *) &me, 0, sizeof(me));
	me.sin_family = AF_INET;
	me.sin_addr.s_addr = htonl(INADDR_ANY);
	me.sin_port = htons(*cbPort);

	addrSize = sizeof(me);
	bindResult = bind(*dataFd, (struct sockaddr *) & me, addrSize);
	if (bindResult < 0) {
		close(*dataFd);
		*dataFd = -1;
		return -1;
	}
	if (*cbPort == 0) {
		/* get our TCP port number */
#if defined(sun) || defined(sgi) || defined(__alpha)
		getsockname(*dataFd, (struct sockaddr *) & me, (int *) &addrSize);
#else
		getsockname(*dataFd, (struct sockaddr *) & me, (socklen_t *) & addrSize);
#endif
		*cbPort = ntohs(me.sin_port);
	}
	listen(*dataFd, 512);
	addrSize = sizeof(him);
	newFd = accept(*dataFd, (struct sockaddr *) & him, (int *) &addrSize);
	close(*dataFd);
	*dataFd = newFd;
	return *cbPort;

}



int
main(int argc, char *argv[])
{
	int             rc;
	int             port;
	int             sock;

	if (argc != 2) {
		printf("Usage %s <port>\n", argv[0]);
		exit(1);
	}
	port = atoi(argv[1]);

	create_socket(&sock, &port);


	gss_check(sock );

	close(sock);
	return 0;

}
