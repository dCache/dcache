/*
 * $Id: client.c,v 1.1 2002-10-14 10:31:36 cvs Exp $
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
#include <strings.h>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>

#include "gssTunnel.h"

int
main(int argc, char *argv[])
{

	int             fd;
	struct sockaddr_in serv_addr;
	struct hostent *hp;
	char *secret;
	int n;
	char c;
	char buffer[512];

	if(argc != 3 ) {
		printf("Usage: %s <host> <port>\n", argv[0]);
		exit(1);

	}

	fd = socket(AF_INET, SOCK_STREAM, 0);
	if (fd < 0) {
		perror("socket");
		exit(1);
	}
	bzero((char *) &serv_addr, sizeof(serv_addr));


	serv_addr.sin_family = AF_INET;
	serv_addr.sin_port = htons(atoi(argv[2]));


	hp = (struct hostent *) gethostbyname(argv[1]);
	if (hp == NULL) {
		if ((serv_addr.sin_addr.s_addr = inet_addr(argv[1])) < 0) {
			close(fd);
			perror("gethostbyname");
			exit(1);
		}
	} else {
		bcopy(hp->h_addr_list[0], &serv_addr.sin_addr.s_addr, hp->h_length);
	}

	if (connect(fd, (struct sockaddr *) & serv_addr, sizeof(serv_addr)) < 0) {
		close(fd);
		perror("connect");
		exit(1);
	}


	printf("Done %d %d\n", fd, eInit(fd));


	while(1) {
		eWrite(fd, "Hello Java\n", 11);
		while(1) {
			if ( eRead(fd,  &c, 1) < 0 ) return 1;
			putchar(c); fflush(stdout);
			if(c=='\n') break;
		}
	}

	close(fd);
	return 0;
}
