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
 * $Id: passive.c,v 1.1 2006-07-17 15:13:36 tigran Exp $
 */

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdlib.h>
#include <string.h>
#include <netdb.h>

#include "dcap.h"
#include "socket_nio.h"
#include "node_plays.h"
#include "system_io.h"

/*

	passive mode.
	clinet connects to pool.
	return: 0 on success


 */



int connect_to_pool( struct vsp_node *node , poolConnectInfo *pool)
{


	int             fd;
	struct sockaddr_in pool_addr;
	struct hostent *hp;
	int32_t         passive[2];


	fd = socket(AF_INET, SOCK_STREAM, 0);
	if (fd < 0) {
		dc_errno = DESOCKET;
		return -1;
	}


	memset((char *) &pool_addr, 0, sizeof(pool_addr));

	pool_addr.sin_family = AF_INET;
	pool_addr.sin_port = htons(pool->port);


	/* first try  by host name, then by address */
	hp = (struct hostent *) gethostbyname(pool->hostname);
	if (hp == NULL) {
		if ((pool_addr.sin_addr.s_addr = inet_addr(pool->hostname)) < 0) {
			system_close(fd);
			dc_errno = DERESOLVE;
			return -1;
		}
	} else {
		memcpy( &pool_addr.sin_addr.s_addr, hp->h_addr_list[0], hp->h_length);
	}

	if (nio_connect(fd, (struct sockaddr *) & pool_addr, sizeof(pool_addr), 20) != 0) {
		system_close(fd);
		dc_errno = DECONNECT;
		return -1;
	}


	passive[0] = htonl(node->queueID);
	passive[1] = htonl( strlen(pool->challenge) );

	system_write(fd, passive, 8);
	system_write(fd, pool->challenge, strlen(pool->challenge) );

	node->isPassive = 1;
	node_attach_fd(node, fd);

	return 0;
}
