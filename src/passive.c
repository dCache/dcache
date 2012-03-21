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
int connect_to_pool(struct vsp_node *node, poolConnectInfo *pool) {
    int fd;
    int32_t passive[2];

    fd = socket_connect(pool->hostname, pool->port);
    if (fd < 0) {
        return -1;
    }

    passive[0] = htonl(node->queueID);
    passive[1] = htonl(strlen(pool->challenge));

    system_write(fd, passive, sizeof(passive));
    system_write(fd, pool->challenge, strlen(pool->challenge));

    node->isPassive = 1;
    node_attach_fd(node, fd);

    return 0;
}
