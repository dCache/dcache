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
 * $Id: LICENSE,v 1.4 2004-11-03 14:09:23 tigran Exp $
 */


/*
 *  This file contains all non-public functions in dcap.c that are
 *  shared with other dcap source-code.
 */
#include <sys/types.h>
#include <stdint.h>

#include "dcap_types.h"
#include "ioTunnel.h"

int cache_open(struct vsp_node *);
int close_data_socket(int);
int close_control_socket(int, ioTunnel *);
int sendControlMessage(int, const char *, size_t, ioTunnel *);
asciiMessage *getControlMessage(int, struct vsp_node *);
int data_hello_conversation(struct vsp_node *);
int sendDataMessage(struct vsp_node *, char *, int , int , ConfirmationBlock *);
int reconnected(struct vsp_node *, int, int64_t);
int dc_set_pos(struct vsp_node *, int, int64_t);
int newControlLine(struct vsp_node *);
int get_data( struct vsp_node *);
int get_fin( struct vsp_node *);
int get_ack(int , ConfirmationBlock * );


#ifndef HAVE_NTOHLL
uint64_t ntohll(uint64_t x);
#endif /* HAVE_NTOHLL */

#ifndef HAVE_HTONLL
uint64_t htonll(uint64_t arg);
#endif /* HAVE_HTONLL */
