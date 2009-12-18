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
 * $Id: dcap_shared.h,v 1.7 2005-02-01 09:23:09 tigran Exp $
 */



#ifndef DCAP_SHARED_H
#define DCAP_SHARED_H

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <limits.h>
#include <stdlib.h>
#include <errno.h>
#include <stdarg.h>
#include <string.h>


#ifdef WIN32
#   include <io.h>
#   include <time.h>
#   include <Winsock2.h>
#   include <process.h>
#   include <mmsystem.h>
#   include "dcap_win32.h"
#   include <windows.h>
#else
#   include <sys/time.h>
#   include <sys/times.h>
#   include <unistd.h>
#   include <sys/socket.h>
#   include <netinet/in.h>
#   include <arpa/inet.h>
#   include <netdb.h>
#endif

#include "dcap_nodes.h"
#include "system_io.h"
#include "dcap_debug.h"
#include "dcap_error.h"
#include "dcap_types.h"
#include "dcap_protocol.h"
#include "dcap_poll.h"
#include "dcap_mqueue.h"
#include "array.h"
#include "pnfs.h"
#include "sysdep.h"
#include "links.h"
#include "dcap_url.h"
#include "dcap_accept.h"
#include "dcap_ahead.h"
#include "dcap_reconnect.h"
#include "ioTunnel.h"
#include "tunnelManager.h"
#include "io.h"
#include "dcap_checksum.h"
#include "xutil.h"


#ifdef DC_CALL_TRACE
#	include "trace_back.h"
#endif

extern int             cache_open(struct vsp_node *);
extern int             initControlLine(struct vsp_node *);
extern int             serverConnect(struct vsp_node *);
extern server         *parseConfig(const char *);
extern int             cache_connect(server *);
extern int             sayHello(int, ioTunnel *);
extern int             create_data_socket(int *, unsigned short *);
extern int             ascii_open_conversation(struct vsp_node *);
extern int             close_data_socket(int);
extern int             sendControlMessage(int, const char *, size_t, ioTunnel *);
extern asciiMessage   *getControlMessage(int, struct vsp_node *);
extern int             getDataMessage(struct vsp_node *);
extern int             data_hello_conversation(struct vsp_node *);
extern int             sendDataMessage(struct vsp_node *, char *, int , int , ConfirmationBlock *);
extern int             reconnected(struct vsp_node *, int, int64_t);
extern int             dc_set_pos(struct vsp_node *, int, int64_t);
extern int             newControlLine(struct vsp_node *);
extern void            dc_setCallbackPortRange( unsigned short, unsigned short );
extern void            dc_setCallbackPort( unsigned short );
extern void            getRevision( revision * );
extern void            dc_setTCPSendBuffer( int );
extern void            dc_setTCPReceiveBuffer( int );
extern int             init_hostname();
extern int             get_data( struct vsp_node *);
extern int             get_fin( struct vsp_node *);
extern int             get_ack(int , ConfirmationBlock * );
extern ConfirmationBlock get_reply(int);
extern int64_t         htonll(int64_t);
extern int64_t         ntohll(int64_t);
extern int             str2errno(const char *);


#endif /* DCAP_SHARED_H */
