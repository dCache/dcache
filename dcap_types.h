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
 * $Id: dcap_types.h,v 1.50 2007-07-09 19:35:09 tigran Exp $
 */
#ifndef _DCAP_TYPES_H_
#define _DCAP_TYPES_H_

#include <sys/types.h>
#ifdef WIN32
#   include "dcap_win32.h"
#endif

#ifdef sun
#include <sys/int_types.h>
#endif

#include "ioTunnel.h"
#include "sysdep.h"

#ifdef __alpha
typedef int int32_t;
typedef long int64_t;
#endif /* alpha */

typedef struct {

	int32_t         code;
	int32_t         in_response;
	int32_t         result;
	int64_t         lseek;
	int64_t         fsize;

}               ConfirmationBlock;

typedef struct {

	unsigned int    destination;
	int             priority;
	int             type;
	char            *msg;

}               asciiMessage;

typedef struct {
	asciiMessage    **mQueue;	/* Queue */
	int             qLen;		/* queue length */
	int             mnum;		/* message number */
	unsigned int	id;		/* message Queue ID*/
	
#ifndef NOT_THREAD_SAFE
	pthread_mutex_t lock;
#endif /* NOT_THREAD_SAFE */
} messageQueue;

typedef struct {
	char *buffer;
	off64_t base;
	size_t    size;
	size_t    used;
	size_t    cur;
	int       isDirty;
} ioBuffer;

#define URL_NONE 0
#define URL_DCAP 1
#define URL_PNFS 2

typedef struct {
	char *host;
	char *file;
	int type;
	char *prefix;
} dcap_url;


typedef struct {
	unsigned long sum;
	int isOk;
	int type;
}checkSum;


typedef struct vsp_node {
	int dataFd;		/* data socket descriptor */
	int fd;			/* contorl socket descriptor */
	off64_t pos;		/* file current position */
	off64_t seek;		/* seek offset */
	int whence;		/* seek whence */
	
	unsigned int asciiCommand; /* the actual command */
	
	char *pnfsId;		/* pNfs ID */
	unsigned short data_port;		/* TCP port for data socket (callback)*/
	struct vsp_node *next;	/* pointer to next node */
	struct vsp_node *prev;	/* pointer to prevision node */
	char *directory;	/* file directory name */
	char *file_name;	/* file name */
	mode_t mode;		/* file open mode */
	uid_t uid;          /* file owner, used by chown */
	gid_t gid;          /* file owner group, used by chown */	
	int flags;          /* file open flags */
	time_t atime;       /* file stageing time */
	char *stagelocation; /* location of client in stage request */
	unsigned int queueID;/* Queue destination ID */
	ioBuffer *ahead;	/* read ahead buffer */
	unsigned int unsafeWrite; /* do not have a conformation on each write */
	dcap_url *url;      /* url-like format */
	ioTunnel *tunnel; /* special functions set to do IO */
	int sndBuf; /* TCP send buffer size */
	int rcvBuf; /* TCP recive buffer size */
	char *ipc;  /* pointer to the "some" data */
	
	
	checkSum *sum; /* checksum */

	/* to be able to have a dup */
	unsigned int reference; /* reference count */
	int fd_set[32]; /* quick hack to allow multiple file descriptors */
		
	int isPassive; /* passive connetion: client connects to the pool */


#ifndef NOT_THREAD_SAFE
	pthread_mutex_t mux;
#endif /* NOT_THREAD_SAFE */
	
}               vsp_Node;



typedef struct {
	char *hostname;
	ioTunnel *tunnel;
	short port;
} server;

typedef struct {
	char *hostname;
	int port;
	char *challenge;
} poolConnectInfo;

typedef struct {
	int sock;
	int id;
} acceptSocket;

typedef struct {
	int Min;
	int Maj;
} revision;



typedef struct {
	int len;
	int *fds;
} fdList;

#ifndef  _IOVEC2_
#define _IOVEC2_
typedef struct {
	off64_t offset;
	int len;
	char *buf;
} iovec2;

#endif

#ifndef DEFAULT_DOOR_PORT
#define DEFAULT_DOOR_PORT 22125
#endif

#endif				/* _DCAP_TYPES_H */
