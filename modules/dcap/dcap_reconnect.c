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
 * $Id: dcap_reconnect.c,v 1.21 2004-11-01 19:33:29 tigran Exp $
 */
#include <sys/types.h>
#include "dcap_types.h"
#include "dcap_debug.h"
#include "dcap_protocol.h"
#include "dcap_poll.h"
#include "socket_nio.h"
#include "sysdep.h"
#include <fcntl.h>
#include <stdlib.h>
#include <signal.h>
#ifdef WIN32
#    include "dcap_win32.h"
#    include "dcap_win_poll.h"
#else
#    include <sys/poll.h>
#    include <unistd.h>
#endif
#include <string.h>
#include <stdio.h>


#ifdef _REENTRANT
static TKEY ioFailedKey;
static TKEY isAlarmKey;
static TKEY sa_alarmKey;

static int ioKeyOnce = 0;
static int alarmKeyOnce = 0;
static int saKeyOnce = 0;
static MUTEX(kLock);

int *
__isIOFailed()
{
	int *io;
  
	m_lock(&kLock);
	if(!ioKeyOnce) {
		t_keycreate(&ioFailedKey, NULL);	
		++ioKeyOnce;
	}
	m_unlock(&kLock);
	
	t_getspecific(ioFailedKey, (void **)&io);
	if( io == NULL ) {
		io = calloc(1, sizeof(int));
		t_setspecific(ioFailedKey, (void *)io);
	}
	
	return io;  
  
}

#define isIOFailed (*(__isIOFailed()))

static int *
__isAlarm()
{
	int *al;
  
	m_lock(&kLock);
	if(!alarmKeyOnce) {
		t_keycreate(&isAlarmKey, NULL);	
		++alarmKeyOnce;
	}
	m_unlock(&kLock);
	
	t_getspecific(isAlarmKey, (void **)&al);
	if( al == NULL ) {
		al = calloc(1, sizeof(int));
		t_setspecific(isAlarmKey, (void *)al);
	}
	
	return al;  
  
}

#define isAlarm (*(__isAlarm()))

static struct sigaction *
__old_sa_alarm()
{
	struct sigaction *sa;
  
	m_lock(&kLock);
	if(!saKeyOnce) {
		t_keycreate(&sa_alarmKey, NULL);	
		++saKeyOnce;
	}
	m_unlock(&kLock);
	
	t_getspecific(sa_alarmKey, (void **)&sa);
	if( sa == NULL ) {
		sa = calloc(1, sizeof(struct sigaction));
		t_setspecific(sa_alarmKey, (void *)sa);
	}
	
	return sa;  
  
}

#define old_sa_alarm (*(__old_sa_alarm()))

#else
int isIOFailed = 0;
static int isAlarm = 0;
#ifndef WIN32
    static struct sigaction old_sa_alarm;
#endif /* WIN32 */
#endif /* _REENTRANT */

extern int dc_set_pos(struct vsp_node *, int, int64_t);
extern asciiMessage   *getControlMessage(int, struct vsp_node *);
extern int sendControlMessage(int, const char *, size_t, ioTunnel *);
extern int data_hello_conversation(struct vsp_node *);

int smart_reconnect(struct vsp_node *, int);
int recover_connection(struct vsp_node *, int);

int recover_connection(struct vsp_node * node, int mode)
{
	char fail_message[64];

	fail_message[0] = '\0';
	sprintf(fail_message, "%d 1 client fail\n", node->queueID);	
	sendControlMessage(node->fd, fail_message, strlen(fail_message), node->tunnel);
	
	return smart_reconnect(node, mode);
}



int
smart_reconnect(struct vsp_node * node, int mode)
{

	int old_fd;

	if(node->flags != O_RDONLY) {
		return 1;
	}

	/* user uses data fd as file descriptor and we have to keep it unchanged */
	old_fd = node->dataFd;

	if(data_hello_conversation(node) < 0) {
		dc_debug(DC_ERROR, "[%d] Failed to make a new data connection.", node->dataFd);
		return 1;
	}
	
	/* try to get old fd in use */
	node->dataFd = dup2(node->dataFd, old_fd);
	if( node->dataFd != old_fd) {
		node->dataFd = old_fd;
		dc_debug(DC_ERROR, "dup2 failed. Reconnection impossible.");
		return 1;
	}	
	
	if(mode && !dc_set_pos(node, mode, -1)) {
		dc_debug(DC_ERROR, "[%d] Failed to set correct position.", node->dataFd);
		return 1;
	}

	dc_debug(DC_INFO, "[%d] Broken connection recovered.", node->dataFd);
	return 0;
}

#ifndef WIN32

void alarm_handler( int sig)
{
	dc_debug(DC_ERROR, "dcap: Last IO operation timeout.");
	isIOFailed = 1;
	isAlarm = 0;
	/* set back old alarm handler */
	sigaction(SIGALRM, &old_sa_alarm, NULL);
}

int dcap_set_alarm(unsigned int t)
{
	struct sigaction sa_alarm;
	struct sigaction *optr;
	struct sigaction *ptr;

	if(t) {
		dc_debug(DC_TRACE, "Setting IO timeout to %d seconds.", t);
		sa_alarm.sa_handler = alarm_handler;
		sigemptyset(&sa_alarm.sa_mask);
		sa_alarm.sa_flags = 0;
		
		ptr = &sa_alarm;
		optr = &old_sa_alarm;
		
		/* reset the  error flag */		
		isIOFailed = 0;
		/* make a note, that alarm is set */
		isAlarm = 1;

	}else{
	
		dc_debug(DC_TRACE, "Removing IO timeout handler.");
		/* set the old interrupt handler */
		
		/* do nothing if alarm not seted */
		if( !isAlarm ) return 0;
		ptr = &old_sa_alarm;
		optr = NULL;
		isAlarm = 0;
	}
	
	if (sigaction(SIGALRM, ptr, optr) < 0) {
		dc_debug(DC_ERROR,"Sigaction failed!");
		return 1;
	
	}
	alarm(t);

	return 0;
}

#else
int dcap_set_alarm(unsigned int t)
{
	return 0;
}
#endif /* WIN32 */

int ping_pong(struct vsp_node * node)
{


	char ping[64];
	int len;	
	asciiMessage   *aM;
	struct pollfd  pfd;
	int rc;
	
	ping[0] = '\0';

	len = sprintf(ping, "%d 2 client ping\n", node->queueID);
	
	
	setNonBlocking(node->fd);	
	rc = sendControlMessage(node->fd, ping, len, node->tunnel);
	clearNonBlocking(node->fd);

	
	if( rc < 0 ) {
		dc_debug(DC_ERROR, "Ping failed (control line down).");
		return 0;		
	}
	
	pfd.fd = node->fd;
	pfd.events = POLLIN;
	rc = poll(&pfd, 1, 1000*10); /* 10 seconds */

	if((rc == 1) && (pfd.revents & POLLIN )) {
		
		dcap_set_alarm(10);
		aM = getControlMessage(HAVETO, node);
		dcap_set_alarm(0);
		
		if( (aM != NULL ) && (aM->type == ASCII_PING) ) {
			free(aM->msg);
			free(aM);
			return 1;
		}
	}
	dc_debug(DC_ERROR, "Ping failed.");
	return 0;

}


