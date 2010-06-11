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
 * $Id: dcap_error.c,v 1.23 2006-09-22 13:25:46 tigran Exp $
 */

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include "sysdep.h"
#include "dcap_errno.h"
#include "system_io.h"
#ifdef WIN32
#    include "dcap_win32.h"
#endif

#define DC_MAX_SRV_ERR_MSG 1024

#ifdef _REENTRANT

static TKEY err_key;
static TKEY srvMessage_key;
static TKEY srvMessagePtr_key;
static MUTEX(kLock);
static int err_once = 0;
static int msg_once = 0;
static int msgPtr_once = 0;

int *
__dc_errno()
{
	int *en;

	/* only one thread allowed to kreate key*/
	m_lock(&kLock);
	if(!err_once) {
		t_keycreate(&err_key, NULL);
		err_once++;
	}
	m_unlock(&kLock);

	t_getspecific(err_key, (void **)&en);
	if( en == NULL ) {
		en = calloc(1, sizeof(int));
		t_setspecific(err_key, (void *)en);
	}

	return en;
}

char **
__dc_srvMessage()
{
	char  **msg;

	/* only one thread allowed to create key*/
	m_lock(&kLock);
	if(!msg_once) {
		t_keycreate(&srvMessage_key, NULL);
		msg_once++;
	}
	m_unlock(&kLock);

	t_getspecific(srvMessage_key, (void **)&msg);
	if ( msg == NULL ) {
		msg = calloc(1,sizeof(char *));
		t_setspecific(srvMessage_key, (void *)msg);
	}

	return msg;
}

static char * dc_errno2srvMessage()
{
	char  *sPtr;

	/* only one thread allowed to kreate key*/
	m_lock(&kLock);
	if(!msgPtr_once) {
		t_keycreate(&srvMessagePtr_key, NULL);
		msgPtr_once++;
	}
	m_unlock(&kLock);

	t_getspecific(srvMessagePtr_key, (void **)&sPtr);
	if ( sPtr == NULL ) {
		sPtr = calloc(DC_MAX_SRV_ERR_MSG + 1,sizeof(char));
		strcat(sPtr,"Server Error Messages");
		sPtr[DC_MAX_SRV_ERR_MSG] = '\0';
		t_setspecific(srvMessagePtr_key, (void *)sPtr);
	}

	return sPtr;
}


#define dc_errno (*(__dc_errno()))
#define srvMessage (*(__dc_srvMessage()))

#else
int dc_errno = 0;
static char *srvMessage = NULL;
static char srvConstMessage[DC_MAX_SRV_ERR_MSG];
#endif

static const char *const dcap_errlist[] = {
	"OK",
	"Unexpected data on CONTROL connection",
	"Unexpected data on DATA connection",
	"Unexpected data on CONTROL or DATA connection",
	"Message not confirmed",
	"Message not acknowledged",
	"Unexpected message",
	"Remote server has closed connection",
	"Can not resolve server name",
	"Invalid configuration format",
	"Can not read configuration file",
	"Write access to existing file",
	"Can not read non existing file",
	"Not Pnfs file system",
	"Invalid file name",
	"Can not create entry in pNfs",
	"Can not get pNfs ID",
	"Can not create new node",
	"Fail to make a new CONTROL connection",
	"Incorrect config file format",
	"Parser error",
	"System error",
	"Can not open config file",
	"Config file not available",
	"Can not create socket",
	"Unable to connect to server",
	"Server rejected \"hello\"",
	"Bind failed",
	"Memory allocation failed",
	"Invalid flags passed to open",
	"Server error",
	"File not cached",
	"Not valid DCAP url",
	"Unsafe write operation failed",
	"Unrecoverable read error",
	""
};

void
dc_perror(const char *msg)
{
	const char * dc_strerror(int);
	char *se;

	if ((msg != NULL) && strlen(msg)) {
		system_write(2, msg, strlen(msg) );
		system_write(2, " : ", 3);
	}


	system_write(2, dc_strerror(dc_errno), strlen(dc_strerror(dc_errno)) );
	system_write(2, "\n", 1);
	if (errno) {
		se = strerror(errno);
		system_write(2, "System error: ", 14);
		system_write(2, se, strlen(se) );
		system_write(2, "\n", 1);
	}
#ifdef WIN32
	reportWinsockError();
#endif

}

void
dc_error(const char *msg)
{
	dc_perror(msg);
}

void dc_setServerError(const char *msg)
{
    char *p;
    int len;

	if(srvMessage != NULL) {
		free(srvMessage);
		srvMessage = NULL;
	}

	if(msg != NULL) {
		srvMessage = strdup(msg);
		dc_errno = DESRVMSG;
	}

	errno = EIO;

	/* 'p' will point to the buffer for error mesage */
#ifdef _REENTRANT
	p = dc_errno2srvMessage();
#else
	p = srvConstMessage;
#endif /* _REENTRANT */

	len = strlen(msg);
	if(len > DC_MAX_SRV_ERR_MSG) {
		len = DC_MAX_SRV_ERR_MSG;
	}

	strncpy(p, msg, len);
	p[len] = '\0';

	/* there is no need to free 'p', while we get it already allocated */
}


const char * dc_strerror(int errnum)
{
    const char *p;

	if( (errnum > DEMAXERRORNUM) || (errnum < DEOK) ) {
		return "Unknown error";
	}

	if(errnum == DESRVMSG) {
#ifdef _REENTRANT
	p = dc_errno2srvMessage();
#else
	p = srvConstMessage;
#endif /* _REENTRANT */
	}else{
		p = dcap_errlist[errnum];
	}

	return (const char *)p;
}
