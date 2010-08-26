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
 * $Id: socket_nio.c,v 1.10 2004-11-01 19:33:30 tigran Exp $
 */

#include <sys/types.h>
#ifndef WIN32
#    include <sys/time.h>
#    include <sys/times.h>
#    include <sys/socket.h>
#    include <unistd.h>
#else
#    include <Winsock2.h>
#endif
#include <fcntl.h>
#include <errno.h>
#include "dcap_reconnect.h"
#include "dcap_debug.h"


int setNonBlocking(int fd)
{
#ifdef WIN32
	unsigned long arg = 1L;
	return ioctlsocket(fd, FIONBIO, &arg);
#else

	int flag;
	
	flag = fcntl(fd, F_GETFL, 0);
	if(flag == -1) {
		flag = 0;
	}

	return fcntl(fd, F_SETFL, flag | O_NONBLOCK);
#endif /* WIN32 */
}

int clearNonBlocking(int fd)
{
#ifdef WIN32
	return ioctlsocket(fd, FIONBIO, NULL);
#else
	int flag;
	
	flag = fcntl(fd, F_GETFL, 0);
	if(flag == -1) {
		flag = 0;
	}

	return fcntl(fd, F_SETFL, flag ^ O_NONBLOCK);
#endif /* WIN32 */
}




int  nio_connect(int  s,  const  struct  sockaddr   *name,   int namelen, unsigned int timeout)
{
	int rc;
#ifndef WIN32
	clock_t         rtime; /* timestamp */
	struct tms      dumm;
#endif

	dcap_set_alarm(timeout);

#ifndef WIN32
	rtime = times(&dumm);	
#endif
	rc = connect(s, name, namelen);
	if((rc == -1) || isIOFailed ) {
		rc = -1;
	}else{
#ifndef WIN32
		dc_debug(DC_TIME, "Connected in %2.2fs.", (times(&dumm) - rtime)/(double)sysconf(_SC_CLK_TCK ));
#endif
	}
	
	dcap_set_alarm(0);
	return rc;
}
