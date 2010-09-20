/*
 * Copyright (c) 1994 Jason R. Thorpe.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. All advertising materials mentioning features or use of this software
 *    must display thBE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED
 * AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 */

/*
 * Approximate the System V system call ``poll()'' with select(2).
 */

#include <sys/types.h>
#include <stdio.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <Winsock2.h>

#include "dcap.h"

struct pollfd {
        int fd;
        short events;
        short revents;
};

#define POLLIN          0x0001
#define POLLNORM        POLLIN
#define POLLPRI         POLLIN
#define POLLOUT         0x0008
#define POLLERR         0x0010          /* not used */
#define POLLHUP         0x0020
#define POLLNVAL        0x0040
#define POLLRDNORM      POLLIN
#define POLLRDBAND      POLLIN
#define POLLWRNORM      POLLOUT
#define POLLWRBAND      POLLOUT
#define POLLMSG         0x0800          /* not used */

#define __INVALIDPOLL   ~(POLLIN | POLLOUT)


static struct timeval *poll_gettimeout (int, int *);

int
poll(struct pollfd *fds, int nfds, int timeout)
{
	fd_set reads, writes;
	int i, rval, tsize;
	struct timeval *tv;


	/*
	 * If nfds is larger than the number of possible file descriptors
	 * or less than zero, simply return an error condition now.
	 */

	tsize = FD_SETSIZE;

	if (nfds < 0 || nfds > tsize) {
		errno = EINVAL;
		return (-1);
	}

		
	/* initialize the fd_sets and invalids */
	FD_ZERO(&reads);
	FD_ZERO(&writes);

	/*
	 * Run through the file descriptors and:
	 * a.  clear revents field
	 * b.  check event and place in appropriate fd_set
	 */
	for (i = 0; i < nfds; i++) {
		/* a.  clear revents field */
		fds[i].revents = 0;


		/* b.  check event and add */
		if ((fds[i].events == 0) || (fds[i].events & __INVALIDPOLL)) {
			errno = EINVAL;
			return (-1);
		}

		if ((fds[i].events & POLLIN) && ((int)fds[i].fd != -1))
			FD_SET((unsigned int)fds[i].fd, &reads);

		if ((fds[i].events & POLLOUT) && ((int)fds[i].fd != -1))
			FD_SET((unsigned int)fds[i].fd, &writes);
	}

	tv = poll_gettimeout(timeout, &rval);
	if (rval == -1) {
		errno = EAGAIN;
		return (-1);
	}

	/*
	 * Call select and loop through the descriptors again, checking
	 * for read and write events.
	 */

	errno = 0;

	rval = select(nfds, &reads, &writes, 0, tv);
	
	if (rval < 1)	 	/* timeout or error condition */
		return (rval);	/* errno will be set by select() */
		
	rval = 0;
	for (i = 0; i < nfds; i++) {
		if (fds[i].revents & POLLNVAL)
			continue;

		if (FD_ISSET(fds[i].fd, &reads))
			fds[i].revents |= POLLIN;

		if (FD_ISSET(fds[i].fd, &writes))
			fds[i].revents |= POLLOUT;

		if (fds[i].revents != 0)
			++rval;
	}

	return (rval);
}

static struct timeval *
poll_gettimeout(timeout, rval)
	int timeout;
	int *rval;
{
	struct timeval *tv;

	if (timeout < 0) {
		*rval = 0;
		return (NULL);
	} else {
		tv = (struct timeval *) malloc(sizeof(struct timeval));
		if (tv == NULL) {
			*rval = -1;
			return (NULL);
		}

		tv->tv_usec = timeout * 1000;
		tv->tv_sec = tv->tv_usec / 1000000;
		tv->tv_usec %= 1000000;

		return (tv);
	}
}



void reportWinsockError()
{
	int err;
	err = WSAGetLastError();
		switch( err ) {
			case 0:
				break;
			case WSANOTINITIALISED :
				dc_debug(DC_ERROR, "Winsock: WSANOTINITIALISED");
				break;
			case WSAENETDOWN :
				dc_debug(DC_ERROR, "Winsock: WSAENETDOWN");
				break;
			case WSAEAFNOSUPPORT :
				dc_debug(DC_ERROR, "Winsock: WSAEAFNOSUPPORT");
				break;
			case WSAEINPROGRESS :
				dc_debug(DC_ERROR, "Winsock: WSAEINPROGRESS");
				break;
			case WSAEMFILE :
				dc_debug(DC_ERROR, "Winsock: WSAEMFILE");
				break;
			case WSAENOBUFS :
				dc_debug(DC_ERROR, "Winsock: WSAENOBUFS");
				break;
			case WSAEPROTONOSUPPORT :
				dc_debug(DC_ERROR, "Winsock: WSAEPROTONOSUPPORT");
				break;
			case WSAEPROTOTYPE :
				dc_debug(DC_ERROR, "Winsock: WSAEPROTOTYPE");
				break;
			case WSAESOCKTNOSUPPORT :
				dc_debug(DC_ERROR, "Winsock: WSAESOCKTNOSUPPORT");
				break;

			case WSAENOTSOCK :
				dc_debug(DC_ERROR, "Winsock: WSAENOTSOCK");
				break;


			default:
				dc_debug(DC_ERROR, "Winsock: Unknown error [%d]", err);
				break;

			}

}


void initWinSock() {

	WORD wVersionRequested;
	WSADATA wsaData;
	int err;
 
	wVersionRequested = MAKEWORD( 1, 1 );
 
	err = WSAStartup( wVersionRequested, &wsaData );
	if ( err != 0 ) {
		/* Tell the user that we could not find a usable */
		/* WinSock DLL.                                  */
		switch( err ) {
			case 0: /* no error */
				break;
			case WSASYSNOTREADY :
				dc_debug(DC_ERROR, "Winsock_Init: WSASYSNOTREADY");
				break;

			case WSAVERNOTSUPPORTED :
				dc_debug(DC_ERROR, "Winsock_Init: WSAVERNOTSUPPORTED");
				break;

			case WSAEINPROGRESS :
				dc_debug(DC_ERROR, "Winsock_Init: WSAEINPROGRESS");
				break;

			case WSAEPROCLIM :
				dc_debug(DC_ERROR, "Winsock_Init: WSAEPROCLIM");
				break;

			case WSAEFAULT :
				dc_debug(DC_ERROR, "Winsock_Init: WSAEFAULT");
				break;

			default:
				dc_debug(DC_ERROR, "Winsock: Unknown error");
				break;

			}
		
		
		return;
	}
 
	/* Confirm that the WinSock DLL supports 1.1.*/
	/* Note that if the DLL supports versions greater    */
	/* than 1.1 in addition to 1.1, it will still return */
	/* 1.1 in wVersion since that is the version we      */
	/* requested.                                        */
 
	if ( LOBYTE( wsaData.wVersion ) != 1 ||
	    HIBYTE( wsaData.wVersion ) != 1 ) {
		/* Tell the user that we could not find a usable */
		/* WinSock DLL.                                  */
		WSACleanup( );
		return; 
	}
 
	/* The WinSock DLL is acceptable. Proceed. */
}
