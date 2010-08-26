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
 * $Id: dcap_win_poll.h,v 1.2 2004-11-01 19:33:29 tigran Exp $
 */

#ifndef DCAP_WIN32_POLL_H
#define DCAP_WIN32_POLL_H



/*
 * Testable select events
 */
#define POLLIN          0x0001          /* fd is readable */
#define POLLPRI         0x0002          /* high priority info at fd */
#define POLLOUT         0x0004          /* fd is writeable (won't block) */
#define POLLRDNORM      0x0040          /* normal data is readable */
#define POLLWRNORM      POLLOUT
#define POLLRDBAND      0x0080          /* out-of-band data is readable */
#define POLLWRBAND      0x0100          /* out-of-band data is writeable */

#define POLLNORM        POLLRDNORM

/*
 * Non-testable poll events (may not be specified in events field,
 * but may be returned in revents field).
 */
#define POLLERR         0x0008          /* fd has error condition */
#define POLLHUP         0x0010          /* fd has been hung up on */
#define POLLNVAL        0x0020          /* invalid pollfd entry */

#define POLLREMOVE      0x0800  /* remove a cached poll fd from /dev/poll */


typedef struct pollfd {
		int fd;
		short events;
		short revents;
	} pollfd_t;

typedef unsigned long   nfds_t;

extern int poll(struct pollfd fds[], nfds_t nfds, int timeout);


#endif
