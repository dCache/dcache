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
 * $Id: dcap_poll.c,v 1.61 2006-07-17 15:13:36 tigran Exp $
 */

#include <sys/types.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>

#ifndef WIN32
#    ifndef __CYGWIN__
#        ifdef HAVE_STROPTS_H
#            include <stropts.h>
#        endif /* HAVE_STROPTS_H */
#    endif /* __CYGWIN__ */
#    include <poll.h>
#    include <unistd.h>
#else
#    include "dcap_win_poll.h"
#endif /* WIN32 */

#include "io.h"
#include "input_parser.h"
#include "dcap.h"
#include "dcap_poll.h"
#include "dcap_interpreter.h"
#include "dcap_str_util.h"
#include "dcap_mqueue.h"
#include "dcap_reconnect.h"
#include "tunnelManager.h"
#include "debug_level.h"
#include "dcap_protocol.h"
#include "system_io.h"
#include "passive.h"

static          MUTEX(gLock);
static          MUTEX(controlLock);
static          COND(gCond);

static struct pollfd *poll_list = NULL;
static unsigned long poll_len = 1; /* keep one room for date chanel */
static unsigned long poll_len_inuse = 1; /* keep one room for date chanel */

/* Local function prototypes */
static void messageDestroy( char ** );
static const char *pevent2str(int event);
static void int_pollDelete(int fd);

#define POLL_CONTROL 0
#define POLL_DATA 1

#define APPEND_TO(TARGET,STRING) dc_safe_strncat(TARGET, sizeof(TARGET), STRING);

const char *pevent2str(int event )
{
    static char m[256];
    int ok = 0;

    m[0] = '\0';

    if( (event & POLLIN) == POLLIN ) {
        APPEND_TO(m, " POLLIN");
        ok = 1;
    }
    if( (event & POLLOUT) == POLLOUT) {
        APPEND_TO(m, " POLLOUT");
        ok = 1;
    }
    if( (event & POLLPRI) == POLLPRI ) {
        APPEND_TO(m, " POLLPRI");
        ok = 1;
    }
    if( (event & POLLERR)  == POLLERR ) {
        APPEND_TO(m, " POLLERR");
        ok = 1;
    }
    if( (event & POLLHUP)  == POLLHUP) {
        APPEND_TO(m, " POLLHUP");
        ok = 1;
    }
    if( (event & POLLNVAL)  == POLLNVAL ) {
        APPEND_TO(m, " POLLNVAL");
        ok = 1;
    }
#ifdef POLLMSG
    if( (event & POLLMSG)  == POLLMSG ) {
        APPEND_TO(m, " POLLMSG");
        ok = 1;
    }
#endif /* POLLMSG */
    if( (event & POLLRDNORM) == POLLRDNORM) {
        APPEND_TO(m, " POLLRDNORM");
        ok = 1;
    }
    if( (event & POLLWRBAND)  == POLLWRBAND ) {
        APPEND_TO(m, " POLLWRBAND");
        ok = 1;
    }
    if( (event & POLLRDBAND) == POLLRDBAND ) {
        APPEND_TO(m, " POLLRDBAND");
        ok = 1;
    }

    if( !ok ) {
        dc_snaprintf( m, sizeof(m), " UNKNOWN (%d)", event);
    }

    return m;
}


int
pollAdd(int fd)
{

	struct pollfd  *tmp_poll_list;

	m_lock(&gLock);

	if(poll_len == poll_len_inuse) {
		tmp_poll_list = (struct pollfd  *)realloc(poll_list, sizeof(struct pollfd) * (poll_len_inuse + 1));
		if (tmp_poll_list == NULL) {
			m_unlock(&gLock);
			return -1;
		}
		++poll_len_inuse;
		poll_list = tmp_poll_list;
	}

	poll_list[poll_len].fd = fd;
	poll_list[poll_len].events = POLLIN ;

	/* actualy,  we need it only once, but how to done it faster? */
	poll_list[0].events = POLLIN;

	++poll_len;
	m_unlock(&gLock);
	return 0;
}

void int_pollDelete(int fd)
{
	unsigned int i;

	for(i=1; i< poll_len; i++) {
		if(poll_list[i].fd == fd) {
			/* if not last element */
			if(i < poll_len -1) {
				poll_list[i].fd = poll_list[poll_len -1].fd;
				poll_list[i].events = poll_list[poll_len -1].events;
			}
			--poll_len;
			dc_debug(DC_INFO, "Removing [%d] form control lines list", fd);
			break;
		}
	}

}
void
pollDelete(int fd)
{
	m_lock(&gLock);

	int_pollDelete(fd);

	m_unlock(&gLock);
}


#define TIMEOUT 1000 /* milliseconds ( 1 second ) */

int
dcap_poll(int mode, struct vsp_node *node, int what)
{
	int             retval;
	unsigned long   i;
	int             rc;
	char          **msg;
	asciiMessage   *aM;
	struct pollfd   pfd;
	int            	isFound = 0;


	while(1) {
		/* do it untill data not here */

		m_lock(&controlLock);


		if( m_trylock(&gLock) == 0 ) {
		/* we got exclusive access to poll list */

			m_unlock(&controlLock);

			if(poll_list == NULL) {
				m_unlock(&gLock);
				return -1;
			}

			/* add the data chanel to the poll list */
			if( what == POLL_DATA ) {

				rc =  queueGetMessage(node->queueID, &aM);
				if( rc != -1 ) {

					switch(aM->type) {
						case ASCII_RETRY:
							free(aM->msg);
							free(aM);
							m_unlock(&gLock);

							m_lock(&controlLock);
							c_broadcast(&gCond);
							m_unlock(&controlLock);
							dc_debug(DC_INFO, "Retry for Queue [%d].", node->queueID);
							recover_connection(node, 1); /*FIXME: mode */
							continue;
						case ASCII_FAILED:
							free(aM->msg);
							free(aM);
							m_unlock(&gLock);

							m_lock(&controlLock);
							c_broadcast(&gCond);
							m_unlock(&controlLock);
							return -1;
						case ASCII_SHUTDOWN:
							/* Lost of control conection */
							free(aM->msg);
							free(aM);

							int_pollDelete(node->fd);
							/* file descriptor can be reused by system */
							system_close(node->fd);

							break;
						case ASCII_CONNECT:
							dc_debug(DC_INFO, "door in passive mode [%d].", node->queueID);

							poolConnectInfo *pool = (poolConnectInfo *)aM->msg;
							int rc = connect_to_pool(node, pool);

							m_unlock(&gLock);
							if(rc == 0 ) {
								dc_debug(DC_INFO, "Connected to %s:%d", pool->hostname, pool->port);
								free(pool->hostname);
								free(pool->challenge);
								free(pool);
								free(aM);

								return 0;
							}else{
								/* passive connection failed...waiting for active connection */
								dc_debug(DC_INFO, "Failed to connect to %s:%d, waiting for door", pool->hostname, pool->port);
								free(pool->hostname);
								free(pool->challenge);
								free(pool);
								free(aM);
								continue;
							}

							break;
						default:
							/* FIXME: what to do with anoder messages? */
							/* simply put it back ... */
							dc_debug(DC_INFO, "[%d] unexpected message (type=%d).",
								node->queueID, aM->type);
							queueAddMessage(node->queueID, aM);
					} /* switch */
				}

				poll_list[0].fd = node->dataFd;
			}else{
				/* No data  line,no control line...something wrong */
				if ( poll_len == 1 ) {
					dc_debug( DC_ERROR, "dcap_poll: noting to do");
					m_unlock(&gLock);
					m_lock(&controlLock);
					c_broadcast(&gCond);
					m_unlock(&controlLock);
					return -1;
				} else {
					poll_list[0].fd = -1;
				}
			}

#if defined(__linux__) || defined(__GNU__) || defined(__FreeBSD_kernel__)
again:
#endif /* linux */

			retval = poll(poll_list, (unsigned long) poll_len, mode);
			if (retval < 0) {
#if defined(__linux__) || defined(__GNU__) || defined(__FreeBSD_kernel__)
				/* on the linux the system calls are not automatically
				  restarted after interruption */
				/* be sure we are not interrupted by timeout */
				if(( errno == EINTR) && !isIOFailed ) {
					dc_debug(DC_INFO, "Restarting poll after interruption.");
					goto again;
				}
#endif
				m_unlock(&gLock);

				m_lock(&controlLock);
				c_broadcast(&gCond);
				m_unlock(&controlLock);

				return -1;
			}

			if(!retval) {
				/* if we are here, mode == MAYBE ... */
	    		/* there are no activity on selected files */

				m_unlock(&gLock);

				m_lock(&controlLock);
				c_broadcast(&gCond);
				m_unlock(&controlLock);

	    		return 0;
			}


			rc = 0; /* reset the error indicator */

			/* poll_list[0] reserver for data chanel and we will look from poll_list[1] */
			for (i = 1 ; i < poll_len ; i++) {

				/* check, that our control line know to the system */
				if( (what == POLL_CONTROL) && ( node != NULL ) && ( node->fd == poll_list[i].fd ) ){
					isFound = 1;
				}

				if (poll_list[i].revents & POLLIN) {
					msg = inputParser(poll_list[i].fd, getTunnelPair(poll_list[i].fd));

					if( (msg == NULL) || (poll_list[i].revents & POLLHUP) ||
							(poll_list[i].revents & POLLERR) || (poll_list[i].revents & POLLNVAL) ) {

						dc_debug(DC_ERROR, "Error (%s) (with data) on control line [%d]",pevent2str(poll_list[i].revents),  poll_list[i].fd);
						int_pollDelete(poll_list[i].fd);

						if( (what == POLL_CONTROL) && ( node != NULL ) && ( node->fd == poll_list[i].fd ) ) {
							rc = -1;
							break;
						} else {
							continue; /* try next cntrol connection */
						}

					}
					if ( dcap_interpreter((const char **)msg) < 0 ) {
						dc_debug(DC_INFO, "Incomplete message over control line [%d]",poll_list[i].fd);
					}
					messageDestroy(msg);

					/* we have got our message, skip others */
					if( (what == POLL_CONTROL) && ( node != NULL ) && ( node->fd == poll_list[i].fd ) ){
						break;
					}
				}

				/* remove control line if its in error state */
				if ( (poll_list[i].revents & POLLHUP) || (poll_list[i].revents & POLLERR) ||
							(poll_list[i].revents & POLLNVAL) ) {

					dc_debug(DC_ERROR, "Error (%s) on control line [%d]",pevent2str(poll_list[i].revents),  poll_list[i].fd);
					int_pollDelete(poll_list[i].fd);

					/* if it's our control line and we are waiting for control message...bad luck */
					if( (what == POLL_CONTROL) && ( node != NULL ) && ( node->fd == poll_list[i].fd ) ) {
						rc = -1;
						break;
					}
				}

				if( poll_list[i].revents != 0 ) {
					dc_debug(DC_TRACE, "dcap_pool: %s on control line [%d] id=%d",pevent2str(poll_list[i].revents),  poll_list[i].fd, i);
				}

			} /* loop over poll_list[i]  */

			m_unlock(&gLock);
			m_lock(&controlLock);
			c_broadcast(&gCond);
			m_unlock(&controlLock);

			if( what == POLL_DATA ) {

				dc_debug(DC_TRACE, "Polling data for destination[%d] queueID[%d].", node->dataFd, node->queueID);

				if(poll_list[0].revents & POLLIN) {
					/* ready for reading */
					return poll_list[0].fd;
				}

				if( (poll_list[0].revents & POLLHUP) || (poll_list[0].revents & POLLERR) || (poll_list[0].revents & POLLNVAL) ) {
					/* error condition. close the socket and reconnect */
					dc_debug(DC_ERROR, "[%d] Data connection in ERR or HUP state (%d).", node->dataFd, poll_list[0].revents );
					return -1;
				}

				continue;
			}

			/* this part reached if we are hunting for control messages */

			if( (node != NULL) && (isFound == 0) ) {
				dc_debug(DC_ERROR, "Control line [%d] unknow to the system", node->fd);
				rc = -1;
			}

			return rc;

		}else{
			/* list  is locked */

			/* we have to wait untill the lock will be free, but
				at the same time we can try to get our data */

			if ( (what == POLL_DATA) &&( node != NULL ) ) {

				m_unlock(&controlLock);

				pfd.fd = node->dataFd;
				pfd.events = POLLIN;

#if defined(__linux__) || defined(__GNU__) || defined(__FreeBSD_kernel__)
again2:
#endif /* linux */
				dc_debug(DC_INFO, "Alternative polling for [%d].",node->dataFd);
				retval = poll(&pfd, 1, TIMEOUT);

				if (retval < 0) {
#if defined(__linux__) || defined(__GNU__) || defined(__FreeBSD_kernel__)
				/* on the linux the system calls are not automatically
				  restarted after interruption */
					if(errno == EINTR) {
						dc_debug(DC_INFO, "Restarting poll after interruption.");
						goto again2;
					}
#endif
				}

				if(retval == 0) {
					continue;
				}

				if(pfd.revents & POLLIN) {
					dc_debug(DC_INFO, "Alternative POLL succeeded for [%d].", node->dataFd);
					return pfd.fd;
				}

				if( (pfd.revents & POLLHUP) || (pfd.revents & POLLERR) || (pfd.revents & POLLNVAL)) {
					dc_debug(DC_ERROR, "[%d] Data connection in ERR or HUP state (%d ).", node->dataFd, pfd.revents);
					return -1;
				}

				continue;

			}else{
				/* we waiting for control message */

				c_wait(&gCond, &controlLock);
				m_unlock(&controlLock);
				return 0;
			}
		}
	}

}


void
messageDestroy( char ** msg)
{
	register int i;

	if(msg == NULL)
		return;

	for(i=0; msg[i] != NULL; i++) {
		free(msg[i]);
	}

	free(msg);
}
