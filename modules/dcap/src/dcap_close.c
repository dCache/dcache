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
 * $Id: dcap_close.c,v 1.8 2004-12-01 14:25:39 tigran Exp $
 */

#include <arpa/inet.h>
#include <stdlib.h>

#include "dcap.h"
#include "dcap_types.h"
#include "dcap_fsync.h"
#include "dcap_functions.h"
#include "dcap_protocol.h"
#include "dcap_lcb.h"
#include "dcap_poll.h"
#include "dcap_mqueue.h"
#include "dcap_reconnect.h"
#include "gettrace.h"
#include "io.h"
#include "node_plays.h"
#include "debug_level.h"
#include "array.h"
#include "system_io.h"

#define ENVAR_TIMEOUT  "DCACHE_CLOSE_TIMEOUT_DEFAULT"
#define ENVAR_TIMEOUT_OVERRIDE  "DCACHE_CLOSE_TIMEOUT_OVERRIDE"
#define ENVAR_TIMEOUT_VALUE_BASE   10

static int closeTimeOut_set, parsed_timeout;

static unsigned int closeTimeOut;
/**
 *  Check if the environment variable ENVAR_TIMEOUT is set.  If so,
 *  and the value is valid, then the value is used to set
 *  closeTimeOut.
 *
 *  This will always override any value set by client-code via
 *  dc_setCloseTimeout().  This is deliberate and in keeping with
 *  other dcap library environment variables.
 *
 *  This function is idempotence: it may be run multiple times with
 *  the same effect as running it once.
 */


static int validate_env_variable(char* timeout_var, long* timeout_val);
static void check_timeout_envar();


int
validate_env_variable(char* timeout_var, long* timeout_val)
{
	char *timeout_str, *end;
	timeout_str = getenv(timeout_var);
	if( timeout_str == NULL || *timeout_str == '\0') {
		return 0;
	}

	*timeout_val = strtol( timeout_str, &end, ENVAR_TIMEOUT_VALUE_BASE);
	if( end == timeout_str) {
		dc_debug( DC_INFO, "Invalid value for %s environment variable",timeout_var);
		return 0;
	}
	if( *end != '\0') {
       		dc_debug( DC_INFO, "Ignoring trailing garbage at end of %s value.", timeout_var);
	}
	if( *timeout_val < 0) {
	        dc_debug( DC_INFO, "Negative numbers are not allowed for %s environment variable.", timeout_var);
		return 0;
	}
	return 1;
}

void
check_timeout_envar()
{
	long timeout_val;
	if( parsed_timeout) {
		return;
	}

	/* Ensure we process the enviroment variable only once */
	parsed_timeout = 1;
	if( !closeTimeOut_set ) {
		if(validate_env_variable(ENVAR_TIMEOUT,&timeout_val)) {
			closeTimeOut = timeout_val;
		}
	}
	if(validate_env_variable(ENVAR_TIMEOUT_OVERRIDE,&timeout_val)) {
		closeTimeOut = timeout_val;
	}
}





int
dc_close(int fd)
{
	int             res = 0;
	int             tmp;
	int32_t         size;
	int32_t         closemsg[6];
	int             msglen;
	struct vsp_node *node;


#ifdef DC_CALL_TRACE
	showTraceBack();
#endif


	/* nothing wrong ... yet */
	dc_errno = DEOK;

	node = delete_vsp_node(fd);
	if (node == NULL) {
		/* we have not such file descriptor, so lets give a try to system */
		dc_debug(DC_INFO, "Using system native close for [%d].", fd);
		return system_close(fd);
	}


	if ( node->lcb != NULL ) {
		dc_lcb_clean( node );
	}

	dc_real_fsync( node );

	if(node->unsafeWrite > 1) {

		size = htonl(-1); /* send end of data */
		writen(node->dataFd, (char *) &size, sizeof(size), NULL);
		/* FIXME: error detection missing */

		if (get_fin(node) < 0) {
			dc_debug(DC_ERROR, "dc_close: mover did not FIN the data blocks.");
			res = -1;
		}
	}

	if(node->reference == 0 ) {

		if( (node->sum != NULL) && ( node->sum->isOk == 1 ) ) {
			closemsg[0] = htonl(20);
			closemsg[2] = htonl(12);
			closemsg[3] = htonl(DCAP_DATA_SUM);
			closemsg[4] = htonl(node->sum->type);
			closemsg[5] = htonl(node->sum->sum);

			msglen = 6;
			dc_debug(DC_INFO, "File checksum is: %u", node->sum->sum);
		}else{
			closemsg[0] = htonl(4);
			msglen = 2;
		}

		closemsg[1] = htonl(IOCMD_CLOSE); /* actual command */

		dc_debug(DC_IO, "Sending CLOSE for fd:%d ID:%d.", node->dataFd, node->queueID);
		check_timeout_envar();
		dcap_set_alarm(closeTimeOut > 0 ? closeTimeOut : DCAP_IO_TIMEOUT/4);
		tmp = sendDataMessage(node, (char *) closemsg, msglen*sizeof(int32_t), ASCII_OK, NULL);
		/* FIXME: error detection missing */
		if( tmp < 0 ) {
			dc_debug(DC_ERROR, "sendDataMessage failed.");
			/* ignore close errors if file was open for read */
			if(node->flags & O_WRONLY) {
				res = -1;
			}

			if(isIOFailed) {
				isIOFailed = 0;
				/* command line dwon */
				if(!ping_pong(node)) {
					/* remove file descriptor from the list of control lines in use */
					lockMember();
					deleteMemberByValue(node->fd);
					unlockMember();
					pollDelete(node->fd);
					system_close(node->fd);
				}
			}
		}
		dcap_set_alarm(0);

		close_data_socket(node->dataFd);
		deleteQueue(node->queueID);

	}



	node_destroy(node);

	return res;
}

/*
   dc_close2  - same as dc_close, but does not send close command to the
   pool.
*/

int
dc_close2(int fd)
{
	int              res = 0;
	struct vsp_node *node;
	int32_t          size;


#ifdef DC_CALL_TRACE
	showTraceBack();
#endif

	/* nothing wrong ... yet */
	dc_errno = DEOK;

	node = delete_vsp_node(fd);
	if (node == NULL) {
		/* we have not such file descriptor, so lets give a try to system */
		return system_close(fd);
	}


	dc_real_fsync( node );


	if(node->unsafeWrite) {

		size = htonl(-1); /* send end of data */
		writen(node->dataFd, (char *) &size, sizeof(size), NULL);
		/* FIXME: error detection missing */

		if (get_fin(node) < 0) {
			dc_debug(DC_ERROR, "dc_close: mover did not FIN the data blocks.");
			res = -1;
		}
	}

	close_data_socket(node->dataFd);

	deleteQueue(node->queueID);

	m_unlock(&node->mux);

	node_destroy(node);

	return res;
}



/*
 * close all open files
 */
void dc_closeAll()
{

	int i;
	fdList list = getAllFD();
	for( i = 0; i < list.len; i++) {
		dc_close(list.fds[i]);
	}


	if( list.len > 0 ) {
		free(list.fds);
	}
}

void dc_setCloseTimeout(unsigned int t)
{
	closeTimeOut_set = 1;
	closeTimeOut = t;
}

