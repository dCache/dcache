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
 * $Id: dcap_signal.c,v 1.9 2004-11-01 19:33:29 tigran Exp $
 */

#include <signal.h>
#include <stdlib.h>
#include <unistd.h>
#ifndef __CYGWIN__
#    ifdef HAVE_STROPTS_H
#        include <stropts.h>
#    endif /* HAVE_STROPTS_H */
#endif /* __CYGWIN__ */

#include "dcap.h"
#include "dcap_signal.h"
#include "debug_level.h"

static void pipe_handler(int s);


void pipe_handler(int s)
{

	dc_debug(DC_ERROR, "SIGPIPE received");
	dc_debug(DC_ERROR, "->Signal handling is not fully implemented yet");
	dc_debug(DC_ERROR, "->Ignoring...");

	return;
}


int
dcap_signal()
{
	struct sigaction sa_pipe;

	sa_pipe.sa_handler = pipe_handler;
	sigemptyset(&sa_pipe.sa_mask);
	sa_pipe.sa_flags = 0;


	if (sigaction(SIGPIPE, &sa_pipe, NULL) < 0) {
		dc_debug(DC_ERROR,"Sigaction failed!");
	}

	return 0;
}
