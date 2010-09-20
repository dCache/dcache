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
 * $Id: dcap_poll.h,v 1.10 2004-11-01 19:33:29 tigran Exp $
 */
#ifndef DCAP_POLL_H
#define DCAP_POLL_H

#ifndef WIN32
#   include <poll.h>
#else
#   include "dcap_win_poll.h"
#endif

#include "dcap_types.h"

#define HAVETO -1
#define MAYBE 2

#define POLL_CONTROL 0
#define POLL_DATA 1

int dcap_poll(int mode, struct vsp_node *node, int what);
extern int pollAdd(int);
extern void pollDelete(int);


#endif
