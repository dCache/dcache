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
 * $Id: dcap_reconnect.h,v 1.10 2004-11-01 19:33:29 tigran Exp $
 */
#ifndef DCAP_RECONNECT_H
#define DCAP_RECONNECT_H

#include "dcap_types.h"

#define DCAP_IO_TIMEOUT 1200 /* if we are 20 minutes wating for date
								sonethig is wrong */

extern int recover_connection(struct vsp_node *, int);
extern int dcap_set_alarm(unsigned int);

#ifdef _REENTRANT
extern int * __isIOFailed();
#define isIOFailed (*(__isIOFailed()))
#else
extern int isIOFailed;
#endif /* _REENTRANT */

/* fault tolerant options */
#define DCFT_CONNECT_ONLY 0
#define DCFT_POSITION     2
#define DCFT_POS_AND_REED 1

extern int ping_pong(struct vsp_node *);

#endif /* DCAP_RECONNECT_H */
