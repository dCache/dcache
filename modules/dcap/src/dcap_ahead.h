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
 * $Id: dcap_ahead.h,v 1.8 2004-11-01 19:33:29 tigran Exp $
 */

#ifndef DCAP_AHEAD_H
#define DCAP_AHEAD_H

#include "dcap_types.h"

#define IO_BUFFER_SIZE 1048576 /* 1024*1024 */

extern void dc_setNodeBufferSize(struct vsp_node *, size_t);

#endif
