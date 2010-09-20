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
 * $Id: dcap_read.c,v 1.20 2007-07-09 19:33:18 tigran Exp $
 */

#include "dcap_types.h"

ssize_t dc_real_read( struct vsp_node *node, void *buff, size_t buflen);
ssize_t dc_readTo(int srcfd, int destdf, size_t size);

