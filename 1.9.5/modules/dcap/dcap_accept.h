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
 * $Id: dcap_accept.h,v 1.3 2004-11-01 19:33:28 tigran Exp $
 */
#ifndef DCAP_ACCEPT_H
#define DCAP_ACCEPT_H

#include "dcap_types.h"

extern int queueAddAccepted(int, int);
extern int queueGetAccepted(int);

#endif
