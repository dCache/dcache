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
 * $Id: dcap_debug.h,v 1.12 2004-11-01 19:33:29 tigran Exp $
 */

#ifndef _DCAP_DEBUG_H_
#define _DCAP_DEBUG_H_

#include "debug_level.h"

extern void dc_debug(unsigned int, const char *, ...);
extern void dc_setDebugLevel(unsigned int);
extern void dc_setStrDebugLevel(const char *);
extern void dc_setRecoveryDebugLevel();
extern void init_dc_debug();


#endif				/* _DCAP_DEBUG_H_ */
