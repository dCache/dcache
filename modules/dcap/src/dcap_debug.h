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

#include <stdarg.h>

#include "debug_level.h"

#define MAX_MESSAGE_LEN 2048

extern void dc_setRecoveryDebugLevel();
void dc_vdebug(unsigned int level, const char *format, va_list ap);
int dc_is_debug_level_enabled( unsigned int level);

#endif				/* _DCAP_DEBUG_H_ */
