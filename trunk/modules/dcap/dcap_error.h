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
 * $Id: dcap_error.h,v 1.3 2004-11-01 19:33:29 tigran Exp $
 */

#ifndef DCAP_ERROR
#define DCAP_ERROR

#include "dcap_errno.h"

/* thread-safe errno hack */
#ifdef _REENTRANT
extern int *__dc_errno();
#define dc_errno (*(__dc_errno()))
#else
extern int dc_errno;
#endif /* _REENTRANT */

extern void dc_error(const char *);
extern void dc_perror(const char *);
extern const char * dc_strerror(int);
extern void dc_setServerError(const char *);

#endif /* DCAP_ERROR */
