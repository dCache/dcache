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
 * $Id: win32_libdl.h,v 1.2 2004-11-01 19:33:30 tigran Exp $
 */

#ifndef WIN32_LIBDL_H
#define WIN32_LIBDL_H

#define RTLD_LAZY 1
#define RTLD_NOW  2

extern void  *dlopen(const char *, int);
extern void  *dlsym(void *, const char *);
extern int   dlclose(void *);
extern char  *dlerror(void);

#endif
