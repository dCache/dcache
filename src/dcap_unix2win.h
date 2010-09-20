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
 * $Id: dcap_unix2win.h,v 1.6 2005-08-15 10:05:03 tigran Exp $
 */

#ifndef DCAP_UNIX2WIN_H
#define DCAP_UNIX2WIN_H

#include <io.h>

/* unix types compatibility */
typedef int ssize_t;
typedef unsigned int size_t;
typedef int mode_t;
typedef __int32 int32_t;
typedef __int64 int64_t;
typedef __int64 off64_t;


#define access _access
#ifndef MAXPATHLEN
#    define MAXPATHLEN 256
#endif

#ifndef PATH_MAX
#     define PATH_MAX 256
#endif

/* acees modes */
#ifndef F_OK
#   define F_OK 00
#endif

#ifndef W_OK
#   define W_OK 02
#endif

#ifndef R_OK
#   define R_OK 04
#endif



/* quick hack for nonblock flag */
#ifndef O_NONBLOCK
#    define O_NONBLOCK _O_SHORT_LIVED
#endif


/* shutdown flag */
#ifndef SHUT_RDWR
#    define SHUT_RDWR SD_BOTH
#endif

/* stat macros */
#define S_ISDIR(m)  (((m)&S_IFMT) == S_IFDIR)
#define S_ISCHR(m)  (((m)&S_IFMT) == S_IFCHR)


extern void initWinSock();
extern void reportWinsockError();



#define atoll(a) atol(a)

#endif
