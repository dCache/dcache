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
 * $Id: dcap_checksum.h,v 1.4 2004-11-01 19:33:29 tigran Exp $
 */


#ifndef DCAP_CHECKSUM_H
#define DCAP_CHECKSUM_H

#include <sys/types.h>

#define ADLER32 1

#define DCAP_DEFAULT_SUM ADLER32

extern void update_checkSum(checkSum *, unsigned char *, size_t);
extern unsigned long initialSum( int );

#endif /* DCAP_CHECKSUM_H */
