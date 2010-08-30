/*
 *   DCAP - dCache Access Protocol client interface
 *
 *   Copyright (C) 2000,2004 DESY Hamburg DMG-Division.
 *
 *   AUTHOR: Paul Millar (paul.millar@desy.de)
 *
 *   This program can be distributed under the terms of the GNU LGPL.
 *   See the file COPYING.LIB
 *
 */


/*
 * $Id:$
 */

#ifndef _DCAP_STR_UTIL_H_
#define _DCAP_STR_UTIL_H_

char *dc_safe_strncat( char *dest, size_t dest_size, char *src);
char *dc_snaprintf( char *dest, size_t dest_size, char *format, ...);

#endif				/* _DCAP_STR_UTIL_H_ */


