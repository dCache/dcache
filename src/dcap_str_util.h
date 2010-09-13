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

typedef struct char_buf char_buf_t;

char_buf_t *dc_char_buf_create();
char *dc_char_buf_sprintf(char_buf_t *context, const char *format, ...);
void dc_char_buf_free(char_buf_t *context);

char *dc_safe_strncat( char *dest, size_t dest_size, char *src);
char *dc_snaprintf( char *dest, size_t dest_size, const char *format, ...);



#endif				/* _DCAP_STR_UTIL_H_ */


