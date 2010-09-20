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
 * $Id: dcap_protocol.c,v 1.11 2006-03-28 11:42:49 tigran Exp $
 */


#include <stdlib.h>

#include "dcap_protocol.h"

#define DCAP_MAX_COMMAND 14

static const char *commandTable[] = {
	 "hello" ,        /* 0 */
	 "open"  ,        /* 1 */
	 "stage" ,        /* 2 */
	 "check" ,        /* 3 */
	 "stat"  ,        /* 4 */
	 "fstat" ,        /* 5 */
	 "lstat" ,        /* 6 */
	 "open"  ,        /* 7 (truncate)*/
	 "unlink",        /* 8 */
	 "rmdir" ,        /* 9 */
	 "mkdir" ,        /* 10 */
	 "chmod",         /* 11 */
	 "opendir",       /* 12 */
	 "rename",        /* 13 */
	 "chown"         /* 14 */
};


const char* asciiCommand( unsigned int cmd )
{
	return ( cmd > DCAP_MAX_COMMAND) ? NULL : commandTable[cmd];
}
