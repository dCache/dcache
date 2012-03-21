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
 * $Id: dcap_command.c,v 1.29 2006-09-26 07:40:16 tigran Exp $
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>

#include "dcap.h"
#include "dcap_types.h"
#include "dcap_protocol.h"
#include "dcap_debug.h"
#include "dcap_error.h"
#include "system_io.h"
#include "str2errno.h"
#include "string2stat.h"

int do_command_fail(char **argv, asciiMessage *result)
{

   result->msg = strdup(argv[2]);
   result->type = ASCII_FAILED;

   /*
    * The error message format is:
    *    <dCache error code> <error message> [POSIX error code]
    */

    /* Temporary hack for dc_check.
     * Pool Manager always should reply error 4 when file not cached
     */

   if(strcmp(argv[1], "4") == 0) {
        dc_errno = DENCACHED;
   }else{
        dc_setServerError(argv[2]);
   }

   /* set errno to errno  recived from  door */
   if( argv[3] != NULL ) {
        errno = str2errno(argv[3]);
   }

    dc_debug(DC_INFO, "Server error message for [%d]: %s (err code: %s, errno: %s).",
            result->destination, argv[2], argv[1], argv[3] );

    return 0;
}

int do_command_welcome(char **argv, asciiMessage *result)
{
   result->type = ASCII_WELCOME;
   dc_debug(DC_INFO, "Server reply: %s.", argv[0]);
   return 0;
}

int do_command_dummy(char **argv, asciiMessage *result)
{
	int i=0;
	system_write(2, "Unknown reply from server: \"", 27);

	while(argv[i] != NULL ){
		system_write(2, argv[i], strlen(argv[i]) );
		system_write(2, " ", 1);
		i++;
	}

	system_write(2, "\"\n", 2);
	return 0;
}

int do_command_reject(char **argv, asciiMessage *result)
{
   result->type = ASCII_REJECTED;
   dc_debug(DC_ERROR, "Server rejected us!");
   return 0;
}

int do_command_byebye(char **argv, asciiMessage *result)
{
   result->type = ASCII_BYE;
   dc_debug(DC_INFO, "Server had dropped down control connection!");
   return 0;
}

int do_command_ok(char **argv, asciiMessage *result)
{

   result->type = ASCII_OK;
   dc_debug(DC_INFO, "Server reply: %s destination [%d].", argv[0], result->destination);
   return 0;
}

int do_command_retry(char **argv, asciiMessage *result)
{
   result->type = ASCII_RETRY;
   dc_debug(DC_INFO, "Server requested to retry: destination [%d].", result->destination);
   return 0;
}

int do_command_pong(char **argv, asciiMessage *result)
{
   result->type = ASCII_PING;
   dc_debug(DC_INFO, "Server reply: Pong. Destination [%d].", result->destination);
   return 0;
}

int do_command_stat(char **argv, asciiMessage *result)
{
#ifdef WIN32
    struct _stati64 *s;
#else
	struct stat64 *s;
#endif

	result->type = ASCII_STAT;
#ifdef WIN32
	s = (struct _stati64 *)malloc( sizeof(struct _stati64) );
#else
	s = (struct stat64 *)malloc( sizeof(struct stat64) );
#endif
	string2stat64( ( const char **)argv, s);
	result->msg = (char *)s;

	return 0;
}

int do_command_shutdown(char **argv, asciiMessage *result)
{
   result->type = ASCII_SHUTDOWN;
   dc_debug(DC_ERROR, "Control line going to shutdown.");
   return 0;
}


int do_command_connect(char **argv, asciiMessage *result)
{
	poolConnectInfo *pool;
	dc_debug(DC_INFO, "'connect to %s:%s' received for [%d]", argv[1], argv[2], result->destination);

	pool = (poolConnectInfo *)malloc( sizeof(poolConnectInfo) );


	pool->hostname = strdup(argv[1]);
	pool->port = strdup(argv[2]);
	pool->challenge = strdup(argv[3]);


	result->msg = (char *)pool;
	result->type = ASCII_CONNECT;
	return 0;
}
