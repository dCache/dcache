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
 * $Id: dcap_interpreter.c,v 1.25 2006-07-17 15:13:36 tigran Exp $
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "dcap_types.h"
#include "dcap_command.h"
#include "dcap_mqueue.h"


typedef struct {
	char           *cmd;
	int            (*action) (char **, asciiMessage *);
}               command;


static command  command_table[] = {
	{"welcome",	do_command_welcome},
	{"reject",	do_command_reject},
	{"byebye",	do_command_byebye},
	{"failed",	do_command_fail},
	{"ok",		do_command_ok},
	{"retry",	do_command_retry},
	{"pong",    do_command_pong},
	{"stat",    do_command_stat},
	{"shutdown", do_command_shutdown},
	{"connect", do_command_connect},	
	{"hello",	NULL},
	{NULL,		NULL},
};


/*
 * DiskCache Door line format <sessionId> <commandId> <comPartner>
 * <requestCommand> [<requestArguments ...>]
 * 
 */


int
dcap_interpreter(const char **argv)
{

	int             i;
	asciiMessage     *result;

	if (argv == NULL) {
		return -1;
	}
		
	if ((argv[0] != NULL) && (argv[1] != NULL) && (argv[2] != NULL) && (argv[3] != NULL)) {

		result = ( asciiMessage *)malloc(sizeof(asciiMessage));
		if(result == NULL) {
			/* no memory */
			return -1;
		}

		result->destination = atoi(argv[0]);
		result->priority = atoi(argv[1]);
		result->msg = NULL;

		for (i = 0; command_table[i].cmd != NULL; i++) {
			if (strcmp(command_table[i].cmd, argv[3]) == 0) {
			    /* call command specific function, if it's exist */
				if (command_table[i].action != NULL) {
					command_table[i].action((char **) &argv[3], result);
				}
				
				queueAddMessage(result->destination, result);
				/* and go a way */
				return 0;	
			}
		}
		
		/* if we are here - command not recognized */
		do_command_dummy((char **)argv, result);
		
		/* no one need the result */
		free(result);
		return 0;

	} else {
		/* incomplite command */
		return -1;
	}

}
