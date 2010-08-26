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
 * $Id: lineparser.c,v 1.3 2004-11-01 19:33:30 tigran Exp $
 */

#include <stdlib.h>
#include <string.h>

#define Idle 0
#define inToken 1
#define MAXLINELEN 1024

char **
lineParser( const char *buffer, const char *separator)
{

	int             i;
	char            c;
	unsigned char   status = Idle;
	char          **argv = NULL;
	char          **tmp;
	char           *token = NULL;
	int             argc = 0;
	int             len;
	int             ti=0;

	if ((buffer == NULL) || (strlen(buffer) > MAXLINELEN)) {
		return NULL;
	}
	len = strlen(buffer);
	for (i = 0; i < len; i++) {

		c = buffer[i];
		if ( ( strchr(separator, c) != NULL ) || (c == '\r') || (c == '\n')){
			if (status == Idle) {
				continue;
			} else {
				/* end of token */
				token[ti] = '\0';
				status = Idle;
				argc++;
				/* FIXME: no error checkup */
				tmp = (char **) realloc(argv, sizeof(char *) * (argc + 1));
				if (tmp != NULL) {
					argv = tmp;
					argv[argc - 1] = (char *) strdup(token);
					free(token);
					argv[argc] = NULL;
				}
			}
		}else{

			if (status == Idle) {
				status = inToken;
				token = (char *) malloc(len - i + 1);
				ti = 0;
			}
			token[ti] = c;
			ti++;
			continue;
			
		}

	}


	if(status != Idle) {
	
		/* if we have incompite token - fix it */
		token[ti] = '\0';
		argc++;
		tmp = (char **) realloc(argv, sizeof(char *) * (argc + 1));
		if (tmp != NULL) {
			argv = tmp;
			argv[argc - 1] = (char *) strdup(token);
			argv[argc] = NULL;
		}
		free(token);
	}

	return argv;
}
