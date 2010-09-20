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
 * $Id: parser.c,v 1.8 2004-11-01 19:33:30 tigran Exp $
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*
 * posible tokens:
 *
 * [0-9]* [a-zAZ]* "[a-zA-Z0-9 ]*" -key=value, where value can be any token
 */

#define MAXLINELEN 4096

#define Idle 0			/* 00000000 */
#define inToken 1		/* 00000001 */
#define inQuote 2		/* 00000010 */
#define inOption 4		/* 00000100 */

static char **CommandParser(const char *buffer);


char **
CommandParser(const char *buffer)
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
		switch (c) {
		case '\t':
		case ' ':

		case '\r':
			if (status & inQuote) {
				token[ti] = c;
				ti++;
				break;
			}
			if (status == Idle) {
				break;
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
			break;
		case '"':
			if (!(status & inQuote)) {
				if (!(status & inToken)) {
					status |= inToken;	/* set inToken */
					token = (char *) malloc(len - i + 1);
					ti = 0;
				}
				status |= inQuote;	/* set inQuote */
			} else {
				status ^= inQuote;	/* clear inQuote */
			}
			token[ti] = c;
			ti++;
			break;
		case '-':
			if (status == Idle) {
				status |= inOption | inToken;	/* set inOption and
								 * inToken */
				token = (char *) malloc(len - i + 1);
				ti = 0;
			}
			token[ti] = c;
			ti++;
			break;

		default:
			if (status == Idle) {
				status |= inToken;	/* set inToken */
				token = (char *) malloc(len - i + 1);
				ti = 0;
			}
			token[ti] = c;
			ti++;
			break;
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


#ifdef PARSER_MAIN

#define MSG " 0 0 server welcome 0 0 \"complex -message\" byebye  -a=-b -key=\"new value -keyoption\" "

main()
{

	char          **argv;
	int             i;

	printf("MESSAGE: [%s]\n", MSG);
	argv = CommandParser(MSG);

	for (i = 0; argv[i] != NULL; i++) {
		printf("%s\n", argv[i]);
		free(argv[i]);
	}
	free(argv);
}


#endif /* PARSER_MAIN */
