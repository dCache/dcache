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
 * $Id: input_parser.c,v 1.10 2005-10-28 09:05:07 tigran Exp $
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifndef WIN32
#    include <unistd.h>
#else
#    include "dcap_win32.h"
#endif
#include "ioTunnel.h"
#include "io.h"

#define MAXLINELEN 4096

#define Idle 0			/* 00000000 */
#define inToken 1		/* 00000001 */
#define inQuote 2		/* 00000010 */
#define inOption 4		/* 00000100 */


char **inputParser(int fd, ioTunnel *en)
{

	char **argv = NULL;
	char *token = NULL;
	unsigned char status = Idle;
	unsigned char c;
	int		ti=0;
	char	**tmp;
	int		argc = 0;

	while (1) {

		if( readn(fd, (char *)&c, 1, en) <=0 ) break;

		/* only ASCII characters are allowed */
		if( c > 127 ) {
			return NULL;
		}

		if(ti >= MAXLINELEN) {
			--ti;
			status = inToken;
			c = '\n';
		}

		switch(c) {
		case '\t':
		case ' ':
		case '\n':
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
					token = (char *) malloc(MAXLINELEN);
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
				token = (char *) malloc(MAXLINELEN);
				ti = 0;
			}
			token[ti] = c;
			ti++;
			break;

		default:
			if (status == Idle) {
				status |= inToken;	/* set inToken */
				token = (char *) malloc(MAXLINELEN);
				ti = 0;
			}
			token[ti] = c;
			ti++;
			break;
		}

		if( (status == Idle) && (c == '\n') ) break;

	} /* while */

	return argv;
}

#ifdef PARSER_MAIN

#define MSG " 0 0 server welcome 0 0 \"complex \n -message\" byebye  -a=-b -key=\"new value -keyoption\" \n"

main()
{

	char          **argv;
	int             i;
	int p[2];

	printf("MESSAGE: [%s]\n", MSG);
	
	
	pipe(p);
	
	write(p[1], MSG, strlen(MSG));
	argv = inputParser(p[0]);

	for (i = 0; argv[i] != NULL; i++) {
		printf("%s\n", argv[i]);
		free(argv[i]);
	}
	free(argv);
}


#endif /* PARSER_MAIN */
