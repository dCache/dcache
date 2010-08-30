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
 * $Id: dcsuck.c,v 1.2 2007-06-07 08:00:06 tigran Exp $
 */


#include <fcntl.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <limits.h>
#ifndef WIN32
#    include <sys/param.h>
#    include "dcap_signal.h"
#else
#    include "dcap_win32.h"
extern int getopt(int, char * const *, const char *);
#endif

#include "dcap.h"
#include "dcap_str_util.h"

#define DEFAULT_BUFFER 1048570L /* 1Mb */

#ifndef MAXPATHLEN
#define MAXPATHLEN 4096
#endif

int main(int argc, char *argv[])
{

	int  src, dest, c;
	struct stat64 sbuf;
	ssize_t data_len;
	int rc;
	char extraOption[MAXPATHLEN];



	if( argc < 3 ) {
		fprintf(stderr,"usage: %s <path in dcache> <local path>\n",argv[0]);
		exit(1);
	}


	extraOption[0] = '\0';

	while( (c = getopt(argc, argv, "Ad:o:h:iX:Pt:l:aB:b:up:T:r:s:w:c")) != EOF) {

		switch(c) {
			case 'd':
				dc_setStrDebugLevel(optarg);
				break;
			case 'X':
				dc_snaprintf(extraOption, sizeof(extraOption),
				             " %s", optarg);
				break;
		}
	}


	rc = dc_stat64(argv[optind], &sbuf);

	if ( (rc == 0) && ( S_ISDIR(sbuf.st_mode) || S_ISCHR(sbuf.st_mode)) ) {
		fprintf(stderr,"file %s: Not a regular file\n",argv[optind]);
		return -1 ;
	}

	rc = 0;
	dc_setExtraOption(extraOption);
	src = dc_open(argv[optind], O_RDONLY );
	if( src < 0 ) {
		dc_perror("Can't open source file");
		return -1;
	}
	dest = dc_open( argv[optind+1], O_WRONLY|O_CREAT|O_TRUNC, sbuf.st_mode & 0777|S_IWUSR);
	if( dest < 0 ) {
		dc_perror("Can't open destination file");
		dc_close(src);
		return -2;
	}

	data_len = dc_readTo(src, dest, sbuf.st_size);
	if( data_len != sbuf.st_size) {
		fprintf(stderr,"recived data (%lld) .NE. file size(%lld) \n", data_len, sbuf.st_size);
		rc = -3;
	}

	dc_close(src);
	close(dest);

	return rc;
}
