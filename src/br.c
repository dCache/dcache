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
 * $Id: br.c,v 1.4 2004-11-01 19:33:28 tigran Exp $
 */

#include <sys/stat.h>
#include <sys/types.h>

#include "dcap.h"

main(int argc, char *argv[])
{
	int             fd;
	time_t          t, tt = 0;
	off_t           size, tsize = 0, n;
	char           *buf;
	int             count;

	if(argc != 4) {
		printf("Usage: %s <file> <buffersize> <count>\n", argv[0]);
		exit(1);
	}
	count = atoi(argv[3]);

	dc_setDebugLevel(2);
	size = atoi(argv[2]);

	buf = (char *)malloc(size);
	fd = dc_creat(argv[1], 0644);
	t = time(NULL);
	while (count--) {
		n = dc_write(fd, buf, size);
		tsize += n;
		printf("%d ", count);
	}
	tt = time(NULL) - t;
	dc_close(fd);
	printf(" Speed = %.2f\n", ((float) tsize / (float) tt)/(float)1024);

}
