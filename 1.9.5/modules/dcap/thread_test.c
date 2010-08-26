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
 * $Id: thread_test.c,v 1.8 2004-11-01 19:33:30 tigran Exp $
 */
#define _REENTRANT
#include <stdio.h>
#include <pthread.h>

#include "dcap.h"

#define DATAFILE "/pnfs/fs/usr/dmg/eagle/tigran/dcap.c"
#define BSIZE 512

void *
thread_task(void *arg) {
	int fd;
	int res;
	char buf[BSIZE];
	int n;
	int totsize = 0;

	fd = dc_open( DATAFILE, O_RDONLY);
	if(fd < 0 ) {
		dc_error("faild to open file:");
		return NULL;
	}
	
	/*
	
	while(1) {
		n = dc_read(fd, buf, BSIZE);
		if(n <= 0) break;
		totsize +=n;
	}
	
	printf("Thread %d readed %d bytes\n", pthread_self(), totsize);	
	
	*/
	
	res = dc_close(fd);
	
	if(res < 0 ) {
		dc_error("faild to close file:");
		return NULL;
	}

	return NULL;

}


main(int argc, char *argv[])
{
	int i;
	int tnum;
	pthread_t *tr;
	int counter = 0;
	
	if(argc != 2) exit(1);
	
	dc_setDebugLevel(2);
	
	tnum = atoi(argv[1]);
	tr = (pthread_t *)malloc(sizeof(pthread_t)*tnum);

	while(1) {
	
		for(i = 0; i < tnum; i++) {
			pthread_create(&tr[i], NULL, thread_task, (void *)NULL);
		}

		for(i = 0; i < tnum; i++) {
			pthread_join(tr[i], NULL);
		}
	
		++counter;
		printf("Loop No: %d passed threads: %d\n", counter, counter*tnum);
	
	}
}
