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
 * $Id: fnal_thread.c,v 1.6 2004-11-01 19:33:29 tigran Exp $
 */
#define _REENTRANT
#define _MULTI_THREADED
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <math.h>

#include "dcap.h"

#define DATAFILE "dcap://scylla:22125/pnfs/desy.de/h1/user/tigran/DUMMY/index.dat"
/* #define DATAFILE "/scratch/usr/tigran/index.dat" */
#define BSIZE 512

int fd;
off_t size;

static long byteSwapL(unsigned long b);
static void *thread_task(void *arg);

long byteSwapL(unsigned long b)
{
	long r;

	r = (b << 24) | ((b & 0xFF00) << 8) |
         ((b & 0xFF0000L) >> 8) | (b >> 24);
	return r;
}

void *
thread_task(void *arg) {
  /* int fd; */
  int tid;
        int res;
        long value;
        int n;
        long offset;
        long rn;
        int ntrans;
        int rc;

        offset = 0;
        tid = ((int *)arg)[0];
        printf("thread %d: fd = %d\n", tid, fd);
        ntrans = 0;


        while (1) {

                rn = rand()%512 - 1 ;
                offset += rn*sizeof(value) ;
                if( offset >= size) {
                  printf("offset >= size: break\n");
                  break;
                }

                n = dc_pread(fd, &value, sizeof(value), offset);

                if(n <= 0) {
                  printf("dc_read return negative: break\n");
                  break;
                }

                offset += sizeof(value);

                if( ( offset != value ) && ( offset != byteSwapL(value)) ) {
                        printf("[%d] PANIC: value! tranfer # = %d ",
tid, ntrans);
                        printf("offset=%08X: value=%08X\n", offset,
value);
                        fflush(stdout);
						exit(11);
                        return NULL;
                } else {
                 /*  printf("offset=%08X: %08X\n", offset, value); */
                }

                if (++ntrans%10000 == 0) {
                  printf("[%d]: %d transfers cur. offset=%08X\n", tid, ntrans, offset);
                  fflush(stdout);
                }


        }

        return NULL;

}

int
main(int argc, char *argv[])
{
        int i;
        int tnum ;
        pthread_t *tr;
        int counter = 0;
        int res;
        int pargs[2];
        char *fname;
        int rc = 0;

        if (argc < 2) exit(1);

        if (argc == 3) {
          fname = argv[2];
        } else {
          fname = DATAFILE;
        }

        dc_setDebugLevel(3);

        fd = dc_open(fname, O_RDONLY);
        if(fd < 0 ) {
          dc_error("failed to open file:");
          return NULL;
        } else {
          printf("open %s, fd=%d\n", fname, fd);
		  /* keep read-ahead buffer for speed */
          /* dc_noBuffering(fd); */
        }


		size = dc_lseek(fd, 0L, SEEK_END);
		dc_lseek(fd, 0L, SEEK_SET);

        /* srand(time(NULL)); */
         tnum = atoi(argv[1]);
        tr = (pthread_t *)malloc(sizeof(pthread_t)*tnum);
        while(1)
        {
          for(i = 0; i < tnum; i++) {
            pargs[0] = i;
            pargs[1] = fd;
            pthread_create(&tr[i], NULL, thread_task, (void *)pargs);
            sleep(5);
          }

          for(i = 0; i < tnum; i++) {
            pthread_join(tr[i], NULL);
          }


          ++counter;

        }
}

