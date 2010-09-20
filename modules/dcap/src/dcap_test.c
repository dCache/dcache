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
 * $Id: dcap_test.c,v 1.37 2004-11-03 14:09:23 tigran Exp $
 */

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <sys/stat.h>
#include <time.h>
#include <grp.h>
#include <pwd.h>

#include "dcap.h"

#define DATAFILE "/pnfs/fs/usr/h1/user/tigran/DUMMY/index.dat"
/* #define DATAFILE "/scratch/usr/tigran/index.dat" */
#define BSIZE 512

int dup_fd[2]; /* to test a dup as well */
off_t size;


static int make_index(const char *path);
static long byteSwapL(unsigned long b);
static void mode2string(mode_t m, char *s);
static void thread_task();


int
make_index(const char *path)
{

  int fd;
  long counter = 0;
  long value = 0;


  fd = dc_creat(path, 0644);

  if( fd < 0 ) {
	dc_perror("dc_creat:");
        return 0;
  }

  dc_unsafeWrite(fd);
  dc_setBufferSize(fd, 1024*1024);

  while(counter != 100*1024*1024) {
     value += sizeof(value);
     dc_write(fd, &value, sizeof(value));
     counter++;
  }

  dc_close(fd);
  return 1;
}



long byteSwapL(unsigned long b)
{

	long r;

	r = (b << 24) | ((b & 0xFF00) << 8) |
         ((b & 0xFF0000L) >> 8) | (b >> 24);
	return r;
}



void mode2string(mode_t m, char *s)
{


	memset(s, '-', 10);

	if( S_ISREG(m) )  s[0] = '-';
	if( S_ISDIR(m) )  s[0] = 'd';
	if( S_ISCHR(m) )  s[0] = 'c';
	if( S_ISBLK(m) )  s[0] = 'b';
	if( S_ISFIFO(m) ) s[0] = 'F';
	if( S_ISLNK(m) )  s[0] = 'l';
	if( S_ISSOCK(m) ) s[0] = 's';


	if( m & S_IRUSR ) s[1] = 'r';
	if( m & S_IWUSR ) s[2] = 'w';
	if( m & S_IXUSR ) s[3] = 'x';

	if( m & S_IRGRP ) s[4] = 'r';
	if( m & S_IWGRP ) s[5] = 'w';
	if( m & S_IXGRP ) s[6] = 'x';

	if( m & S_IROTH ) s[7] = 'r';
	if( m & S_IWOTH ) s[8] = 'w';
	if( m & S_IXOTH ) s[9] = 'x';

	s[10] = '\0';

}


#define TESTBLOCK 27720

void
thread_task() {


	long value[TESTBLOCK];
	int n;
	long offset;
	long rn;
	int ntrans;
	int i;
	static int initFlag;
	int sw = 0;
	int fd;
	offset = 0;
	ntrans = 0;

	while (1) {
		sw = sw == 1 ? 0 : 1;

		fd = dup_fd[sw];

		rn = rand()%512 - 1 ;
		offset += rn*sizeof(long) ;
		if( (off_t)(offset + sizeof(long)*TESTBLOCK) >= size) {
			printf("offset(%ld) >= size(%ld): break\n", offset, size);
			break;
		}

		n = dc_pread(fd, value, sizeof(long)*TESTBLOCK, offset);
		if (initFlag == 0) {
			dc_setBufferSize( fd, 221760);
			initFlag++;
		}
		if(n != TESTBLOCK*sizeof(long)) {
			printf("dc_pread: expected %d , real %d. Offset %ld. Break.\n",
			TESTBLOCK*sizeof(long), n, offset);
			exit(3);
		}


		for( i = 0; i < TESTBLOCK; i++) {

			offset += sizeof(long);

			if( (value[i]  != offset) && ( offset != byteSwapL(value[i]) ) ){
				printf("PANIC: value! tranfer # = %d ", ntrans);
				printf("offset=%ld: value=%ld@%d\n", offset, value[i], i);
				fflush(stdout);
				exit(2) ;
			}

		}

		if (++ntrans%10 == 0) {
			printf("%d transfers cur. offset=%ld\n", ntrans, offset);
			fflush(stdout);
		}


	}

	return;

}

int
main(int argc, char *argv[])
{
	char *fname;
	struct stat s;
	struct passwd *pw;
	struct group *gr;
	char mode[11];

	if (argc > 2) exit(1);

	if (argc == 2) {
		fname = argv[1];
	} else {
		fname = DATAFILE;
	}

	dc_setStrDebugLevel("info");



	if( dc_access(fname, F_OK) < 0) {
		if( !make_index(fname) ) {
			exit(2);
		}
	}

	if( dc_lstat(fname, &s) < 0 ) {
		dc_perror("dc_lstat fialed: ");
		return -1;
	}else{

		pw = getpwuid(s.st_uid);
		gr = getgrgid(s.st_gid);
		mode2string(s.st_mode, mode);
		printf("dc_stat result:\n");
		printf("st_ino:     %ld\n", s.st_ino);
		printf("st_dev:     %lld\n", s.st_dev);
		printf("st_mode:    %s (0%o)\n", mode, s.st_mode);
		printf("st_nlink:   %d\n", s.st_nlink);
		printf("st_uid:     %s (%d)\n", pw == NULL ? "unknown" : pw->pw_name,  s.st_uid);
		printf("st_gid:     %s (%d)\n", gr == NULL? "unknown": gr->gr_name, s.st_gid);
		printf("st_size:    %lld\n", s.st_size);
		printf("st_blksize: %ld\n", s.st_blksize);
		printf("st_blocks:  %ld\n", s.st_blocks);
		printf("st_atime:   %s", ctime(&s.st_atime) );
		printf("st_mtime:   %s", ctime(&s.st_mtime) );
		printf("st_ctime:   %s", ctime(&s.st_ctime) );
		printf("\n");


	/*	if(pw != NULL) free(pw);
		if(gr != NULL) free(gr); */
	}

	srand(time(NULL));

	while(1)
	{

		dup_fd[0] = dc_open(fname, O_RDONLY);
		if(dup_fd[0] < 0 ) {
			dc_error("failed to open file:");
			exit(1);
		}

		printf("open %s, fd=%d\n", fname, dup_fd[0]);
		/* keep read-ahead buffer for speed */
		/* dc_noBuffering(fd); */


		if( dc_fstat(dup_fd[0], &s) < 0 ) {
			dc_perror("dc_fstat fialed: ");
			return -1;
		}else{

			pw = getpwuid(s.st_uid);
			gr = getgrgid(s.st_gid);
			mode2string(s.st_mode, mode);
			printf("dc_fstat result:\n");
			printf("st_ino:     %ld\n", s.st_ino);
			printf("st_dev:     %lld\n", s.st_dev);
			printf("st_mode:    %s (0%o)\n", mode, s.st_mode);
			printf("st_nlink:   %d\n", s.st_nlink);
			printf("st_uid:     %s (%d)\n", pw == NULL ? "unknown" : pw->pw_name,  s.st_uid);
			printf("st_gid:     %s (%d)\n", gr == NULL? "unknown": gr->gr_name, s.st_gid);
			printf("st_size:    %lld\n", s.st_size);
			printf("st_blksize: %ld\n", s.st_blksize);
			printf("st_blocks:  %ld\n", s.st_blocks);
			printf("st_atime:   %s", ctime(&s.st_atime) );
			printf("st_mtime:   %s", ctime(&s.st_mtime) );
			printf("st_ctime:   %s", ctime(&s.st_ctime) );
			printf("\n");


		/*	if(pw != NULL) free(pw);
			if(gr != NULL) free(gr); */
		}


		dup_fd[1] = dc_dup(dup_fd[0]);
		if(dup_fd[1] < 0 ) {
			dc_error("dc_dup failed");
			exit(1);
		}


		size = dc_lseek(dup_fd[0], (off_t)0, SEEK_END);
		dc_lseek(dup_fd[1], (off_t)0, SEEK_SET);


		thread_task();

		dc_close(dup_fd[0]);
		dc_close(dup_fd[1]);
	}
}

