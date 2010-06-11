#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include "dcap.h"
#include "dcap_types.h"




long byteSwapL(unsigned long b)
{

	long r;

	r = (b << 24) | ((b & 0xFF00) << 8) |
         ((b & 0xFF0000L) >> 8) | (b >> 24);
	return r;
}

int main(int argc, char *argv[])
{
	iovec2 vector[3];
	int fd;
	int rc;
	int i, j;
	long offset;
	long tbuf[1024];

	vector[0].buf = malloc(1024);
	vector[1].buf = malloc(1024);
	vector[2].buf = &tbuf[0];

	vector[0].len = 1024;
	vector[1].len = 1024;
	vector[2].len = 1024;


	fd = dc_open(argv[1], O_RDONLY);
	if( fd < 0 ) {
		dc_perror("open");
		exit(1);
	}



	for( j = 0; j < 1000; j ++ ) {



		vector[0].offset = 2 ;
		vector[1].offset = 512 ;
		vector[2].offset = 1024 + 4*(random()%4096);



		rc = dc_readv2(fd, vector, 3);
		if( rc < 0 ) {
			dc_perror("readv");
			exit(2);
		}

		offset = vector[2].offset;

		for( i = 0; i < 1024/4; i++) {

			offset += sizeof(long);

			if( (tbuf[i]  != offset) && ( offset != byteSwapL(tbuf[i]) ) ){
				printf(" PANIC: offset=%ld: value=%ld(%ld)@%d\n", offset, tbuf[i], byteSwapL(tbuf[i]), i);
				fflush(stdout);
				exit(2) ;
			}else{
				printf(" OK: offset=%ld: value=%ld\n", offset, tbuf[i]);
			}

		}

	}

	dc_close(fd);
}