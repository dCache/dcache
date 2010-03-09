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
 * $Id: wdccp.c,v 1.6 2006-07-17 15:13:36 tigran Exp $
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

#define DEFAULT_BUFFER 1048570L /* 1Mb */


#ifdef WIN32
#	define PATH_SEPARATOR '\\'
#	define STDIN "CON"
#	define STDOUT "CON"
#else
#	define PATH_SEPARATOR '/'
#	define STDIN "-"
#	define STDOUT "-"
#	define O_BINARY 0 /* there is no BINARY OPEN in unix world */
#endif /* WIN32 */


static void usage();
static int copyfile(int src, int dest, size_t buffsize, off_t  *size);
static int file2file( const char *, const char *, int, size_t, int, int);
static char *path2filename(const char *, const char *);


/**
  *  path2filename
  *  creates a new filename from path + basename(filename)
  *  returns NULL on error
  */


static
char *path2filename(const char *path, const char *filename)
{
	char *newFile;
	char *oFile;
	int dirLen;
	int fileLen;

	oFile = strrchr( filename, PATH_SEPARATOR );
	if( oFile != NULL ) {
		++oFile;
	}else{
		oFile = (char *)filename;
	}

	dirLen = strlen(path);
	if( path[ dirLen -1 ] ==  PATH_SEPARATOR ) {
		--dirLen;
	}
	fileLen = strlen(oFile);


	/* 2 extra butes for '/' and '\0' at the end */
	newFile = malloc( dirLen + fileLen + 2 );
	if( newFile == NULL ) {
		perror("malloc: ");
		return NULL;
	}

	memcpy(newFile, path, dirLen);
	newFile[dirLen] = PATH_SEPARATOR ;
	memcpy( newFile + dirLen + 1, oFile, fileLen);
	newFile[dirLen + 1 + fileLen] = '\0';

	return newFile;
}


/**
  *  file2file
  *  copies source to destination
  *  if destination is a directory, then destination + basename(source)
  *  used as final destination
  *  if overwrite flag is not defined, existing files are skipped.
  *  stdin and adtout can be used as source or destination
  *  returns -1 on error and 0 on success
  */


static
int file2file( const char *source, const char *destination, int overwrite, size_t buffer_size, int unsafeWrite, int doCheckSum)
{
	int rc = 0;
	int src;
	int dest;
	time_t starttime, endtime, copy_time;
	off_t size;
	int isStdin = 0;
	int isStdout = 0;
	struct stat sbuf;
	char extracommand[128];
	char *real_destination;
	int isDir = 0;

#ifdef WIN32
	mode_t mode = _S_IWRITE;
#else
	mode_t mode = 0666;
#endif /* WIN32 */

	errno = 0;

	if(strcmp(source, STDIN) == 0) {
		src = fileno(stdin);
		isStdin = 1;
	}

	if(strcmp(destination, STDOUT) == 0) {
		dest = fileno(stdout);
		isStdout = 1;
	}

	if( ! isStdin ) {

		rc = stat(source, &sbuf) ;
		if( (rc == 0) && ( S_ISDIR(sbuf.st_mode) || S_ISCHR(sbuf.st_mode)) ) {
			fprintf(stderr,"file %s: Not a regular file\n",source);
			return -1 ;
		}

		if( rc == 0 ) {
			/* if file do not exist it can be a url, and
				dc_open will handle this */
			mode = sbuf.st_mode & 0777;
			/* tell to pool how many bytes we want to write */
			extracommand[0] = '\0';
			sprintf(extracommand, "-alloc-size=%ld",sbuf.st_size);
			dc_setExtraOption(extracommand);
		}
	}

	if( ! isStdout ) {

		if ( (stat( destination, &sbuf) == 0) &&  S_ISDIR(sbuf.st_mode) ) {
			isDir = 1;
			real_destination = path2filename(destination, isStdin ? "stdin" : source);
			if( real_destination == NULL ) {
				return -1;
			}
		}else{
			real_destination = (char *)destination;
		}

		if( overwrite && (access(real_destination, F_OK) == 0)) {
			fprintf(stderr, "   Skipping existing file %s.\n", real_destination);
			if( isDir ){
				free(real_destination);
			}
			return 0;
		}
	}

	if( !isStdin ) {
		src = dc_open(source, O_RDONLY|O_BINARY);
		if( src < 0 ) {
			dc_perror("Can't open source file");
			return -1;
		}

/*		dc_noBuffering(src); */
	}

	if( ! isStdout ) {
		dest = dc_open(real_destination, O_WRONLY|O_CREAT|O_TRUNC|O_BINARY, mode | S_IWUSR);
		if( dest < 0 ) {
			dc_perror("Can't open destination file");
			if( ! isStdin ) {
				dc_close(src);
			}
			return -1;
		}

		if( unsafeWrite ) {
			dc_unsafeWrite( dest );
		}

		if( ! doCheckSum ) {
			dc_noCheckSum( dest );
		}
	}

	time(&starttime);
	rc = copyfile(src, dest, buffer_size, &size);
	time(&endtime);

	if ( ! isStdin && (dc_close(src) < 0) ) {
		dc_perror("Failed to close source file");
		rc = -1;
	}

	if (! isStdout ) {
		if (dc_close(dest) < 0) {
			dc_perror("Failed to close destination file");
			rc = -1;
		}

		if( isDir ) {
			free(real_destination);
		}
	}

	if (rc != -1 )  {
		copy_time = endtime-starttime ;
		fprintf(stderr,"%s => %s: %lld bytes in %lu seconds",source, destination, (off_t)size, copy_time);
		if ( copy_time > 0) {
			fprintf(stderr," (%.2f KB/sec)\n",(double)size/(double)(1024*copy_time) );
		}else{
			fprintf(stderr,"\n");
		}
	}else{
		fprintf(stderr,"Failed to copy %s to %s.", source, destination);
	}

	return rc;
}




/**
  *  copyfile
  *  copies data form src to dest
  *  returns -1 on error and 0 on success
  *  the size will return the number of copied bytes
  */


static
int copyfile(int src, int dest, size_t bufsize, off_t *size)
{
	ssize_t n, m ;
	char * cpbuf;
	size_t count;
	off_t total_bytes = 0;
	size_t off;


	if ( ( cpbuf = malloc(bufsize) ) == NULL ) {
		perror("malloc");
		return -1;
	}

	do{
		off = 0;
		do{
			n = dc_read(src, cpbuf + off, bufsize - off);
			if( n <=0 ) break;
			off += n;
		} while (off != bufsize );

		/* do not continue if read fails*/
		if (n < 0) {
			/* Read failed. */
			free(cpbuf);
			return -1;
		}

		if (off > 0) {
			count = 0;

			total_bytes += off;
			while ((count != off) && ((m = dc_write(dest, cpbuf+count, off-count)) > 0))
				count += m;

			if (m < 0) {
				/* Write failed. */
				free(cpbuf);
				return -1;
			}
		}

	} while (n != 0);

	if(size != NULL) {
		*size = total_bytes;
	}

	free(cpbuf);
	return 0;
}



/**
  *  usage
  *  printn's some help
  */


static
void usage()
{
	fprintf(stderr,"DiskCache Copy Program. LibDCAP version: %d.%d.%d-%s\n",
		dc_getProtocol(),
		dc_getMajor(),
		dc_getMinor(),
		dc_getPatch());
	fprintf(stderr,"Usage:  dccp [-d <debugLevel>]  [-h <replyhostname>] [-i]\n");
	fprintf(stderr,"    [-B <bufferSize>] [-X <extraOption>]\n");
	fprintf(stderr,"    [-u] [-p <first port>[:last port]]\n");
	fprintf(stderr,"    [-r <buffers size>] [-s <buffer size>] [-n] [-c] <src> <dest>\n");

	/* some help */

	fprintf(stderr, "\n\n");
	fprintf(stderr, "\t-A                            : active client mode ( client connects to the pool).\n");
	fprintf(stderr, "\t-d <debugLevel>               : set debug level.\n");
	fprintf(stderr, "\t-h <replyhostname>            : specify hostname for data connection.\n");
	fprintf(stderr, "\t-i                            : do not overwrite existing files.\n");
	fprintf(stderr, "\t-o <seconds>                  : specify timeout for the 'open' operation\n");
	fprintf(stderr, "\t-B                            : specify transfer buffer size (default=%ld).\n",DEFAULT_BUFFER);
	fprintf(stderr, "\t-X <extraOption>              : add extra wishes into \"open\" request.\n");
	fprintf(stderr, "\t-u                            : enable unsafe write operations.\n");
	fprintf(stderr, "\t-p <first port>[:<last port>] : specify port range number for data connection.\n");
	fprintf(stderr, "\t-T <plugin name>              : specify control line IO tunneling plugin.\n");
	fprintf(stderr, "\t-w <tunnel type>              : specify tunnel type.\n");
	fprintf(stderr, "\t-r <buffer size>              : specify TCP receive buffer size.\n");
	fprintf(stderr, "\t-s <buffer size>              : specify TCP send buffer size.\n");
	fprintf(stderr, "\t-n                            : do not continue multi-file copy on error.\n");
	fprintf(stderr, "\t-c                            : disable chacksum calculation.\n");
	exit(1);
}


/**
  *  main
  *  entry point for any application
  */


int
main(int argc, char *argv[])
{
	struct stat sbuf;
	size_t buffer_size = DEFAULT_BUFFER; /* transfer buffer size */
	int rc = 0;
	char *outfile;
	int overwrite = 1;
	char *firstP, *lastP;
	unsigned short first_port, last_port;
	int file_count;
	int isMulti = 0;
	int stopOnError = 0;
	int unsafeWrite = 0;
	int doCheckSum = 1;

	/* for getopt */
	int c;
	extern char *optarg;
	extern int optind;


	if (argc < 3) {
		usage();
	}

	while( (c = getopt(argc, argv, "Ad:o:h:iX:PB:p:T:r:s:w:nuc")) != EOF) {

		switch(c) {
			case 'd':
				dc_setStrDebugLevel(optarg);
				break;
			case 'o':
				dc_setOpenTimeout(atol(optarg));
				break;
			case 'h':
				dc_setReplyHostName(optarg);
				break;
			case 'i':
				overwrite = 0;
				break;
			case 'n':
				stopOnError = 1;
				break;
			case 'B' :
				buffer_size = atol(optarg);
				break;
			case 'X':
				dc_setExtraOption(optarg);
				break;
			case 'u':
				unsafeWrite = 1;
				break;
			case 'p':
				lastP = strchr(optarg, ':');
				if( lastP == NULL ) {
				    first_port = atoi(optarg);
					last_port = first_port;
				}else{
				    firstP = optarg;
					lastP[0] = '\0';

				    first_port = atoi(firstP);
					last_port = atoi(lastP +1);

				}

				dc_setCallbackPortRange(first_port, last_port);
				break;
			case 'T':
				dc_setTunnel(optarg);
				break;
			case 'r':
				dc_setTCPReceiveBuffer( atoi(optarg) );
				break;
			case 's':
				dc_setTCPSendBuffer( atoi(optarg) );
				break;
			case 'w':
				dc_setTunnelType(optarg);
				break;
			case 'c':
				doCheckSum = 0;
			case 'A':
				dc_setClientActive();
				break;
			case '?':
				usage();
		}
	}

	file_count = argc - optind;

	if( file_count < 2 ) {
		usage();
	}

	if( file_count > 2 ) {
		isMulti = 1;
	}

	outfile = argv[argc - 1];

	if( isMulti ) {
		if( stat( outfile, &sbuf) != 0 ||  !S_ISDIR(sbuf.st_mode) ){
			fprintf(stderr, "%s: copying multiple files, but target %s must be a directory.\n",
						argv[0], outfile);
			exit(2);
		}
	}

	rc = 0;

	do {

		rc |= file2file(argv[argc - file_count], outfile,overwrite,  buffer_size , unsafeWrite, doCheckSum);

		if( (rc != 0) && stopOnError )
			break;

	}while(--file_count  > 1);

	return rc;
}
