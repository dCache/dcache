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
 * $Id: dccp.c,v 1.77 2007-02-22 10:19:46 tigran Exp $
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
#    include "dcap_unix2win.h"
extern int getopt(int, char * const *, const char *);
#endif

#include "dcap.h"
#include "dcap_str_util.h"

#define DEFAULT_BUFFER 1048570L /* 1Mb */


#ifdef WIN32
#    define PATH_SEPARATOR '\\'
#    define S_IWUSR 0 /* */
#else
#    define PATH_SEPARATOR '/'
#    ifndef O_BINARY
#       define O_BINARY 0 /* there is no BINARY OPEN in unix world */
#    endif /* O_BINARY */
#endif /* WIN32 */

#ifndef MAXPATHLEN
#define MAXPATHLEN 4096
#endif

static void usage();
static int copyfile(int src, int dest, size_t buffsize, off64_t  *size);

int main(int argc, char *argv[])
{

	int  src, dest;
	struct stat64 sbuf, sbuf2;
	time_t starttime, endtime, copy_time;
	off64_t size=0;
	size_t buffer_size = DEFAULT_BUFFER; /* transfer buffer size */
	int rc ;
	char filename[MAXPATHLEN],*inpfile, *outfile;
	char extraOption[MAXPATHLEN];
	char allocSpaceOption[MAXPATHLEN];
	char *cp ;
	int c;
	int overwrite = 1;
	int isStdin = 0;
	mode_t mode = 0666;
	char *firstP, *lastP;
	unsigned short first_port, last_port;
	int stage = 0;
	int stagetime = 0;
	int unsafeWrite = 0;
	char *stagelocation = NULL;
	int ahead = 0;
	size_t ra_buffer_size = 1048570L;
	int doCheckSum = 1;

	/* for getopt */
	extern char *optarg;
	extern int optind;


	if (argc < 3) {
		usage();
	}

	extraOption[0] = '\0';
	allocSpaceOption[0] = '\0';

	while( (c = getopt(argc, argv, "Ad:o:h:iX:Pt:l:aB:b:up:T:r:s:w:cC:")) != EOF) {

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
			case 'P':
				stage = 1;
				break;
			case 't' :
				stagetime = atoi(optarg);
				break;
			case 'l' :
				stagelocation = optarg;
				break;
			case 'B' :
				buffer_size = atol(optarg);
				break;
			case 'a' :
				ahead = 1;
				break;
			case 'b' :
				ra_buffer_size = atol(optarg);
				break;
			case 'X':
				dc_snaprintf(extraOption, sizeof(extraOption), " %s", optarg);
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
				    firstP = optarg; /*just to be simple */
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
				break;
			case 'A':
				dc_setClientActive();
				break;
			case 'C':
				dc_setCloseTimeout(atoi(optarg));
				break;
			case '?':
				usage();

		}
	}

	if(((argc - optind) != 2) && (!stage)) {
		usage();
	}

#ifndef WIN32
	dcap_signal();
#endif

	inpfile = argv[optind];
	if(stage) {
		dc_setExtraOption(extraOption);
		if ( (rc = dc_stage(inpfile, stagetime, stagelocation)) < 0 ) {
			dc_perror("dc_stage fail");
			rc = -1;
		}
		return rc;
	}

	outfile = argv[optind+1];

#ifndef WIN32
	if(strcmp(inpfile, "-") == 0) {
		isStdin = 1;
		src = fileno(stdin);
		inpfile = strdup("/dev/stdin");
	}

	if(strcmp(outfile, "-") == 0) {
		outfile = strdup("/dev/stdout");
	}
#endif /* WIN32 */

	if(!isStdin) {
		dc_setExtraOption(extraOption);
		rc = dc_stat64(inpfile, &sbuf);
		if ( (rc == 0) && ( S_ISDIR(sbuf.st_mode) || S_ISCHR(sbuf.st_mode)) ) {
			fprintf(stderr,"file %s: Not a regular file\n",inpfile);
			return -1 ;
		}

		if( rc == 0 ) {
			/* if file do not exist it can be a url, and
				dc_open will handle this */
			mode = sbuf.st_mode & 0777;
			/* tell to pool how many bytes we want to write */
#ifdef WIN32
			dc_snaprintf(allocSpaceOption, sizeof(allocSpaceOption),
			             " -alloc-size=%lld", (__int64)sbuf.st_size);
#else
			dc_snaprintf(allocSpaceOption, sizeof(allocSpaceOption),
			             " -alloc-size=%lld", (long long)sbuf.st_size);
#endif
		}
	}

	dc_setExtraOption(extraOption);
	if ( dc_stat64( outfile, &sbuf2) == 0 &&  S_ISDIR(sbuf2.st_mode) ) {
		if ( (cp = strrchr(inpfile,PATH_SEPARATOR))  != NULL  ) {
			cp++;
		}else{
			cp = inpfile;
		}
		sprintf(filename, "%s%c%s", outfile, PATH_SEPARATOR, cp);
	}else{
		strcpy(filename,outfile) ;
	}

	dc_setExtraOption(extraOption);
	if((!overwrite) && (dc_access(filename, F_OK) == 0)) {
		fprintf(stderr, "%s: Skipping existing file %s.\n", argv[0], filename);
		return 0;
	}

	errno = 0 ;

	if(!isStdin) {
		dc_setExtraOption(extraOption);
		src = dc_open(inpfile,O_RDONLY | O_BINARY );
		if (src < 0) {
			dc_perror("Can't open source file");
			return -1;
		}
	}

	if(!ahead || (ra_buffer_size <= buffer_size)) {
		dc_noBuffering(src);
	}else{
		dc_setBufferSize(src,ra_buffer_size);
	}

	errno = 0 ;

#ifdef WIN32
	mode = _S_IWRITE ;
#endif /* WIN32 */

	dc_setExtraOption(extraOption);
	dc_setExtraOption(allocSpaceOption);
	dest = dc_open( filename, O_WRONLY|O_CREAT|O_TRUNC|O_BINARY, mode|S_IWUSR);
	if (dest < 0) {
		dc_perror("Can't open destination file");
		return -1;
	}

	if(unsafeWrite) {
		dc_unsafeWrite(dest);
	}

	if( ! doCheckSum ) {
		dc_noCheckSum(dest);
	}

	time(&starttime);
	rc = copyfile(src, dest, buffer_size, &size);
	time(&endtime);

	if (dc_close(src) < 0) {
		perror("Failed to close source file");
		rc = -1;
	}

	if (dc_close(dest) < 0) {
		perror("Failed to close destination file");
		dc_stat64( outfile, &sbuf2);
		mode = sbuf2.st_mode & S_IFMT;
		if (mode == S_IFREG) dc_unlink(outfile);
		rc = -1;
	}

	if (rc != -1 )  {
		copy_time = endtime-starttime ;
		fprintf(stderr,"%llu bytes in %lu seconds",(off64_t)size, copy_time);
		if ( copy_time > 0) {
			fprintf(stderr," (%.2f KB/sec)\n",(double)size/(double)(1024*copy_time) );
		}else{
			fprintf(stderr,"\n");
		}
	}else{
		fprintf(stderr,"dccp failed.\n");

		/* remove destination if copy failed */
		dc_stat64( outfile, &sbuf2);
		mode = sbuf2.st_mode & S_IFMT;
		if (mode == S_IFREG) dc_unlink(outfile);
	}

	return rc;
}

int copyfile(int src, int dest, size_t bufsize, off64_t *size)
{
	ssize_t n, m ;
	char * cpbuf;
	size_t count;
	off64_t total_bytes = 0;
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

			total_bytes += (off64_t)off;
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

void usage()
{

	fprintf(stderr,"DiskCache Copy Program. LibDCAP version: %d.%d.%d-%s\n",
        dc_getProtocol(),
        dc_getMajor(),
        dc_getMinor(),
        dc_getPatch());

	fprintf(stderr,"Usage:  dccp [-d <debugLevel>]  [-h <replyhostname>] [-i]\n");
	fprintf(stderr,"    [-P [-t <time in seconds>] [-l <stage location>] ]\n");
	fprintf(stderr,"    [-a] [-b <read_ahead bufferSize>] [-B <bufferSize>]\n");
	fprintf(stderr,"    [-X <extraOption>] [-u] [-p <first port>[:last port]]\n");
	fprintf(stderr,"    [-r <buffers size>] [-s <buffer size>] [-c] [-C <seconds>] <src> <dest>\n");

	/* some help */

	fprintf(stderr, "\n\n");
	fprintf(stderr, "\t-A                            : active client mode ( client connects to the pool).\n");
	fprintf(stderr, "\t-d <debugLevel>               : set debug level.\n");
	fprintf(stderr, "\t-h <replyhostname>            : specify hostname for data connection.\n");
	fprintf(stderr, "\t-i                            : do not overwrite existing files.\n");
	fprintf(stderr, "\t-P                            : pre-stage request.\n");
	fprintf(stderr, "\t-t <seconds>                  : specify time offset for pre-stage.\n");
	fprintf(stderr, "\t-o <seconds>                  : specify timeout for the 'open' operation\n");
	fprintf(stderr, "\t-l <stage location>           : specify host or network,\n");
	fprintf(stderr, "\t                                where from staged file will be accessed.\n");
	fprintf(stderr, "\t-a                            : enable read-ahead.\n");
	fprintf(stderr, "\t-b                            : specify read-ahead buffer size.\n");
	fprintf(stderr, "\t-B                            : specify transfer buffer size (default=%ld).\n",DEFAULT_BUFFER);
	fprintf(stderr, "\t-X <extraOption>              : add extra wishes into \"open\" request.\n");
	fprintf(stderr, "\t-u                            : enable unsafe write operations.\n");
	fprintf(stderr, "\t-p <first port>[:<last port>] : specify port range number for data connection.\n");
	fprintf(stderr, "\t-T <plugin name>              : specify control line IO tunneling plugin.\n");
	fprintf(stderr, "\t-w <tunnel type>              : specify tunnel type.\n");
	fprintf(stderr, "\t-r <buffer size>              : specify TCP receive buffer size.\n");
	fprintf(stderr, "\t-s <buffer size>              : specify TCP send buffer size.\n");
	fprintf(stderr, "\t-c                            : disable checksum calculation.\n");
	fprintf(stderr, "\t-C <seconds>                  : specify timeout for the 'close' operation.\n");
	exit(1);
}
