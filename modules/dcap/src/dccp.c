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

#include <sys/ioctl.h>
#include <termios.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <limits.h>
#include <string.h>
#ifndef WIN32
#    include <sys/param.h>
#    include "dcap_signal.h"
#else
#    include "dcap_unix2win.h"
extern int getopt(int, char * const *, const char *);
#endif

#include "dcap.h"
#include "sigfig.h"
#include "print_size.h"
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

/* Number of bytes transferred for one sweep of the activity bar */
#define ACTIVITY_BAR_ONE_SWEEP_SIZE 107374182

/* How many hash symbols should form the activity bar */
#define ACTIVITY_BAR_SIZE 5

/* Number of characters to leave blank at the end of the line.  This
   could be zero; however, due to latency in obtaining the correct
   window size, if the user reduces the window size then the line will
   overflow, corrupting the output.  We leave a small gap to reduce
   the likelihood of this happening. */
#define END_OF_LINE_EMPTY 4

/* Placeholder value for total-length when file's length is unknown */
#define SIZE_FOR_UNKNOWN_TRANSFER_LENGTH 0

/* The width of window to assume when cannot establish the terminal's
   actual width */
#define DEFAULT_TERMINAL_WIDTH 80

/* Number of items we could potentially put in the line output */
#define ITEM_COUNT 3

typedef enum  {
	ett_set,
	ett_measure
} operation_t;

typedef enum {
	progress_set,
	progress_finished
} progress_op_t;

typedef struct {
  char *content;
  int should_display;
  int hide_order;
  size_t length;
} display_item_t;


static int is_feedback_enabled;


static void usage();
static int copyfile(int src, int dest, size_t buffsize, off64_t  *size, off64_t total_size);
static void hash_printing_accept_byte_count(progress_op_t op,
                                            off64_t total_bytes,
                                            off64_t total_size);
static void build_output(char *buffer, int width,
                         off64_t bytes_so_far, off64_t total_size);
static void append_spaces_if_shrunk( char *buffer, int width);
static void write_bytes_written(char *buffer, off64_t bytes_written,
                                off64_t total_size);
static void write_avr_rate(char *buffer, off64_t bytes_written,
                           off64_t total_size);
static void write_percent(char *buffer, off64_t bytes_written,
                          off64_t total_size);
static void write_percentage_progress_bar(char *buffer,
                                          size_t progress_bar_size,
                                          off64_t bytes_written,
                                          off64_t total_size);
static void write_activity_progress_bar(char *buffer, size_t progress_bar_size,
                                        off64_t bytes_written);
static void write_spaces( char *buffer, int count);
static time_t elapsed_transfer_time( operation_t op);
static int get_terminal_width();
static int transfer_has_known_size( off64_t total_size);
static int build_item( display_item_t *item, off64_t bytes_written,
		       off64_t total_size, int priority, int bar_size,
		       void (*fn)(char *buffer, off64_t bytes_written, off64_t total_size));
static int hide_items_for_minimum_bar_size( display_item_t *item, int bar_size, int minimum_size);
static void write_items( char *buffer, display_item_t *items);
static int hide_items_for_minimum_bar_size( display_item_t *item, int bar_size,
					    int minimum_size);

int main(int argc, char *argv[])
{

	int  src, dest;
	struct stat64 sbuf, sbuf2;
	time_t copy_time;
	off64_t size=0, total_size;
	size_t buffer_size = DEFAULT_BUFFER; /* transfer buffer size */
	int rc ;
	char filename[MAXPATHLEN],*inpfile, *outfile;
	char formatted_rate[12], formatted_size[12];
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

	if( getenv("DCACHE_SHOW_PROGRESS") != NULL) {
		is_feedback_enabled = 1;
	}

	/* FIXME removing the DC_LOCAL_CACHE_BUFFER environment
	 * variable vetos dcap's use of the lcb (the local cache).
	 * This is an ugly work-around needed because the current lcb
	 * code gives terrible performance when the client streams
	 * data in large chunks.  Rather than rewrite LCB, we
	 * introduce this as a "temporary" work-around.
	 *
	 * Although clients should tune their software for their
	 * access patterns, this is "impossible" (or at least
	 * unlikely); therefore LCB should be rewritten to provide
	 * better performance in this case.
	 */
	unsetenv("DC_LOCAL_CACHE_BUFFER");

	while( (c = getopt(argc, argv, "Ad:o:h:iX:Pt:l:aB:b:up:T:r:s:w:cC:H")) != EOF) {

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
		        case 'H':
				is_feedback_enabled=1;
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
		total_size = sbuf.st_size;
	} else {
		total_size = SIZE_FOR_UNKNOWN_TRANSFER_LENGTH;
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

	elapsed_transfer_time( ett_set);
	rc = copyfile(src, dest, buffer_size, &size, total_size);

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
		copy_time = elapsed_transfer_time(ett_measure);
		dc_bytes_as_size(formatted_size, size);
		fprintf(stderr,"%llu bytes (%s) in %lu seconds",(off64_t)size, formatted_size, copy_time);
		if ( copy_time > 0) {
			dc_bytes_as_size(formatted_rate, (double)size / copy_time);
			fprintf(stderr," (%s/s)\n", formatted_rate);
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

int copyfile(int src, int dest, size_t bufsize, off64_t *size, off64_t total_size)
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

	if( is_feedback_enabled) {
		hash_printing_accept_byte_count(progress_set, 0, total_size);
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

			while ((count != off) && ((m = dc_write(dest, cpbuf+count, off-count)) > 0)) {
				total_bytes += (off64_t)m;
				count += m;
				if( is_feedback_enabled) {
					hash_printing_accept_byte_count(progress_set, total_bytes, total_size);
				}
			}

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

	if( is_feedback_enabled) {
		hash_printing_accept_byte_count( progress_finished, 0, 0);
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

	fprintf(stderr,"Usage:  dccp [-H] [-d <debugLevel>]  [-h <replyhostname>] [-i]\n");
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
	fprintf(stderr, "\t-H                            : show progress during file transfer.\n");
	exit(1);
}


void hash_printing_accept_byte_count(progress_op_t op, off64_t bytes_written,
                                     off64_t total_size)
{
	static char *output;
	static size_t max_output_size;
	int width;

	switch( op) {
	case progress_set:
		width = get_terminal_width();

		if( width >= max_output_size) {
			max_output_size = width+1;
			output = realloc( output, max_output_size);
		}

		build_output(output, width, bytes_written, total_size);
		append_spaces_if_shrunk(output, width);
		printf( "%s\r", output);
		fflush(stdout);
		break;

	case progress_finished:
		free(output);
		output = NULL;
		max_output_size = 0;
		printf( "\n");
		break;
	}
}

/* Add spaces to block out stale output */
void append_spaces_if_shrunk( char *buffer, int width)
{
	static size_t previous;
	size_t current;
	int count, max_spaces;

	current = strlen( buffer);
	max_spaces = width - current;

	if( previous > current) {
		count = (previous-current) < max_spaces ? (previous-current) : \
		        max_spaces;
		write_spaces( &buffer [current], count);
	}

	previous = current;
}

void write_spaces( char *buffer, int count)
{
	char *p = buffer;
	int i;

	for( i = 0; i < count; i++) {
		p [i] = ' ';
	}

	p [i] = '\0';
}

void build_output( char *buffer, int width, off64_t bytes_written,
                   off64_t total_size)
{
	int bar_size = width - END_OF_LINE_EMPTY;
	display_item_t item [ITEM_COUNT];

	if( transfer_has_known_size( total_size)) {
		bar_size = build_item( &item[0], bytes_written, total_size, 0,
		                       bar_size, write_percent);
	} else {
		item[0].should_display=0;
	}

	bar_size = build_item( &item[1], bytes_written, total_size, 1,
			       bar_size, write_bytes_written);
	bar_size = build_item( &item[2], bytes_written, total_size, 2,
			       bar_size, write_avr_rate);

	bar_size = hide_items_for_minimum_bar_size( item, bar_size, 20);

	if( transfer_has_known_size( total_size)) {
		write_percentage_progress_bar(buffer, bar_size, bytes_written,
		                              total_size);
	} else {
		write_activity_progress_bar( buffer, bar_size, bytes_written);
	}

	write_items( buffer, item);
}


int build_item( display_item_t *item, off64_t bytes_written,
		 off64_t total_size, int hide_order, int bar_size,
		 void (*fn)(char *buffer,  off64_t bytes_written, off64_t total_size))
{
	char tmp[15]; /* must ensure this is big enough for all items */

	fn( tmp, bytes_written, total_size);
	item->content = strdup(tmp);
	item->should_display = 1;
	item->hide_order = hide_order;
	item->length = strlen( tmp);
	bar_size -= item->length+1;
	return bar_size;
}

int hide_items_for_minimum_bar_size( display_item_t *items, int bar_size,
				     int minimum_size)
{
	int i, cur_hide_order=0;
	display_item_t *item;

	while( bar_size < minimum_size) {
		for( i = 0; i < ITEM_COUNT; i++) {
			item = &items[i];

			if( item->should_display &&
			    item->hide_order == cur_hide_order) {
				item->should_display = 0;
				free(item->content);
				bar_size += item->length + 1;
				break;
			}
		}

		if( i == ITEM_COUNT) {
			if( cur_hide_order <  ITEM_COUNT-1)
				cur_hide_order++;
			else
				break;  /* give up, we're run out of things to hide */
		}
	}

	return bar_size;
}


int transfer_has_known_size( off64_t total_size)
{
	return total_size != SIZE_FOR_UNKNOWN_TRANSFER_LENGTH;
}

void write_items( char *buffer, display_item_t *items)
{
	int i;
	display_item_t *item;

	for( i = 0; i < ITEM_COUNT; i++) {
		item = &items [i];
		if( item->should_display) {
			strcat( buffer, " ");
			strcat( buffer, item->content);
			free(item->content);
		}
	}
}


/**
 *  Write a progress bar into the buffer that shows the percent of the
 *  file transferred.  This overwrites any pre-existing content in
 *  buffer, so the memory need not be initialised.  Requires
 *  bar_size+1 bytes.
 */
void write_percentage_progress_bar( char *buffer, size_t bar_size,
                                    off64_t bytes_written, off64_t total_size)
{
	int nhashes, i;
	int bar_active_size = bar_size-2;  /* size, excluding '[' and ']' */

	nhashes = (bar_active_size * bytes_written) / total_size;

	buffer[0] = '[';
	for( i = 0; i < nhashes; i++) {
		buffer[i+1] = '#';
	}
	for( i = nhashes; i < bar_active_size; i++) {
		buffer[i+1] = '-';
	}
	buffer[i+1] = ']';
	buffer[i+2] = '\0';
}


/**
 *  Write an activity bar into the buffer that indicates the flow of
 *  data.  This overwrites any pre-existing content in buffer, so the
 *  memory need not be initialised.  Requires bar_size+1 bytes.
 */
void write_activity_progress_bar( char *buffer, size_t bar_size,
                                  off64_t bytes_written)
{
	int i, start, end, extra_at_start;
	int bar_active_size = bar_size-2;  /* size, excluding '[' and ']' */
	off64_t remainder;

	remainder = bytes_written % ACTIVITY_BAR_ONE_SWEEP_SIZE;
	start = (bar_active_size * remainder) / ACTIVITY_BAR_ONE_SWEEP_SIZE;
	end = start + ACTIVITY_BAR_SIZE - 1;

	extra_at_start = end >= bar_active_size ?  1 + end - bar_active_size : 0;

	buffer[0] = '[';
	for( i = 0; i < bar_active_size; i++) {
		if( i < extra_at_start ||
		   (i >= start && i <= end)) {
			buffer[i+1] = '#';
		} else {
			buffer[i+1] = '-';
		}
	}
	buffer[i+1] = ']';
	buffer[i+2] = '\0';
}


/* Memory pointed to by buffer must be (at least) 10 bytes in size (9
 * bytes for the output and 1 byte for '\0') */
void write_bytes_written( char *buffer, off64_t bytes_written,
                          off64_t total_size)
{
	char si_size[10];

	dc_bytes_as_size( si_size, bytes_written);
	strcpy( buffer, si_size);
}

/* Memory pointed to by buffer must be (at least) 14 bytes in size (13
 * bytes for the output and 1 byte for '\0'). */
void write_avr_rate( char *buffer, off64_t bytes_written,
                     off64_t total_size)
{
	double rate;
	char formatted_bytes[10];
	time_t copy_time;

	copy_time = elapsed_transfer_time( ett_measure);

	if(copy_time > 0) {
		/* Bytes transferred in first second, at average rate */
		rate = ((double)bytes_written)  / copy_time;
		dc_bytes_as_size( formatted_bytes, rate);
		sprintf( buffer, "(%s/s)", formatted_bytes);
	} else {
		buffer[0] = '\0';
	}
}

/* Memory pointed to by buffer needs to be at least 6 bytes in size (5
 * bytes for the output and 1 byte for the '\0') */
void write_percent(char *buffer, off64_t bytes_written, off64_t total_size)
{
	char percent[5];

	dc_print_with_sigfig(percent, sizeof(percent), 2,
	                     100.0*bytes_written/total_size);

	sprintf(buffer, "%s%%", percent);
}


int get_terminal_width()
{
	static int fd;
	struct winsize size;

	if( fd == 0) {
		fd = open("/dev/tty", O_RDONLY);
	}

	if( fd < 0) {
		return DEFAULT_TERMINAL_WIDTH;
	}

	if( ioctl(fd, TIOCGWINSZ, &size) < 0) {
		close(fd);
		fd = -1;
		return DEFAULT_TERMINAL_WIDTH;
	}

	return size.ws_col;
}


/**
 * A simple timer mechansim: calling ett_set (re-)sets the timer
 * (returns zero); calling with ett_measure returns elapsed time, in
 * seconds, since the last call to ett_set.
 */
time_t elapsed_transfer_time( operation_t op)
{
	static time_t start_time;
	time_t now, retval=0;

	switch( op) {
	case ett_set:
		time(&start_time);
		break;

	case ett_measure:
		time(&now);
		retval = now - start_time;
		break;
	}

	return retval;
}
