/*
 *   DCAP - dCache Access Protocol client interface
 *
 *   Copyright (C) 2000,2004 DESY Hamburg DMG-Division.
 *
 *   AUTHOR: Tigran Mkrtchayn (tigran.mkrtchyan@desy.de),
 *           Paul Millar <paul.millar@desy.de>
 *
 *   This program can be distributed under the terms of the GNU LGPL.
 *   See the file COPYING.LIB
 *
 */

/*
 * This program exercises dcap library.  It needs a dCache instance to
 * test against.  The program uses a specially formatted file to test
 * different read access patterns and takes this file's URI as a
 * command-line argument.  If the file doesn't exist, the program will
 * attempt to create it.
 *
 * The test file is formatted so it contains a sequence of four-byte
 * integer values.  Each value is the the offset (zero-index, within
 * the file) of the first byte of the sequence.  The four-byte
 * integers are stored in network-byte-order.
 *
 * Different test scenarios are available by setting up the
 * environment variables; for example:
 *
 *   DC_LOCAL_CACHE_BUFFER=1 DC_LOCAL_CACHE_BLOCK_SIZE=1048576 ./dcap_test \
 *       dcap://example.org/world-writeable/dcap-test-file
 *
 * Generally useful environment variables:
 *
 *   DCACHE_DEBUG: set the debug level
 *   DCACHE_REPLY: set hostname of client
 */

/* TODO:
 *
 *   1. adapt existing running (or add new running) to allow access
 *      patterns to use different fds.
 *
 *   2. add runner to exercise access patterns in different threads.
 *
 *   3. add "scenarios", such as read-ahead enabled, LCB enabled, etc
 *      and iterate over these.
 *
 *   4. add support for parameterised access-patterns; rewrite
 *      access-patterns to reduce redundancy.
 *
 *   5. always create the file at start of tests; delete file at end
 *      of exercise; exercise other aspects of dcap library, such as
 *      namespace operations.
 */

/*
 * $Id: dcap_test.c,v 1.37 2004-11-03 14:09:23 tigran Exp $
 */

#include <sys/stat.h>
#include <arpa/inet.h>
#include <stdarg.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <inttypes.h>
#include <time.h>
#include <grp.h>
#include <pwd.h>
#include <errno.h>

#include "dcap.h"


#define CONTINUE_UNTIL_FIRST_FINISHED -1
#define CONTINUE_UNTIL_LAST_FINISHED  -2

int dup_fd[2]; /* to test a dup as well */
off64_t file_size;
static off_t *random_offsets;

typedef enum {
  op_init,
  op_access,
  op_fini
} pattern_op_t;

typedef enum {
  pr_error = -1,
  pr_finished,
  pr_continue
} pattern_result_t;

typedef pattern_result_t (*access_pattern_t)(int fd, pattern_op_t op,
					     void **state_p, float *percent_p);

static int make_ref_datafile(const char *path);
static void mode2string(mode_t m, char *s);
static off_t *fisher_yates_shuffle( size_t count, size_t item_size);
static int is_valid_offset( long offset, long value);
static ssize_t read_and_assert(int fd, void *buf, size_t count, off64_t offset);
static void check_offset_valid( long this_offset, long this_value);
static void stat_and_print_fd(int fd);
static void stat_and_print(const char *filename);
static void print_stat_info( struct stat *s);
static off_t i_th_item( int i, size_t item_size);
static void read_from_random_offsets(int fd, void *storage, int index, int item_count);
static int is_placeholder_max_itr( int max_itr);
static int is_offset_in_file(long index);

/* Access patterns */
static pattern_result_t pattern_walk_forward(int fd, pattern_op_t op,
					     void **state_p, float *percent_p);
static pattern_result_t pattern_walk_forward_with_rnd_start(int fd,
							    pattern_op_t op,
							    void **state_p,
							    float *percent_p);
static pattern_result_t pattern_walk_backward(int fd, pattern_op_t op,
					      void **state_p, float *percent_p);
static pattern_result_t pattern_read_float_random_offsets(int fd,
							  pattern_op_t op,
							  void **state_p,
							  float *percent_p);
static pattern_result_t pattern_read_block_random_offsets(int fd,
							  pattern_op_t op,
							  void **state_p,
							  float *percent_p);

/* Runners for executing access patterns */
static int runner_round_robin(const char *label, int fd, int max_itr, int pattern_count, ...);


int make_ref_datafile(const char *path)
{
    int fd;
    long counter;
    long value = 0;

    fd = dc_creat(path, 0644);

    if( fd < 0 ) {
	dc_perror("dc_creat:");
        return 0;
    }

    dc_unsafeWrite(fd);
    dc_setBufferSize(fd, 1024*1024);

    for( counter = 0; counter < 100*1024*1024; counter++) {
	value = htonl(counter * sizeof(long));
	dc_write(fd, &value, sizeof(long));
    }

    dc_close(fd);
    return 1;
}



#define TESTBLOCK 27720

ssize_t read_and_assert(int fd, void *buf, size_t count, off64_t offset)
{
    long *storage = buf;
    ssize_t n;
    int i;

    if( !is_offset_in_file(offset)) {
      return -1;
    }

    if( !is_offset_in_file(offset+count-1)) {
      count = file_size - offset;
    }

    n = dc_pread(fd, buf, count, offset);

    if(n == -1) {
	printf("PANIC: dc_pread returned an error\n");
	printf("    errno %s (%d); expected count %lu; offset %lu\n",
	       strerror(errno), errno, (unsigned long)count, (unsigned long) offset);
	exit(3);

    }

    if(n != count) {
	printf("PANIC: dc_pread was short\n");
	printf("    expected count %lu, actual count %ld; offset %lu\n",
	       (unsigned long)count, (long)n, (unsigned long)offset);
	exit(3);
    }

    for( i = 0; i < count / sizeof(long); i++) {
      check_offset_valid( offset, storage[i]);
      offset += sizeof(long);
    }

    return n;
}


void check_offset_valid(long this_offset, long this_value)
{
    if( !is_valid_offset( this_offset, this_value)) {
	printf("PANIC: read gave incorrect value\n");
	printf("    offset=%ld: value=%ld\n", this_offset, this_value);
	fflush(stdout);
	exit(2);
    }
}


int is_valid_offset( long offset, long value)
{
    return ntohl(value) == offset;
}


/* Return an array of count list-items.  Each list-item is an offset
 * within in file of length (count*item_size).  Each offset points to
 * the start of item_size byte sequence or (equivalently) each offset
 * is exactly divisible by item_size.
 *
 * The algorithm is adapted from the Fisher Yates shuffle.  For more
 * details, see
 *
 *  http://en.wikipedia.org/wiki/Fisher%E2%80%93Yates_shuffle
 */
off_t *fisher_yates_shuffle( size_t item_size, size_t count)
{
    off_t *read_order;
    size_t i, k;

    printf( "Building list of %ld random read offsets (this may take a few seconds)...\n", (long)count);

    read_order = calloc( count, sizeof(off_t));

    read_order [0] = i_th_item(0,item_size);

    for( i = 1; i < count; i++) {
	k = drand48() * (i+1); /* 0 <= k <= i */
	read_order[i] = read_order[k];
	read_order[k] = i_th_item(i,item_size);

	if( i % 10000000 == 0) {
	    printf( "    %u of %u (%d%%)\n", i, count, (int)((double)i/count *100));
	}
    }

    return read_order;
}

off_t i_th_item( int i, size_t item_size)
{
    return i*item_size;
}



int
main(int argc, char *argv[])
{
    char *fname;

    if (argc != 2) exit(1);

    fname = argv[1];

    if( dc_access(fname, F_OK) < 0) {
	if( !make_ref_datafile(fname) ) {
	    exit(2);
	}
    }

    stat_and_print( fname);

    srand(time(NULL));

    while(1) {

	dup_fd[0] = dc_open(fname, O_RDONLY);

	if(dup_fd[0] < 0 ) {
	    dc_error("failed to open file:");
	    exit(1);
	}

	stat_and_print_fd( dup_fd[0]);

	dup_fd[1] = dc_dup(dup_fd[0]);
	if(dup_fd[1] < 0 ) {
	    dc_error("dc_dup failed");
	    exit(1);
	}

	file_size = dc_lseek(dup_fd[0], (off_t)0, SEEK_END);

	dc_lseek(dup_fd[1], 0, SEEK_SET);

	runner_round_robin( "Walking forward (fd from open)", dup_fd[0],
			    CONTINUE_UNTIL_FIRST_FINISHED, 1,
			    pattern_walk_forward);

	runner_round_robin( "Walking forward (fd from dup)", dup_fd[1],
			    CONTINUE_UNTIL_FIRST_FINISHED, 1,
			    pattern_walk_forward);

	runner_round_robin( "Walking backward", dup_fd[0],
			    CONTINUE_UNTIL_FIRST_FINISHED, 1,
			    pattern_walk_backward);

	runner_round_robin( "Cross", dup_fd[0],
			    CONTINUE_UNTIL_LAST_FINISHED, 2,
			    pattern_walk_forward, pattern_walk_backward);

	random_offsets = fisher_yates_shuffle(sizeof(long),
					      file_size/sizeof(long));

	runner_round_robin( "Random reads of floats", dup_fd[0], 4000, 1,
			    pattern_read_float_random_offsets);

	runner_round_robin( "Random reads of blocks", dup_fd[0], 4000, 1,
			    pattern_read_block_random_offsets);

	runner_round_robin( "Combined random and walk forward", dup_fd[0],
			    CONTINUE_UNTIL_FIRST_FINISHED, 2,
			    pattern_read_block_random_offsets,
			    pattern_walk_forward);

	runner_round_robin( "Combined random and walk backward", dup_fd[0],
			    CONTINUE_UNTIL_FIRST_FINISHED, 2,
			    pattern_read_block_random_offsets,
			    pattern_walk_backward);

	runner_round_robin( "Combined two walks", dup_fd[0],
			    CONTINUE_UNTIL_FIRST_FINISHED, 2,
			    pattern_walk_forward_with_rnd_start,
			    pattern_walk_forward_with_rnd_start);

	dc_close(dup_fd[0]);
	dc_close(dup_fd[1]);
	free( random_offsets);
    }
}

void stat_and_print_fd(int fd)
{
    struct stat s;

    if( dc_fstat(fd, &s) < 0 ) {
	dc_perror("dc_fstat failed: ");
	exit(1);
    }

    print_stat_info(&s);
}

void stat_and_print(const char *filename)
{
    struct stat s;

    if( dc_lstat(filename, &s) < 0 ) {
	dc_perror("dc_lstat failed: ");
	exit(1);
    }

    print_stat_info(&s);
}


void print_stat_info( struct stat *s)
{
    struct passwd *pw;
    struct group *gr;
    char mode[11];

    pw = getpwuid(s->st_uid);
    gr = getgrgid(s->st_gid);
    mode2string(s->st_mode, mode);
    printf("dc_stat result:\n");
    printf("st_ino:     %ld\n", s->st_ino);
    printf("st_dev:     %lu\n", (unsigned long) s->st_dev);
    printf("st_mode:    %s (0%o)\n", mode, s->st_mode);
    printf("st_nlink:   %d\n", s->st_nlink);
    printf("st_uid:     %s (%d)\n", pw == NULL ? "unknown" : pw->pw_name,  s->st_uid);
    printf("st_gid:     %s (%d)\n", gr == NULL? "unknown": gr->gr_name, s->st_gid);
    printf("st_size:    %lu\n", (unsigned long) s->st_size);
    printf("st_blksize: %ld\n", s->st_blksize);
    printf("st_blocks:  %ld\n", s->st_blocks);
    printf("st_atime:   %s", ctime(&s->st_atime));
    printf("st_mtime:   %s", ctime(&s->st_mtime));
    printf("st_ctime:   %s", ctime(&s->st_ctime));
    printf("\n");
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


pattern_result_t pattern_walk_forward(int fd, pattern_op_t op, void **state_p, float *percent_p)
{
  long storage[TESTBLOCK];
  off64_t *state = *state_p;
  pattern_result_t result = pr_continue;

  switch( op) {

  case op_init:
    *state_p = calloc(1, sizeof(off64_t));
    break;

  case op_access:
    *percent_p = 100.0*(unsigned long)*state/(unsigned long)file_size;

    if( !is_offset_in_file(*state)) {
      result = pr_finished;
      break;
    }

    read_and_assert(fd, storage, sizeof(storage), *state);
    *state += sizeof(storage) + (rand()%512-1) * sizeof(long);
    break;

  case op_fini:
    free( state);
    break;
  }

  return result;
}


pattern_result_t pattern_walk_forward_with_rnd_start(int fd, pattern_op_t op, void **state_p, float *percent_p)
{
  long storage[TESTBLOCK];
  off64_t *state = *state_p;
  pattern_result_t result = pr_continue;

  switch( op) {

  case op_init:
    state = *state_p = calloc(1, sizeof(off64_t));
    *state = rand() % file_size;
    break;

  case op_access:
    *percent_p = 100.0*(unsigned long)*state/(unsigned long)file_size;

    if( !is_offset_in_file(*state)) {
      result = pr_finished;
      break;
    }

    read_and_assert(fd, storage, sizeof(storage), *state);
    *state += sizeof(storage) + (rand()%512-1) * sizeof(long);
    break;

  case op_fini:
    free( state);
    break;
  }

  return result;
}


pattern_result_t pattern_walk_backward(int fd, pattern_op_t op, void **state_p, float *percent_p)
{
  long storage[TESTBLOCK];
  off64_t *state = *state_p;
  pattern_result_t result = pr_continue;

  switch( op) {

  case op_init:
    state = *state_p = calloc( 1, sizeof(off64_t));
    *state = file_size - sizeof(storage);
    break;

  case op_access:
    *percent_p = 100.0*((unsigned long)file_size - (unsigned long)*state)/(unsigned long)file_size;

    if(*state < 0) {
      result = pr_finished;
      break;
    }

    read_and_assert(fd, storage, sizeof(storage), *state);
    *state -= sizeof(storage) + (rand()%512-1) * sizeof(long);
    break;

  case op_fini:
    free( state);
    break;
  }

  return result;
}


pattern_result_t pattern_read_float_random_offsets(int fd, pattern_op_t op, void **state_p, float *percent_p)
{
  long storage;
  long index, *state = *state_p;
  pattern_result_t result = pr_continue;

  switch( op) {
  case op_init:
    state = *state_p = malloc(sizeof(long));
    *state = 0;
    break;

  case op_access:
    index = *state;
    read_from_random_offsets(fd, &storage, index, 1);
    index++;
    *state = index;

    *percent_p = 100.0 * index * sizeof(float) / file_size;

    if( index >= file_size / sizeof(float)) {
      result = pr_finished;
    }
    break;

  case op_fini:
    free( state);
    break;
  }

  return result;
}


pattern_result_t pattern_read_block_random_offsets(int fd, pattern_op_t op, void **state_p, float *percent_p)
{
  long storage [TESTBLOCK];
  size_t index, *state = *state_p;
  pattern_result_t result = pr_continue;

  switch( op) {
  case op_init:
    state = *state_p = malloc(sizeof(long));
    *state = 0;
    break;

  case op_access:
    index = *state;
    read_from_random_offsets(fd, &storage, index, TESTBLOCK);
    index++;
    *state = index;

    *percent_p = 100.0 * index * sizeof(float) / file_size;

    if( index >= file_size / sizeof(float)) {
      result = pr_finished;
    }
    break;

  case op_fini:
    free( state);
    break;
  }

  return result;
}


int is_offset_in_file(long offset)
{
  return offset < file_size ?  1 : 0;
}


void read_from_random_offsets(int fd, void *storage, int index, int item_count)
{
  float *values = storage;
  off64_t read_offset = random_offsets [index];
  size_t read_size = item_count * sizeof(long);

  read_and_assert(fd, values, read_size, read_offset);
}


struct pattern_info {
  void *state;
  int init, is_finished;
  access_pattern_t pattern;
};


int runner_round_robin(const char *label, int fd, int max_itr, int pattern_count, ...)
{
  struct pattern_info *info, *this_info;
  float percent_complete=0, pattern_percent, this_round_percent;
  void *state;
  va_list pl;
  int i, exit_code=0, itr_count;
  int active_patterns;

  printf( "%s\n", label);

  info = calloc( pattern_count, sizeof(struct pattern_info));

  va_start(pl, pattern_count);
  for( i = 0; i < pattern_count; i++) {
    info [i].pattern = va_arg(pl, access_pattern_t);
    if( info[i].pattern(fd, op_init, &info[i].state, NULL) == pr_error) {
      exit_code = -1;
      goto clean_exit;
    }
    info[i].init=1;
  }
  va_end(pl);

  active_patterns = pattern_count;

  for( itr_count = 0;
       is_placeholder_max_itr(max_itr) || itr_count < max_itr;
       itr_count++) {

    this_round_percent = 0;

    for( i = 0; i < pattern_count; i++) {
      this_info = &info [i];

      if( this_info->is_finished)
	continue;

      state = this_info->state;
      switch( this_info->pattern(fd, op_access, &state, &pattern_percent)) {
      case pr_error:
	exit_code = -1;
	goto clean_exit;

      case pr_continue:
	if( max_itr == CONTINUE_UNTIL_FIRST_FINISHED) {
	  if( pattern_percent > percent_complete)
	    percent_complete = pattern_percent;
	} else if( max_itr == CONTINUE_UNTIL_LAST_FINISHED) {
	  if( this_round_percent == 0 || pattern_percent < this_round_percent)
	    this_round_percent = pattern_percent;
	}
	break;

      case pr_finished:
	if( max_itr == CONTINUE_UNTIL_FIRST_FINISHED) {
	  goto clean_exit;
	} else {
	  active_patterns--;
	  this_info->is_finished = 1;
	}
      }
    }

    if( max_itr == CONTINUE_UNTIL_LAST_FINISHED) {
      percent_complete = this_round_percent;
    } else if( max_itr != CONTINUE_UNTIL_FIRST_FINISHED) {
      percent_complete = 100.0 * itr_count / max_itr;
    }

    if( itr_count % 1000 == 0) {
      printf( "    %4d  %2d%% complete\n", itr_count,
	      (int)percent_complete);
    }

    if( active_patterns == 0)
      break;
  }

 clean_exit:
  for( i = 0; i < pattern_count; i++) {
    if( info [i].init) {
      info [i].pattern(fd, op_fini, &info [i].state, NULL);
    }
  }

  return exit_code;
}

int is_placeholder_max_itr( int max_itr)
{
  return max_itr == CONTINUE_UNTIL_FIRST_FINISHED || max_itr == CONTINUE_UNTIL_LAST_FINISHED;
}
