/*
 *   Provide a local cache for blocks read.
 *
 *   AUTHOR: Guenter Duckeck <Guenter.Duckeck@physik.uni-muenchen.de>
 *
 *   This program can be distributed under the terms of the GNU LGPL.
 *   See the file COPYING.LIB
 *
 *   Code refactored to aid readability and maintainability by Paul
 *   Millar <paul.millar@desy.de>
 *
 *   This file provides a cache for read requests.  It maintains a
 *   linked-list of cache blocks, all (nominally) the same size.  The
 *   cache is updated with every read, if there is a cache-hit then
 *   the block accounting is updated and data is read, otherwise the
 *   LRU block is used to store freshly loaded data to satisfy the
 *   read request.
 *
 *   Requests that do not fit within a block are processed on a
 *   block-by-block fashion, triggering loading of data as needed.
 *
 *   The linked-list is maintained in block-number order and the
 *   implementation requires that the first block in the linked-list
 *   is always the first block from the file.
 *
 *   A number of comments are included that describe possible future
 *   work, based on the code-review process.  These comments are
 *   labelled "REVISIT".
 */
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdarg.h>
#include <errno.h>

#include "dcap.h"
#include "dcap_read.h"
#include "dcap_lcb.h"
#include "dcap_lseek.h"
#include "debug_level.h"
#include "dcap_debug.h"
#include "node_plays.h"

#define MEMORY_PER_FILE_DEFAULT (1024*1024)
#define MEMORY_PER_FILE_MINIMUM 1024

#define MINIMUM_NUMBER_OF_BLOCKS 2

/**
 *  This structure contains information that is valid for the duration
 *  of a read-request.
 */
struct read_request {
    struct vsp_node *node;
    off64_t fpos;        /* current filepointer */
    off64_t bnum;        /* The index of buffer fpos is within */
    off64_t bpos;        /* Offset within buffer bnum correspoding to fpos */
    char *buf;           /* User supplied buffer to write into */
    ssize_t remaining;   /* Number of bytes still to read */
};

static void ensure_data_in_currentcb(struct read_request *request);
static void establish_buffer_size( local_cache_buffer_t *lb);
static void flush_cache( struct vsp_node *node);
static void read_first_buffer(struct read_request *request);
static ssize_t copy_data_from_currentcb(struct read_request *request);
static void build_request( struct read_request *request, struct vsp_node *node,
			   char *buf, ssize_t len);
static void update_request(struct read_request *request,
			   ssize_t bytes_copied);
static void update_currentcb_to_existing_cb(struct read_request *request,
					    cbindex_t *new_current);
static void update_currentcb_by_reading(struct read_request *request,
					cbindex_t *nearest);
static void update_global_stats_from_retired_cb(cbindex_t *cb);
static int have_unused_buf(local_cache_buffer_t *lb);
static void read_and_add(struct read_request *request, cbindex_t *nearest,
			 cbindex_t *new);
static long get_envvar(const char *envvar_name, long default_value,
		       long minimum_value);
static cbindex_t *find_closest_existing_cb( off64_t bnum, cbindex_t *start);
static cbindex_t *find_closest_existing_cb_scan_down(off64_t bnum,
						     cbindex_t *start);
static cbindex_t *find_closest_existing_cb_scan_up(off64_t bnum,
						   cbindex_t *start);
static cbindex_t *next_unallocated_cb(local_cache_buffer_t *lb, off64_t bnum);
static cbindex_t *find_lru_buf(local_cache_buffer_t *lb);
static cbindex_t *extract_lru_cb(local_cache_buffer_t *lb);
static void remove_from_ll(cbindex_t *item);
static void insert_into_ll_after_existing(cbindex_t *existing_item,
					  cbindex_t *new_item);
static void emit_debug_for_current_buf(struct read_request *request);
static void check_ll_consistency( local_cache_buffer_t *lb);
static int is_first_ever_read_request(local_cache_buffer_t *lb);
static ssize_t read_currentcb( struct read_request *request);
static cbindex_t *build_from_extracted_lru_cb(local_cache_buffer_t *lb,
					      off64_t bnum);
static void emit_debug( const char *format, ...);
static void set_fpos(struct read_request *request, off64_t fpos);
static void record_read_hit( local_cache_buffer_t *lb, cbindex_t *cb);

static unsigned long global_stats_total_block_reads;
static unsigned long global_stats_blocks_allocated;

static int current_fd;


/**
 *  Flush any cached blocks.
 */
void dc_lcb_flush(int fd)
{
	struct vsp_node *node;

	current_fd = fd;

	emit_debug( "flushing LCB");

#ifdef DC_CALL_TRACE
	showTraceBack();
#endif

	/* nothing wrong ... yet */
	dc_errno = DEOK;

	node = get_vsp_node(fd);
	if (node != NULL) {
		if( node->lcb ) {
			flush_cache(node);
		}
		m_unlock(&node->mux);
	}
}




long dc_lcb_init( struct vsp_node *node ) {

    local_cache_buffer_t *lb = NULL;

    current_fd = node->dataFd;

    lb = node->lcb = malloc(sizeof(local_cache_buffer_t));
    if( lb == NULL) {
	goto out_of_memory_exit;
    }

    memset( lb, 0, sizeof( local_cache_buffer_t));

    establish_buffer_size(lb);

    lb->cbi = calloc( lb->nbuf, sizeof(cbindex_t));
    if( lb->cbi == NULL) {
	goto out_of_memory_exit;
    }

    return 0;

  out_of_memory_exit:
    free( lb);
    node->lcb = NULL;
    dc_errno = DEMALLOC;
    errno = ENOMEM;
    return -1;
}


void establish_buffer_size( local_cache_buffer_t *lb)
{
    long buf_size, new_buf_size, total_mem;
    int pagesize;

    pagesize = getpagesize();

    total_mem = get_envvar( "DC_LOCAL_CACHE_MEMORY_PER_FILE",
			    MEMORY_PER_FILE_DEFAULT,
			    MINIMUM_NUMBER_OF_BLOCKS * pagesize);

    buf_size = get_envvar( "DC_LOCAL_CACHE_BLOCK_SIZE", pagesize, pagesize);

    if( total_mem / buf_size < MINIMUM_NUMBER_OF_BLOCKS) {
	new_buf_size = buf_size;
	while( total_mem / new_buf_size < MINIMUM_NUMBER_OF_BLOCKS) {
	    new_buf_size >>= 1;
	}
	dc_debug( DC_ERROR, "chosen buffer size (%ld) is too large for memory (%ld),"
		  " decreasing buffer size to %ld", buf_size, total_mem, new_buf_size);
	buf_size = new_buf_size;
    }

    lb->buflen = buf_size;
    lb->nbuf = total_mem / buf_size;

    dc_debug( DC_INFO, "init %d buffers of size %d bytes (%ld total)", lb->nbuf,
	      lb->buflen, lb->nbuf*lb->buflen);
}


long get_envvar( const char *envvar_name, long default_value,
		 long minimum_value)
{
    long envvar_value, value_to_use;
    char *envvar_str, *end;

    value_to_use = default_value;

    envvar_str = getenv(envvar_name);

    if( envvar_str != NULL && envvar_str[0] != '\0') {
	envvar_value = strtol(envvar_str, &end, 10);

	if( *end != '\0') {
	    dc_debug(DC_ERROR, "environment variable %s is not an integer value, "
		     "ignoring it.", envvar_name);
	} else if( envvar_value < minimum_value) {
	    dc_debug(DC_ERROR, "environment variable %s value (%d) is less than the "
		     "minimum value (%d), ignoring it.", envvar_name,
		     envvar_value, minimum_value);
	} else {
	    value_to_use = envvar_value;
	}
    }

    return value_to_use;
}

void dc_lcb_clean(struct vsp_node *node)
{
    local_cache_buffer_t *lb = node->lcb;

    current_fd = node->dataFd;

    flush_cache(node);

    emit_debug( "statistics: reads=%lu,  blocks=%lu,  hit ratio=%f",
		global_stats_total_block_reads,
		global_stats_blocks_allocated,
		(double) global_stats_total_block_reads /
		global_stats_blocks_allocated);

    free(lb->cbi);
    free(node->lcb);
    node->lcb = NULL;
}

void update_global_stats_from_retired_cb(cbindex_t *cb)
{
    global_stats_blocks_allocated++;
    global_stats_total_block_reads += cb->nused;
}


void flush_cache( struct vsp_node *node)
{
    local_cache_buffer_t *lb = node->lcb;
    cbindex_t *cb, *next;

    for( cb = lb->cbi; cb; cb = next) {
	free(cb->bpo);
	cb->bpo = NULL;
	next = cb->next;
	cb->prev = NULL;
	cb->next = NULL;
	cb->bnum = 0;
	update_global_stats_from_retired_cb(cb);
    }

    lb->nbcache = 0;
}


ssize_t dc_lcb_read( struct vsp_node * node, char *buf, ssize_t len)
{
    local_cache_buffer_t *lb = node->lcb;
    ssize_t total_copied=0, bytes_copied=0;
    struct read_request request_storage, *request = &request_storage;

    current_fd = node->dataFd;

    build_request(request, node, buf, len);

    if ( is_first_ever_read_request(lb)) {
	read_first_buffer(request);
    }

    while(request->remaining > 0) {
	ensure_data_in_currentcb(request);
	bytes_copied = copy_data_from_currentcb(request);

	// Consider zero bytes read as reaching EOF
	if( bytes_copied <= 0) {
	    break;
	}

	update_request(request, bytes_copied);
	total_copied += bytes_copied;
    }

    return total_copied > 0 ? total_copied : bytes_copied;
}


int is_first_ever_read_request(local_cache_buffer_t *lb)
{
    return lb->nbcache == 0;
}


void read_first_buffer(struct read_request *request)
{
    local_cache_buffer_t *lb = request->node->lcb;
    lb->currentcb = next_unallocated_cb(lb, 0);
    read_currentcb(request);
}


void build_request( struct read_request *request, struct vsp_node *node,
		    char *buf, ssize_t len)
{
    off64_t fpos;

    request->node = node;
    request->buf = buf;
    request->remaining = len;

    fpos = dc_real_lseek( node, 0, SEEK_CUR);
    set_fpos(request, fpos);
}


void set_fpos(struct read_request *request, off64_t fpos)
{
    local_cache_buffer_t *lb = request->node->lcb;

    request->fpos = fpos;
    request->bnum = fpos / lb->buflen;
    request->bpos = fpos % lb->buflen;
}


void ensure_data_in_currentcb(struct read_request *request)
{
    local_cache_buffer_t *lb = request->node->lcb;
    cbindex_t *nearest;

    nearest = find_closest_existing_cb( request->bnum, lb->currentcb);

    if( nearest->bnum == request->bnum ) {
	update_currentcb_to_existing_cb(request, nearest);
    } else {
	update_currentcb_by_reading(request, nearest);
    }

    record_read_hit(lb, lb->currentcb);
}


void record_read_hit(local_cache_buffer_t *lb, cbindex_t *cb)
{
    cb->nused++;
    cb->lastused = lb->nbread;
}


cbindex_t *find_closest_existing_cb( off64_t bnum, cbindex_t *start)
{
    cbindex_t *found;

    if( bnum == start->bnum) {
	found = start;
    } else if ( bnum > start->bnum ) {
	found = find_closest_existing_cb_scan_down( bnum, start);
    } else {
	found = find_closest_existing_cb_scan_up( bnum, start);
    }

    return found;
}


cbindex_t *find_closest_existing_cb_scan_down( off64_t bnum, cbindex_t *start)
{
    cbindex_t *found;

    for( found = start; found->next; found = found->next) {
	if( found->bnum >= bnum)
	    break;
    }

    return (found->bnum <= bnum) ? found : found->prev;
}


cbindex_t *find_closest_existing_cb_scan_up( off64_t bnum, cbindex_t *start)
{
    cbindex_t *found;

    for( found = start; found->prev; found = found->prev) {
	if( found->bnum <= bnum)
	    break;
    }

    return found;
}


void update_currentcb_to_existing_cb(struct read_request *request,
				     cbindex_t *new_current)
{
    local_cache_buffer_t *lb = request->node->lcb;

    if( lb->currentcb == new_current) {
	emit_debug( "reading from same buffer:  %d ", request->bnum);
    } else {
	lb->currentcb = new_current;
	emit_debug( "switching to buffer %lld nused %ld",
		    new_current->bnum, new_current->nused );
    }

    /* REVISIT: if re-reading a block created from a partial read,
       we should attempt to re-read the block. */
}

void update_currentcb_by_reading(struct read_request *request,
				 cbindex_t *nearest)
{
    struct vsp_node *node = request->node;
    local_cache_buffer_t *lb = node->lcb;
    off64_t bnum = request->bnum;
    cbindex_t *new;

    if( have_unused_buf(lb)) {
	new = next_unallocated_cb(lb, bnum);
    } else {
	new = build_from_extracted_lru_cb(lb, bnum);

	if( new == nearest) {
	    nearest = nearest->prev;
	}
    }

    read_and_add( request, nearest, new);
}


int have_unused_buf(local_cache_buffer_t *lb)
{
    return lb->nbcache < lb->nbuf;
}


/* REVISIT: One could consider allocating all buffers in one long
   array rather than individually. This reduces the allocation
   overhead and fragmentation. The OS will not actually allocate any
   of the memory until you access the first time in the application,
   so the working set of the process will not be increased. */
cbindex_t *next_unallocated_cb(local_cache_buffer_t *lb, off64_t bnum)
{
    cbindex_t *next;

    if( !have_unused_buf(lb)) {
	/* Catch inconsistencies */
	emit_debug( "caught attempt to allocate too many cbindex objects");
	return NULL;
    }

    next = &lb->cbi [lb->nbcache];
    next->bpo = malloc(lb->buflen);

    if( next->bpo == NULL) {
	emit_debug( "out-of-memory allocating index-%d cbindex object",
		    lb->nbcache);
	return NULL;
    }

    lb->nbcache++;
    next->nused = 0;
    next->bnum = bnum;

    emit_debug( "allocating next buffer %llu %lu ",
		bnum, lb->nbcache);

    return next;
}


cbindex_t *build_from_extracted_lru_cb(local_cache_buffer_t *lb, off64_t bnum)
{
    cbindex_t *target;

    target = extract_lru_cb(lb);

    emit_debug( "replacing buffer %lld with %lld",
		target->bnum, bnum);

    update_global_stats_from_retired_cb(target);

    target->nused = 0;
    target->bnum = bnum;

    return target;
}


cbindex_t *extract_lru_cb(local_cache_buffer_t *lb)
{
    cbindex_t *lru_buf;

    lru_buf = find_lru_buf(lb);
    remove_from_ll( lru_buf);

    return lru_buf;
}


/*  REVISIT; If we are implementing LRU, then there are cheaper ways
    that iterating over all buffers. The current algorithm spends time
    linear in the number of buffers, however an LRU linked list can be
    used to keep a list in LRU order with constant time overhead per
    access; with constant time overhead to get the least recently used
    element (it will be at the end of the list).  */
cbindex_t *find_lru_buf(local_cache_buffer_t *lb)
{
    int i;
    cbindex_t *cb, *lru_cb;
    unsigned long age, maxage;

    /* The algorithm requires that the first buffer (index 0) of the
     * array is never returned */
    for( i = 1; i < lb->nbcache; i++) {
	cb = &lb->cbi [i];
	age = lb->nbread - cb->lastused;
	if( i == 1 || age > maxage) {
	    maxage = age;
	    lru_cb = cb;
	}
    }

    return lru_cb;
}


void read_and_add(struct read_request *request, cbindex_t *nearest,
		  cbindex_t *new)
{
    local_cache_buffer_t *lb = request->node->lcb;

    lb->currentcb = new;
    read_currentcb(request);

    insert_into_ll_after_existing( nearest, new);

    emit_debug_for_current_buf(request);

    check_ll_consistency(lb);
}


void emit_debug_for_current_buf(struct read_request *request)
{
    local_cache_buffer_t *lb = request->node->lcb;
    cbindex_t *current = lb->currentcb;
    off64_t bprev, bnext;

    if ( current->prev != NULL ) {
	bprev = current->prev->bnum;
    } else {
	bprev = 0;
    }

    if ( current->next != NULL ) {
	bnext = current->next->bnum;
    } else {
	bnext = 0;
    }

    emit_debug( "ll %lld,  %x %lld,  %x %lld,  %x %lld",
		request->bnum,
		current->prev, bprev,
		current, current->bnum,
		current->next, bnext);
}


void check_ll_consistency( local_cache_buffer_t *lb)
{
    cbindex_t *current = lb->currentcb;
    cbindex_t *prev = current->prev;
    cbindex_t *next = current->next;

    if ( next && current->bnum >= next->bnum ) {
	dc_debug( DC_ERROR, "LCB trouble with linked list next:  %lld %lld %x %x",
		  current->bnum, next->bnum, current, next);
	current->next = NULL;
    }

    if ( prev && current->bnum <= prev->bnum ) {
	dc_debug( DC_ERROR, "LCB trouble with linked list prev:  %lld %lld %x %x",
		  current->bnum, prev->bnum, current, prev);
    }
}


ssize_t read_currentcb( struct read_request *request)
{
    struct vsp_node * node = request->node;
    local_cache_buffer_t *lb = node->lcb;
    cbindex_t *current = lb->currentcb;
    off64_t start_of_current = current->bnum * lb->buflen;

    emit_debug( "reading %ld bytes for block %lld at %lld ",
		lb->buflen, current->bnum, start_of_current);

    dc_real_lseek(node, start_of_current, SEEK_SET);

    current->blen = dc_real_read(node, current->bpo, lb->buflen);

    dc_real_lseek(node, request->fpos, SEEK_SET);

    if( current->blen < lb->buflen && request->remaining > current->blen) {
	emit_debug( "short-read detected; requested %ld bytes at %lld "
		    "but got %ld", lb->buflen, start_of_current,
		    current->blen);
    }

    lb->nbread++;

    return current->blen;
}


ssize_t copy_data_from_currentcb( struct read_request *request)
{
    struct vsp_node *node = request->node;
    local_cache_buffer_t *lb = node->lcb;
    cbindex_t *current = lb->currentcb;
    ssize_t available, copy_len;
    char *copy_from;

    available = current->blen - request->bpos;

    if( available <= 0) {
        return available == 0 ? 0 : -1;
    }

    copy_len = (request->remaining<available) ? request->remaining : available;

    copy_from = &current->bpo [request->bpos];

    memcpy( request->buf, copy_from, copy_len);

    emit_debug( "copy, at fp=%llu, from bnum=%llu bpos=%llu, %ld bytes",
		request->fpos, request->bnum, request->bpos, copy_len);

    return copy_len;
}


void update_request( struct read_request *request, ssize_t bytes_copied)
{
    request->buf += bytes_copied;
    request->remaining -= bytes_copied;

    set_fpos(request, request->fpos + bytes_copied);

    emit_debug( "updated request, now fp=%llu, bnum=%llu bpos=%llu remaining=%ld",
		request->fpos, request->bnum, request->bpos,
		request->remaining);

    dc_real_lseek(request->node, request->fpos, SEEK_SET);
}


/* Emit debugging output in a consistent fashion */
void emit_debug( const char *format, ...)
{
    va_list ap;
    char appended_format[MAX_MESSAGE_LEN];
    size_t len;

    if( dc_is_debug_level_enabled(DC_IO)) {
        snprintf(appended_format, sizeof(appended_format), "[%d] LCB ", current_fd);
	len = strlen(appended_format);

	strncat( appended_format, format, sizeof(appended_format) - len -1);

	appended_format[MAX_MESSAGE_LEN-1] = '\0';

	va_start(ap, format);
	dc_vdebug(DC_IO, appended_format, ap);
	va_end(ap);
    }
}


void remove_from_ll(cbindex_t *item)
{
    if ( item->prev != NULL ) {
	item->prev->next = item->next;
    }

    if ( item->next != NULL ) {
	item->next->prev = item->prev;
    }
}


void insert_into_ll_after_existing(cbindex_t *existing_item,
				   cbindex_t *new_item)
{
    new_item->prev = existing_item;
    new_item->next = existing_item->next;

    if( existing_item->next) {
	existing_item->next->prev = new_item;
    }

    existing_item->next = new_item;
}

