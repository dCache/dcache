/*
 *   DCAP - dCache Access Protocol client interface
 *
 *   Copyright (C) 2000,2004 DESY Hamburg DMG-Division.
 *
 *   AUTHOR: Paul Millar <paul.millar@desy.de>
 *
 *   This program can be distributed under the terms of the GNU LGPL.
 *   See the file COPYING.LIB
 *
 */

/*
 * $Id: dccp.c,v 1.77 2007-02-22 10:19:46 tigran Exp $
 */

#include <stdio.h>

#include "sigfig.h"
#include "print_size.h"

#define NUMBER_OF_PREFIXES (sizeof(si_byte_prefix) / \
			    sizeof(struct si_byte_prefix))

/* What fraction of a prefix's value triggers using the next prefix */
#define DEFAULT_MIN_FRACTION 0.9

struct si_byte_prefix {
  off64_t amount;
  char *symbol;
};

static struct si_byte_prefix si_byte_prefix[] = {
  {1,"B"},
  {1024,"kiB"},
  {1048576L, "MiB"},
  {1073741824L, "GiB"},
  {1099511627776LL, "TiB"},
  {1125899906842624LL, "PiB"},
};



static void format_for_prefix( char *buffer, struct si_byte_prefix *prefix,
			       off64_t size);

/**
 * Takes a file size (in bytes) and write into buffer a description of
 * the size using SI binary prefixes (kiB, MiB, etc).  The prefix with
 * the largest value is used such that the size, measured against that
 * prefix's value (i.e., size / prefix->amount), is greater than
 * min_fraction.
 *
 * Prints (at most) 10 bytes (9 for output and 1 for '\0') into
 * the memory pointed to by buffer.
 */
void dc_bytes_as_size_with_max_fraction( char *buffer, off64_t size,
					 double min_fraction)
{
  struct si_byte_prefix *this_prefix, *next_prefix;
  int i;

  for( i = 0; i < NUMBER_OF_PREFIXES-1; i++) {
    this_prefix = &si_byte_prefix [i];
    next_prefix = &si_byte_prefix [i+1];

    if( size <= next_prefix->amount*min_fraction) {
      format_for_prefix( buffer, this_prefix, size);
      return;
    }
  }

  format_for_prefix( buffer, next_prefix, size);
}


/**
 * Takes a file size (in bytes) and write into buffer a description of
 * the size using SI binary prefixes (kiB, MiB, etc).  A suitable
 * prefix is selected for the given size.
 *
 * Prints (at most) 10 bytes (9 for output and 1 for '\0') into
 * the memory pointed to by buffer.
 */
void dc_bytes_as_size( char *buffer, off64_t size)
{
  dc_bytes_as_size_with_max_fraction( buffer, size, DEFAULT_MIN_FRACTION);
}

/* Print a file size using given SI prefix.  Requires a maximum of 9+1
   bytes */
void format_for_prefix( char *buffer, struct si_byte_prefix *prefix,
			off64_t size)
{
  char tmp[6];

  dc_print_with_sigfig( tmp, sizeof(tmp), 3, (double)size / prefix->amount);
  sprintf( buffer, "%s %s", tmp, prefix->symbol);
}

