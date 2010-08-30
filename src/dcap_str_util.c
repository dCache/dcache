#include <string.h>
#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>

#include "dcap_str_util.h"

/**
 *  Copy zero or more bytes from src to dest.  Dest has storage
 *  capacity of dest_size so that the maximum string that may be
 *  stored in dest is dest_size-1 characters long (-1 for term. '\0').
 *
 *  After returning, dest is always '\0' terminated.
 *
 *  dest is returned.
 */
char *dc_safe_strncat( char *dest, size_t dest_size, char *src)
{
  size_t dest_used_space, available_space;

  dest_used_space = strlen( dest);

  available_space = dest_size - dest_used_space -1; /* -1 for term. '\0' */

  if( available_space > 0) {
    strncpy( &dest [dest_used_space], src, available_space);
    dest [dest_size-1] = '\0';
  }

  return dest;
}


/**
 *  Append a formatted string to an existing string.  This is roughly equivalent to:
 *
 *    sprintf( tmp, format, ...);
 *    strcat( dest, tmp);
 *
 *  However, we're careful not to exceed the available storage of dest.
 *
 *  Returns dest on success, NULL if out-of-memory allocating
 *  temporary string.
 */
char *dc_snaprintf( char *dest, size_t dest_size, char *format, ...)
{
  /* Based on example code from Linux vsnprintf(3) man page */

  /* initial guess of 100 bytes */
  int n, size = 100;
  char *p, *np;
  va_list ap;

  if ((p = malloc(size)) == NULL)
    return NULL;

  while (1) {
    /* Try to print in the allocated space. */
    va_start(ap, format);
    n = vsnprintf(p, size, format, ap);
    va_end(ap);

    /* If that worked, carry on */
    if (n > -1 && n < size)
      break;

    /* Else try again with more space. */
    if (n > -1)    /* glibc 2.1 */
      size = n+1; /* precisely what is needed */
    else           /* glibc 2.0 */
      size *= 2;  /* twice the old size */
    if ((np = realloc (p, size)) == NULL) {
      free(p);
      return NULL;
    } else {
      p = np;
    }
  }

  dc_safe_strncat( dest, dest_size, p);

  free(p);

  return dest;
}

