#include <string.h>
#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>

#include "dcap_str_util.h"


#define DC_STR_LEN_DEFAULT 1024


struct char_buf {
    int buf_size;
    char *out_buff;
    char *tmp_buff;
};



/**
 * Create a structure to manage extendable strings.
 *
 * Note two char * are used so that the module can be chained with
 * output being used as input.
 *
 * Returns NULL on error.
 */
char_buf_t*
dc_char_buf_create()
{
    struct char_buf *output;
    output = malloc(sizeof(struct char_buf));
    if (output == NULL){
        return NULL;
    }
    output->buf_size = DC_STR_LEN_DEFAULT;
    output->out_buff = malloc(output->buf_size);
    if (output->out_buff == NULL) {
        free(output);
        return NULL;
    }
    output->tmp_buff = malloc(output->buf_size);
    if (output->tmp_buff == NULL) {
        free(output->out_buff);
        free(output);
        return NULL;
    }
    return output;
}

/**
 * sprintf like behaviour to the extendable string.
 *
 * Returns NULL on error.
 */
char *
dc_char_buf_sprintf(char_buf_t *context,const char *format, ...){
    int new_buf_size;
    int characters_copied;
    va_list ap;
    char *tmp_char_ptr;
    if (NULL == context)
    {
        return NULL;
    }
    new_buf_size = context->buf_size;
    while (1) {
        /* Try to print in the allocated space. */
        va_start(ap, format);
        characters_copied = vsnprintf(context->tmp_buff, new_buf_size, format, ap);
        va_end(ap);

        /* If that worked, carry on */
        if (characters_copied > -1 && characters_copied < new_buf_size)
            break;
        /* Else try again with more space. */
        if (characters_copied > -1)    /* glibc 2.1 */
            new_buf_size = characters_copied+1; /* precisely what is needed */
        else           /* glibc 2.0 */
            new_buf_size *= 2;  /* twice the old size */
        if ((tmp_char_ptr = realloc (context->tmp_buff, new_buf_size)) == NULL) {
            return NULL;
        } else {
            context->tmp_buff = tmp_char_ptr;
        }
    }
    if (new_buf_size != context->buf_size) {
        if ((tmp_char_ptr = realloc (context->out_buff, new_buf_size)) == NULL) {
            return NULL;
        }
	else {
            context->out_buff = tmp_char_ptr;
	}
        context->buf_size = new_buf_size;
    }
    tmp_char_ptr = context->out_buff;
    context->out_buff = context->tmp_buff;
    context->tmp_buff = tmp_char_ptr;
    return context->out_buff;
}
/**
 * Free the structure to manage extendable strings.
 */
void
dc_char_buf_free(char_buf_t *context){
    if (NULL == context)
    {
        return;
    }
    free(context->out_buff);
    free(context->tmp_buff);
    free(context);
}


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
char *dc_snaprintf( char *dest, size_t dest_size, const char *format, ...)
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

