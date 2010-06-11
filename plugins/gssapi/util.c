/*
 * $Id: util.c,v 1.1 2002-10-14 10:31:36 cvs Exp $
 */

/*
 * Copyright (c) 1997 - 2002 Kungliga Tekniska Högskolan (Royal Institute of
 * Technology, Stockholm, Sweden). All rights reserved.
 */


#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <ctype.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <inttypes.h>
#include <gssapi.h>
#ifdef MIT_KRB5
#include <gssapi_krb5.h>
#include <gssapi_generic.h>
#endif /* MIT_KRB5 */

void gss_print_errors (int min_stat);
void gss_err(int exitval, int status, const char *fmt, ...);

enum format_flags {
    minus_flag     =  1,
    plus_flag      =  2,
    space_flag     =  4,
    alternate_flag =  8,
    zero_flag      = 16
};


/*
 * Common state
 */

struct state {
  unsigned char *str;
  unsigned char *s;
  unsigned char *theend;
  size_t sz;
  size_t max_sz;
  void (*append_char)(struct state *, unsigned char);
  /* XXX - methods */
};



/* some math... */
int max( int a, int b)
{
    return a > b ? a : b;
}

int min( int a, int b)
{
	return a < b ? a : b;
}


/* longest integer types */

#ifdef HAVE_LONG_LONG
typedef unsigned long long u_longest;
typedef long long longest;
#else
typedef unsigned long u_longest;
typedef long longest;
#endif

static int
as_reserve (struct state *state, size_t n)
{
  if (state->s + n > state->theend) {
    int off = state->s - state->str;
    unsigned char *tmp;

    if (state->max_sz && state->sz >= state->max_sz)
      return 1;

    state->sz = max(state->sz * 2, state->sz + n);
    if (state->max_sz)
      state->sz = min(state->sz, state->max_sz);
    tmp = realloc (state->str, state->sz);
    if (tmp == NULL)
      return 1;
    state->str = tmp;
    state->s = state->str + off;
    state->theend = state->str + state->sz - 1;
  }
  return 0;
}

static void
as_append_char (struct state *state, unsigned char c)
{
  if(!as_reserve (state, 1))
    *state->s++ = c;
}/*
 * is # supposed to do anything?
 */

static int
use_alternative (int flags, u_longest num, unsigned base)
{
  return flags & alternate_flag && (base == 16 || base == 8) && num != 0;
}

static int
append_number(struct state *state,
	      u_longest num, unsigned base, char *rep,
	      int width, int prec, int flags, int minusp)
{
  int len = 0;
  int i;
  u_longest n = num;

  /* given precision, ignore zero flag */
  if(prec != -1)
    flags &= ~zero_flag;
  else
    prec = 1;
  /* zero value with zero precision -> "" */
  if(prec == 0 && n == 0)
    return 0;
  do{
    (*state->append_char)(state, rep[n % base]);
    ++len;
    n /= base;
  } while(n);
  prec -= len;
  /* pad with prec zeros */
  while(prec-- > 0){
    (*state->append_char)(state, '0');
    ++len;
  }
  /* add length of alternate prefix (added later) to len */
  if(use_alternative(flags, num, base))
    len += base / 8;
  /* pad with zeros */
  if(flags & zero_flag){
    width -= len;
    if(minusp || (flags & space_flag) || (flags & plus_flag))
      width--;
    while(width-- > 0){
      (*state->append_char)(state, '0');
      len++;
    }
  }
  /* add alternate prefix */
  if(use_alternative(flags, num, base)){
    if(base == 16)
      (*state->append_char)(state, rep[10] + 23); /* XXX */
    (*state->append_char)(state, '0');
  }
  /* add sign */
  if(minusp){
    (*state->append_char)(state, '-');
    ++len;
  } else if(flags & plus_flag) {
    (*state->append_char)(state, '+');
    ++len;
  } else if(flags & space_flag) {
    (*state->append_char)(state, ' ');
    ++len;
  }
  if(flags & minus_flag)
    /* swap before padding with spaces */
    for(i = 0; i < len / 2; i++){
      char c = state->s[-i-1];
      state->s[-i-1] = state->s[-len+i];
      state->s[-len+i] = c;
    }
  width -= len;
  while(width-- > 0){
    (*state->append_char)(state,  ' ');
    ++len;
  }
  if(!(flags & minus_flag))
    /* swap after padding with spaces */
    for(i = 0; i < len / 2; i++){
      char c = state->s[-i-1];
      state->s[-i-1] = state->s[-len+i];
      state->s[-len+i] = c;
    }
  return len;
}

/*
 * return length
 */

static int
append_string (struct state *state,
	       const unsigned char *arg,
	       int width,
	       int prec,
	       int flags)
{
    int len = 0;

    if(arg == NULL)
	arg = (const unsigned char*)"(null)";

    if(prec != -1)
	width -= prec;
    else
	width -= strlen((const char *)arg);
    if(!(flags & minus_flag))
	while(width-- > 0) {
	    (*state->append_char) (state, ' ');
	    ++len;
	}
    if (prec != -1) {
	while (*arg && prec--) {
	    (*state->append_char) (state, *arg++);
	    ++len;
	}
    } else {
	while (*arg) {
	    (*state->append_char) (state, *arg++);
	    ++len;
	}
    }
    if(flags & minus_flag)
	while(width-- > 0) {
	    (*state->append_char) (state, ' ');
	    ++len;
	}
    return len;
}

static int
append_char(struct state *state,
	    unsigned char arg,
	    int width,
	    int flags)
{
  int len = 0;

  while(!(flags & minus_flag) && --width > 0) {
    (*state->append_char) (state, ' ')    ;
    ++len;
  }
  (*state->append_char) (state, arg);
  ++len;
  while((flags & minus_flag) && --width > 0) {
    (*state->append_char) (state, ' ');
    ++len;
  }
  return 0;
}

/*
 * This can't be made into a function...
 */

#ifdef HAVE_LONG_LONG

#define PARSE_INT_FORMAT(res, arg, unsig) \
if (long_long_flag) \
     res = (unsig long long)va_arg(arg, unsig long long); \
else if (long_flag) \
     res = (unsig long)va_arg(arg, unsig long); \
else if (short_flag) \
     res = (unsig short)va_arg(arg, unsig int); \
else \
     res = (unsig int)va_arg(arg, unsig int)

#else

#define PARSE_INT_FORMAT(res, arg, unsig) \
if (long_flag) \
     res = (unsig long)va_arg(arg, unsig long); \
else if (short_flag) \
     res = (unsig short)va_arg(arg, unsig int); \
else \
     res = (unsig int)va_arg(arg, unsig int)

#endif

/*
 * zyxprintf - return length, as snprintf
 */

static int
xyzprintf (struct state *state, const char *char_format, va_list ap)
{
  const unsigned char *format = (const unsigned char *)char_format;
  unsigned char c;
  int len = 0;

  while((c = *format++)) {
    if (c == '%') {
      int flags          = 0;
      int width          = 0;
      int prec           = -1;
      int long_long_flag = 0;
      int long_flag      = 0;
      int short_flag     = 0;

      /* flags */
      while((c = *format++)){
	if(c == '-')
	  flags |= minus_flag;
	else if(c == '+')
	  flags |= plus_flag;
	else if(c == ' ')
	  flags |= space_flag;
	else if(c == '#')
	  flags |= alternate_flag;
	else if(c == '0')
	  flags |= zero_flag;
	else
	  break;
      }

      if((flags & space_flag) && (flags & plus_flag))
	flags ^= space_flag;

      if((flags & minus_flag) && (flags & zero_flag))
	flags ^= zero_flag;

      /* width */
      if (isdigit(c))
	do {
	  width = width * 10 + c - '0';
	  c = *format++;
	} while(isdigit(c));
      else if(c == '*') {
	width = va_arg(ap, int);
	c = *format++;
      }

      /* precision */
      if (c == '.') {
	prec = 0;
	c = *format++;
	if (isdigit(c))
	  do {
	    prec = prec * 10 + c - '0';
	    c = *format++;
	  } while(isdigit(c));
	else if (c == '*') {
	  prec = va_arg(ap, int);
	  c = *format++;
	}
      }

      /* size */

      if (c == 'h') {
	short_flag = 1;
	c = *format++;
      } else if (c == 'l') {
	long_flag = 1;
	c = *format++;
	if (c == 'l') {
	    long_long_flag = 1;
	    c = *format++;
	}
      }

      switch (c) {
      case 'c' :
	append_char(state, va_arg(ap, int), width, flags);
	++len;
	break;
      case 's' :
	len += append_string(state,
			     va_arg(ap, unsigned char*),
			     width,
			     prec,
			     flags);
	break;
      case 'd' :
      case 'i' : {
	longest arg;
	u_longest num;
	int minusp = 0;

	PARSE_INT_FORMAT(arg, ap, signed);

	if (arg < 0) {
	  minusp = 1;
	  num = -arg;
	} else
	  num = arg;

	len += append_number (state, num, 10, "0123456789",
			      width, prec, flags, minusp);
	break;
      }
      case 'u' : {
	u_longest arg;

	PARSE_INT_FORMAT(arg, ap, unsigned);

	len += append_number (state, arg, 10, "0123456789",
			      width, prec, flags, 0);
	break;
      }
      case 'o' : {
	u_longest arg;

	PARSE_INT_FORMAT(arg, ap, unsigned);

	len += append_number (state, arg, 010, "01234567",
			      width, prec, flags, 0);
	break;
      }
      case 'x' : {
	u_longest arg;

	PARSE_INT_FORMAT(arg, ap, unsigned);

	len += append_number (state, arg, 0x10, "0123456789abcdef",
			      width, prec, flags, 0);
	break;
      }
      case 'X' :{
	u_longest arg;

	PARSE_INT_FORMAT(arg, ap, unsigned);

	len += append_number (state, arg, 0x10, "0123456789ABCDEF",
			      width, prec, flags, 0);
	break;
      }
      case 'p' : {
	unsigned long arg = (unsigned long)va_arg(ap, void*);

	len += append_number (state, arg, 0x10, "0123456789ABCDEF",
			      width, prec, flags, 0);
	break;
      }
      case 'n' : {
	int *arg = va_arg(ap, int*);
	*arg = state->s - state->str;
	break;
      }
      case '\0' :
	  --format;
	  /* FALLTHROUGH */
      case '%' :
	(*state->append_char)(state, c);
	++len;
	break;
      default :
	(*state->append_char)(state, '%');
	(*state->append_char)(state, c);
	len += 2;
	break;
      }
    } else {
      (*state->append_char) (state, c);
      ++len;
    }
  }
  return len;
}

int
vasnprintf (char **ret, size_t max_sz, const char *format, va_list args)
{
  int st;
  struct state state;

  state.max_sz = max_sz;
  state.sz     = 1;
  state.str    = malloc(state.sz);
  if (state.str == NULL) {
    *ret = NULL;
    return -1;
  }
  state.s = state.str;
  state.theend = state.s + state.sz - 1;
  state.append_char = as_append_char;

  st = xyzprintf (&state, format, args);
  if (st > state.sz) {
    free (state.str);
    *ret = NULL;
    return -1;
  } else {
    char *tmp;

    *state.s = '\0';
    tmp = realloc (state.str, st+1);
    if (tmp == NULL) {
      free (state.str);
      *ret = NULL;
      return -1;
    }
    *ret = tmp;
    return st;
  }
}

int
vasprintf (char **ret, const char *format, va_list args)
{
  return vasnprintf (ret, 0, format, args);
}

int
asprintf (char **ret, const char *format, ...)
{
  va_list args;
  int val;

  va_start(args, format);
  val = vasprintf (ret, format, args);
  va_end(args);
  return val;
}

void
gss_print_errors (int min_stat)
{
    OM_uint32 new_stat;
    OM_uint32 msg_ctx = 0;
    gss_buffer_desc status_string;
    OM_uint32 ret;

    do {
	ret = gss_display_status (&new_stat,
				  min_stat,
				  GSS_C_MECH_CODE,
				  GSS_C_NO_OID,
				  &msg_ctx,
				  &status_string);
	fprintf (stderr, "%s\n", (char *)status_string.value);
	gss_release_buffer (&new_stat, &status_string);
    } while (!GSS_ERROR(ret) && msg_ctx != 0);
}

void
sockaddr_to_gss_address (const struct sockaddr *sa,
			 OM_uint32 *addr_type,
			 gss_buffer_desc *gss_addr)
{
	struct sockaddr_in *sin;

    switch (sa->sa_family) {
    case AF_INET :
		sin = (struct sockaddr_in *)sa;

		gss_addr->length = 4;
		gss_addr->value  = &sin->sin_addr;
		*addr_type       = GSS_C_AF_INET;
		break;
    default :
		fprintf(stderr, "unknown address family %d", sa->sa_family);
		break;
    }
}
