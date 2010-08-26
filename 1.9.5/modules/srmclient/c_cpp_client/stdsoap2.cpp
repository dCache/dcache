/*	stdsoap2.c[pp] 2.2.3b

The contents of this file are subject to the gSOAP Public License Version 1.0
(the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.cs.fsu.edu/~engelen/soaplicense.html
Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.

The Initial Developer of the Original Code is Robert A. van Engelen.
Copyright (C) 2000-2003 Robert A. van Engelen, Genivia inc. All Rights Reserved.

Note:

Win32 build needs winsock.dll (Visual C++ "wsock32.lib")
To do this in Visual C++ 6.0, go to "Project", "settings", select the "Link"
tab (the project file needs to be selected in the file view) and add
"wsock32.lib" to the "Object/library modules" entry

*/

#include "stdsoap2.h"

#ifdef __cplusplus
SOAP_SOURCE_STAMP("@(#) stdsoap2.cpp ver 2.2.3b 2003-03-06 12:00:00 GMT")
extern "C" {
#else
SOAP_SOURCE_STAMP("@(#) stdsoap2.c ver 2.2.3b 2003-03-06 12:00:00 GMT")
#endif

/*      EOF=-1 */
#define LT (-2) /* XML character '<' */
#define TT (-3) /* XML character '</' */
#define GT (-4) /* XML character '>' */
#define QT (-5) /* XML character '"' */
#define AP (-6) /* XML character ''' */

#define blank(c) ((c)>=0 && (c)<=32)
#define notblank(c) ((c)>32)
#define hash_ptr(p) (((long)(p)>>3)&(SOAP_PTRHASH-1))

static const char soap_env1[42] = "http://schemas.xmlsoap.org/soap/envelope/";
static const char soap_enc1[42] = "http://schemas.xmlsoap.org/soap/encoding/";
static const char soap_env2[40] = "http://www.w3.org/2002/06/soap-envelope";
static const char soap_enc2[40] = "http://www.w3.org/2002/06/soap-encoding";

static wchar soap_char(struct soap*);
static wchar soap_getchunkchar(struct soap*);
static int hash_id(const char*);
static void soap_update_ptrs(struct soap*, char*, char*, long);
static void soap_add_copies(struct soap*, char*, char*, long);
static int soap_putdimefield(struct soap*, const char*, size_t);
static char *soap_getdimefield(struct soap*, int);
static char *soap_base64_string(const char*, char*, size_t);
static int soap_puthttphdr(struct soap*, int, size_t);
static void soap_init_iht(struct soap*);
static void soap_free_iht(struct soap*);
static void soap_init_pht(struct soap*);
static void soap_free_pht(struct soap*);
static int soap_set_error(struct soap*, const char*, const char*, const char*, int);
static int soap_set_sender_error(struct soap*, const char*, const char*, int);
static int soap_set_receiver_error(struct soap*, const char*, const char*, int);
static int soap_copy_fault(struct soap*, const char*, const char*, const char*);
static int soap_element(struct soap*, const char*, int, const char*);
static int soap_getattrval(struct soap*, char*, size_t, wchar);
static size_t soap_begin_dime(struct soap*);
static void soap_close_logfile(struct soap*, int);
static void soap_set_logfile(struct soap*, int, const char*);
static void soap_set_local_namespaces(struct soap*);
#ifdef WITH_GZIP
static int soap_getgzipheader(struct soap*);
#endif
#ifndef UNDER_CE
static time_t soap_timegm(struct tm*);
#endif

static int http_post(struct soap*, const char*, const char*, int, const char*, const char*, size_t);
static int http_post_header(struct soap*, const char*, const char*);
static int http_response(struct soap*, int, size_t);
static int http_parse(struct soap*);
static int http_parse_header(struct soap*, const char*, const char*);
static int tcp_connect(struct soap*, const char*, const char*, int);
static int tcp_accept(struct soap*, SOAP_SOCKET, struct sockaddr*, int*);
static int tcp_disconnect(struct soap*);
static int fsend(struct soap*, const char*, size_t);
static size_t frecv(struct soap*, char*, size_t);
static int fignore(struct soap*, const char*);
static void *fplugin(struct soap*, const char*);

static const char base64[65] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

struct soap_double_nan soap_double_nan = {0xFFFFFFFF, 0xFFFFFFFF};

struct code_map
{ int code;
  const char *string;
};

static struct code_map h_errno_codes[] =
{
#ifdef HOST_NOT_FOUND   
    { HOST_NOT_FOUND, "Host not found" },
#endif
#ifdef TRY_AGAIN
    { TRY_AGAIN, "Try Again" },
#endif
#ifdef NO_RECOVERY  
    { NO_RECOVERY, "No Recovery" },
#endif
#ifdef NO_DATA
    { NO_DATA, "No Data" },
#endif
#ifdef NO_ADDRESS
    { NO_ADDRESS, "No Address" },
#endif
    {0, NULL}
};

#ifdef WITH_OPENSSL
static SSL_CTX *soap_ssl_ctx = NULL;
static const char *soap_ssl_pw = NULL;
#endif

#ifdef WIN32
static int tcp_done = 0;
#endif

/******************************************************************************/

#ifndef MAC_CARBON
/******************************************************************************/
static int
fsend(struct soap *soap, const char *s, size_t n)
{ int nwritten;
  while (n)
  { if (soap->socket >= 0)
    { if (soap->send_timeout)
      { struct timeval timeout;
        fd_set fd;
        if (soap->send_timeout > 0)
        { timeout.tv_sec = soap->send_timeout;
          timeout.tv_usec = 0;
        }
        else
        { timeout.tv_sec = -soap->send_timeout/1000000;
          timeout.tv_usec = -soap->send_timeout%1000000;
        }
        FD_ZERO(&fd);
        FD_SET(soap->socket, &fd);
        for (;;)
        { int r = select(soap->socket + 1, NULL, &fd, &fd, &timeout);
          if (r > 0)
            break;
          if (!r)
          { soap->errnum = 0;
            return SOAP_EOF;
          }
          if (soap_socket_errno != SOAP_EINTR)
          { soap->errnum = soap_socket_errno;
            return SOAP_EOF;
          }
        }
      }
#ifdef WITH_OPENSSL
      if (soap->ssl)
        nwritten = SSL_write(soap->ssl, s, n);
      else
#endif
        nwritten = send(soap->socket, s, n, soap->socket_flags);
      if (nwritten <= 0)
      {
#ifdef WITH_OPENSSL
        if (soap->ssl && SSL_get_error(soap->ssl, nwritten) != SSL_ERROR_NONE)
          return SOAP_EOF;
#endif
        if (soap_socket_errno != SOAP_EINTR && soap_socket_errno != SOAP_EWOULDBLOCK && soap_socket_errno != SOAP_EAGAIN)
        { soap->errnum = soap_socket_errno;
          return SOAP_EOF;
        }
        nwritten = 0; /* and call write() again */
      }
    }
    else
    {
#ifdef WITH_FASTCGI
      nwritten = fwrite(s, 1, n, stdout);
      fflush(stdout);
#else
#ifdef UNDER_CE
      nwritten = fwrite(s, 1, n, soap->sendfd);
#else
      nwritten = write(soap->sendfd, s, n);
#endif
#endif
      if (nwritten <= 0)
      { if (soap_errno != SOAP_EINTR && soap_errno != SOAP_EWOULDBLOCK && soap_errno != SOAP_EAGAIN)
        { soap->errnum = soap_errno;
          return SOAP_EOF;
        }
        nwritten = 0; /* and call write() again */
      }
    }
    n -= nwritten;
    s += nwritten;
  }
  return SOAP_OK;
}
/******************************************************************************/
#endif

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_flush_raw(struct soap *soap, const char *s, size_t n)
{ if ((soap->mode & SOAP_IO) == SOAP_IO_STORE)
  { char *t;
    if (!(t = (char*)soap_push_block(soap, n)))
      return soap->error = SOAP_EOM;
    memcpy(t, s, n);
    return SOAP_OK;
  }
  if ((soap->mode & SOAP_IO) == SOAP_IO_CHUNK)
  { char t[16];
    sprintf(t, "\r\n%lX\r\n" + (soap->chunksize ? 0 : 2), (unsigned long)n);
    DBGMSG(SENT, t, strlen(t));
    if ((soap->error = soap->fsend(soap, t, strlen(t))))
      return soap->error;
    soap->chunksize += n;
  }
  DBGMSG(SENT, s, n);
  return soap->error = soap->fsend(soap, s, n);
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_flush(struct soap *soap)
{ if (soap->bufidx)
  {
#ifdef WITH_ZLIB
    if (soap->mode & SOAP_ENC_ZLIB)
    { soap->d_stream.next_in = (Byte*)soap->buf;
      soap->d_stream.avail_in = (unsigned int)soap->bufidx;
#ifdef WITH_GZIP
      soap->z_crc = crc32(soap->z_crc, (Byte*)soap->buf, (unsigned int)soap->bufidx);
#endif
      do
      { DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Deflating %u bytes\n", soap->d_stream.avail_in));
        if (deflate(&soap->d_stream, Z_NO_FLUSH) != Z_OK)
        { DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Unable to deflate: %s\n", soap->d_stream.msg?soap->d_stream.msg:""));
          return soap->error = SOAP_ZLIB_ERROR;
        }
        if (!soap->d_stream.avail_out)
        { if (soap_flush_raw(soap, soap->z_buf, SOAP_BUFLEN))
            return soap->error;
          soap->d_stream.next_out = (Byte*)soap->z_buf;
          soap->d_stream.avail_out = SOAP_BUFLEN;
        }
      } while (soap->d_stream.avail_in);
    }
    else
#endif
    if (soap_flush_raw(soap, soap->buf, soap->bufidx))
      return soap->error;
    soap->bufidx = 0;
  }
  return SOAP_OK;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_send_raw(struct soap *soap, const char *s, size_t n)
{ if (soap->mode & SOAP_IO_LENGTH)
  { soap->count += n;
    return SOAP_OK;
  }
  if (soap->mode & SOAP_IO)
  { size_t i = SOAP_BUFLEN - soap->bufidx;
    while (n >= i)
    { memcpy(soap->buf + soap->bufidx, s, i);
      soap->bufidx = SOAP_BUFLEN;
      if (soap_flush(soap))
        return soap->error;
      s += i;
      n -= i;
      i = SOAP_BUFLEN;
    }
    memcpy(soap->buf + soap->bufidx, s, n);
    soap->bufidx += n;
    return SOAP_OK;
  }
  return soap_flush_raw(soap, s, n);
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_send(struct soap *soap, const char *s)
{ if (s)
    return soap_send_raw(soap, s, strlen(s));
  return SOAP_OK;
}

/******************************************************************************/

#ifndef MAC_CARBON
/******************************************************************************/
static size_t
frecv(struct soap *soap, char *s, size_t n)
{ int r;
  if (soap->socket >= 0)
  { for (;;)
    { if (soap->recv_timeout)
      { struct timeval timeout;
        fd_set fd;
        if (soap->recv_timeout > 0)
        { timeout.tv_sec = soap->recv_timeout;
          timeout.tv_usec = 0;
        }
        else
        { timeout.tv_sec = -soap->recv_timeout/1000000;
          timeout.tv_usec = -soap->recv_timeout%1000000;
        }
        FD_ZERO(&fd);
        FD_SET(soap->socket, &fd);
        for (;;)
        { r = select(soap->socket + 1, &fd, NULL, &fd, &timeout);
          if (r > 0)
            break;
          if (!r)
          { soap->errnum = 0;
            return 0;
          }
          if (soap_socket_errno != SOAP_EINTR)
          { soap->errnum = soap_socket_errno;
            return 0;
          }
        }
      }
#ifdef WITH_OPENSSL
      if (soap->ssl)
      { r = SSL_read(soap->ssl, s, n);
        if (SSL_get_error(soap->ssl, r) == SSL_ERROR_NONE)
          return (size_t)r;
        return 0;
      }
#endif
      r = recv(soap->socket, s, n, soap->socket_flags);
      if (r >= 0)
        return (size_t)r;
      if (soap_socket_errno != SOAP_EINTR && soap_socket_errno != SOAP_EAGAIN)
      { soap->errnum = soap_socket_errno;
        return 0;
      }
      else
      { struct timeval timeout;
        fd_set fd;
        timeout.tv_sec = 1;
        timeout.tv_usec = 0;
        FD_ZERO(&fd);
        FD_SET(soap->socket, &fd);
        r = select(soap->socket + 1, &fd, NULL, &fd, &timeout);
        if (r < 0 && soap_socket_errno != SOAP_EINTR)
        { soap->errnum = soap_socket_errno;
          return 0;
        }
      }
    }
  }
#ifdef WITH_FASTCGI
  return fread(s, 1, n, stdin);
#else
#ifdef UNDER_CE
  return fread(s, 1, n, soap->recvfd);
#else
  r = read(soap->recvfd, s, n);
  if (r >= 0)
    return r;
  return 0;
#endif
#endif
}
/******************************************************************************/
#endif

/******************************************************************************/
static wchar
soap_getchunkchar(struct soap *soap)
{ if (soap->bufidx < soap->buflen)
    return soap->buf[soap->bufidx++];
  soap->bufidx = 0;
  soap->buflen = soap->chunkbuflen = soap->frecv(soap, soap->buf, SOAP_BUFLEN);
  DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Read %u bytes\n", (unsigned int)soap->buflen));
  DBGMSG(RECV, soap->buf, soap->buflen);
  if (soap->buflen)
    return soap->buf[soap->bufidx++];
  return EOF;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_recv_raw(struct soap *soap)
{ size_t ret;
#ifdef WITH_ZLIB
  if (soap->mode & SOAP_ENC_ZLIB)
  { if (soap->d_stream.next_out == Z_NULL)
      return EOF;
    if (soap->d_stream.avail_in || !soap->d_stream.avail_out)
    { int r;
      DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Inflating\n"));
      soap->d_stream.next_out = (Byte*)soap->buf;
      soap->d_stream.avail_out = SOAP_BUFLEN;
      r = inflate(&soap->d_stream, Z_NO_FLUSH);
      if (r == Z_OK || r == Z_STREAM_END)
      { soap->bufidx = 0;
        soap->buflen = SOAP_BUFLEN - soap->d_stream.avail_out;
        if (soap->zlib_in == SOAP_ZLIB_GZIP)
          soap->z_crc = crc32(soap->z_crc, (Byte*)soap->buf, (unsigned int)soap->buflen);
        if (r == Z_STREAM_END)
        { DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Inflated %lu->%lu bytes\n", soap->d_stream.total_in, soap->d_stream.total_out));
          soap->z_ratio_in = (float)soap->d_stream.total_in / (float)soap->d_stream.total_out;
          soap->d_stream.next_out = Z_NULL;
        }
        if (soap->buflen)
        { soap->count += soap->buflen;
          return SOAP_OK;
        }
      }
      else if (r != Z_BUF_ERROR)
      { DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Inflate error: %s\n", soap->d_stream.msg?soap->d_stream.msg:""));
        soap->d_stream.next_out = Z_NULL;
        return EOF;
      }
    }
zlib_again:
    if ((soap->mode & SOAP_IO) == SOAP_IO_CHUNK && !soap->chunksize)
    { memcpy(soap->buf, soap->z_buf, SOAP_BUFLEN);
      soap->buflen = soap->z_buflen;
    }
  }
#endif
  if ((soap->mode & SOAP_IO) == SOAP_IO_CHUNK) /* read HTTP chunked transfer */
  { 
chunk_again:
    if (soap->chunksize)
    { if (soap->chunksize > SOAP_BUFLEN)
        soap->buflen = ret = soap->frecv(soap, soap->buf, SOAP_BUFLEN);
      else
        soap->buflen = ret = soap->frecv(soap, soap->buf, soap->chunksize);
      DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Getting chunk: read %u bytes\n", (unsigned int)ret));
      DBGMSG(RECV, soap->buf, ret);
      soap->bufidx = 0;
      soap->chunksize -= ret;
    }
    else
    { wchar c;
      char tmp[8], *t;
      t = tmp;
      if (!soap->chunkbuflen)
      { soap->chunkbuflen = ret = soap->frecv(soap, soap->buf, SOAP_BUFLEN);
        DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Read %u bytes\n", (unsigned int)ret));
        DBGMSG(RECV, soap->buf, ret);
        soap->bufidx = 0;
        if (!ret)
          return EOF;
      }
      else
        soap->bufidx = soap->buflen;
      soap->buflen = soap->chunkbuflen;
      DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Getting chunk size (%u %u)\n", (unsigned int)soap->bufidx, (unsigned int)soap->buflen));
      while (!isxdigit(c = soap_getchunkchar(soap)))
        if (c == EOF)
	  return EOF;
      do
        *t++ = (char)c;
      while (isxdigit(c = soap_getchunkchar(soap)) && t - tmp < 7);
      while (c != EOF && c != '\n')
        c = soap_getchunkchar(soap);
      if (c == EOF)
        return EOF;
      *t = '\0';
      soap->chunksize = strtoul(tmp, &t, 16);
      if (!soap->chunksize)
      { soap->chunkbuflen = 0;
        DBGLOG(TEST, SOAP_MESSAGE(fdebug, "End of chunked message\n"));
	while (c != EOF && c != '\n')
          c = soap_getchunkchar(soap);
        return EOF;
      }
      soap->buflen = soap->bufidx + soap->chunksize;
      DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Moving buf len to %u (%u %s)\n", (unsigned int)soap->buflen, (unsigned int)soap->bufidx, tmp));
      if (soap->buflen > soap->chunkbuflen)
      { soap->buflen = soap->chunkbuflen;
        soap->chunksize -= soap->buflen - soap->bufidx;
        soap->chunkbuflen = 0;
        DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Passed end of buffer for chunked HTTP (%lu bytes left)\n", (unsigned long)(soap->buflen - soap->bufidx)));
      }
      else if (soap->chunkbuflen)
        soap->chunksize = 0;
      ret = soap->buflen - soap->bufidx;
      if (!ret)
        goto chunk_again;
    }
  }
  else
  { soap->bufidx = 0;
    soap->buflen = ret = soap->frecv(soap, soap->buf, SOAP_BUFLEN);
    DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Read %u bytes\n", (unsigned int)ret));
    DBGMSG(RECV, soap->buf, ret);
  }
#ifdef WITH_ZLIB
  if (soap->mode & SOAP_ENC_ZLIB)
  { int r;
    memcpy(soap->z_buf, soap->buf, SOAP_BUFLEN);
    soap->d_stream.next_in = (Byte*)(soap->z_buf + soap->bufidx);
    soap->d_stream.avail_in = (unsigned int)ret;
    soap->d_stream.next_out = (Byte*)soap->buf;
    soap->d_stream.avail_out = SOAP_BUFLEN;
    r = inflate(&soap->d_stream, Z_NO_FLUSH);
    if (r == Z_OK || r == Z_STREAM_END)
    { soap->bufidx = 0;
      soap->z_buflen = soap->buflen;
      soap->buflen = ret = SOAP_BUFLEN - soap->d_stream.avail_out;
      if (soap->zlib_in == SOAP_ZLIB_GZIP)
        soap->z_crc = crc32(soap->z_crc, (Byte*)soap->buf, (unsigned int)soap->buflen);
      DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Inflated %u bytes\n", (unsigned int)ret));
      if (!ret)
        goto zlib_again;
      if (r == Z_STREAM_END)
      { DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Inflated %lu->%lu bytes\n", soap->d_stream.total_in, soap->d_stream.total_out));
        soap->z_ratio_in = (float)soap->d_stream.total_in / (float)soap->d_stream.total_out;
        soap->d_stream.next_out = Z_NULL;
      }
    }
    else
    { DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Unable to inflate: (%d) %s\n", r, soap->d_stream.msg?soap->d_stream.msg:""));
      soap->d_stream.next_out = Z_NULL;
      return EOF;
    }
  }
#endif
  soap->count += ret;
  return !ret;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_recv(struct soap *soap)
{ if (soap->mode & SOAP_ENC_DIME)
  { if (soap->dime_buflen)
    { char *s;
      int i;
      unsigned char tmp[12];
      DBGLOG(TEST, SOAP_MESSAGE(fdebug, "DIME hdr for chunked DIME is in buffer\n"));
      soap->count += soap->dime_buflen - soap->buflen;
      soap->buflen = soap->dime_buflen;
      DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Skip padding (%u bytes)\n", -(int)soap->dime_size&3));
      for (i = -(int)soap->dime_size&3; i > 0; i--)
      { soap->bufidx++;
        if (soap->bufidx >= soap->buflen)
          if (soap_recv_raw(soap))
            return EOF;
      }
      DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Get DIME hdr for next chunk\n"));
      s = (char*)tmp;
      for (i = 12; i > 0; i--)
      { *s++ = soap->buf[soap->bufidx++];
        if (soap->bufidx >= soap->buflen)
          if (soap_recv_raw(soap))
            return EOF;
      }
      soap->dime_flags = tmp[0]&0x7;
      soap->dime_size = tmp[8] << 24 | tmp[9] << 16 | tmp[10] << 8 | tmp[11];
      DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Get DIME chunk (%u bytes)\n", (unsigned int)soap->dime_size));
      if (soap->dime_flags & SOAP_DIME_CF)
      { DBGLOG(TEST, SOAP_MESSAGE(fdebug, "More chunking\n"));
        soap->dime_chunksize = soap->dime_size;
        if (soap->buflen - soap->bufidx >= soap->dime_size)
        { soap->dime_buflen = soap->buflen;
          soap->buflen = soap->bufidx + soap->dime_chunksize;
        }
        else
          soap->dime_chunksize -= soap->buflen - soap->bufidx;
      }
      else
      { DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Last chunk\n"));
        soap->dime_buflen = 0;
        soap->dime_chunksize = 0;
      }
      soap->count = soap->buflen - soap->bufidx;
      DBGLOG(TEST, SOAP_MESSAGE(fdebug, "%u bytes remaining\n", (unsigned int)soap->count));
      return SOAP_OK;
    }
    if (soap->dime_chunksize)
    { DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Get next DIME hdr for chunked DIME (%u bytes chunk)\n", (unsigned int)soap->dime_chunksize));
      if (soap_recv_raw(soap))
        return EOF;
      if (soap->buflen - soap->bufidx >= soap->dime_chunksize)
      { soap->dime_buflen = soap->buflen;
        soap->count -= soap->buflen - soap->bufidx - soap->dime_chunksize;
        soap->buflen = soap->bufidx + soap->dime_chunksize;
      }
      else
        soap->dime_chunksize -= soap->buflen - soap->bufidx;
      DBGLOG(TEST, SOAP_MESSAGE(fdebug, "%lu bytes remaining, count=%u\n", (unsigned long)(soap->buflen-soap->bufidx), (unsigned int)soap->count));
      return SOAP_OK;
    }
  }
  return soap_recv_raw(soap);
}

/******************************************************************************/
SOAP_FMAC1
wchar
SOAP_FMAC2
soap_getchar(struct soap *soap)
{ register wchar c;
  if (soap->ahead)
  { c = soap->ahead;
    soap->ahead = 0;
    return c;
  }
  return soap_get1(soap);
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_tag_cmp(register const char *s, register const char *t)
{ for (; *s; s++, t++)
    if (tolower(*s) != tolower(*t))
      if (*t != '-')
      { if (*t != '*')
          return 1;
        if (*++t)
        { register int c = tolower(*t);
          for (; *s; s++)
          { if (tolower(*s) == c)
              if (!soap_tag_cmp(s+1, t+1))
                return 0;
          }
          break;
        }
        else
          return 0;
      }
  if (*t == '*' && !t[1])
    return 0;
  return *t;
}

/******************************************************************************/
static wchar
soap_char(struct soap *soap)
{ int i, c;
  char tmp[8], *t = tmp;
  for (i = 0; i < 7; i++)
  { c = soap_get1(soap);
    if (c == ';' || c == EOF)
      break;
    *t++ = c;
  }
  *t = '\0';
  if (*tmp == '#')
  { if (tmp[1] == 'x' || tmp[1] == 'X')
      return strtol(tmp + 2, NULL, 16) | 0x80000000;
    return atol(tmp + 1) | 0x80000000;
  }
  if (!soap_tag_cmp(tmp, "lt"))
    return '<';
  if (!soap_tag_cmp(tmp, "gt"))
    return '>';
  if (!soap_tag_cmp(tmp, "amp"))
    return '&';
  if (!soap_tag_cmp(tmp, "quot"))
    return '"';
  if (!soap_tag_cmp(tmp, "apos"))
    return '\'';
  return '?';
}

/******************************************************************************/
SOAP_FMAC1
wchar
SOAP_FMAC2
soap_get(struct soap *soap)
{ register wchar c;
  if (soap->ahead)
  { c = soap->ahead;
    soap->ahead = 0;
  }
  else
    c = soap_get1(soap);
redo:
  if (soap->cdata)
  { if (c == ']')
    { c = soap_get1(soap);
      if (c == ']')
      { soap->cdata = 0;
        c = soap_get1(soap); /* skip > */
        c = soap_get1(soap);
        goto redo;
      }
      else
      { /* soap->ahead2 = c; */
	soap_revget1(soap);
        return ']';
      }
    }
    else
      return c;
  }
  switch (c)
  { case '<':
      do c = soap_get1(soap);
      while (blank(c));
      if (c == '!' || c == '?' || c == '%')
      { if (c == '!')
        { c = soap_get1(soap);
          if (c == '[')
          { do c = soap_get1(soap);
            while (c != EOF && c != '[');
            if (c == EOF)
              return EOF;
            soap->cdata = 1;
            return soap_get1(soap);
          }
          if (c == '-' && (c = soap_get1(soap)) == '-')
          { do
            { c = soap_get1(soap);
              if (c == '-' && (c = soap_get1(soap)) == '-')
                break;
            } while (c != EOF);
          }
        }
        while (c != EOF && c != '>')
          c = soap_get1(soap);
        if (c == '>')
        { c = soap_get1(soap);
          goto redo;
        }
        return EOF;
      }
      if (c == '/')
        return TT;
      /* soap->ahead2 = c; */
      soap_revget1(soap);
      return LT;
    case '>':
      return GT;
    case '"':
      return QT;
    case '\'':
      return AP;
    case '&':
      c = soap_char(soap);
  }
  return c;
}

/******************************************************************************/
SOAP_FMAC1
wchar
SOAP_FMAC2
soap_advance(struct soap *soap)
{ wchar c;
  while (((c = soap_get(soap)) != EOF) && c != LT && c != TT)
    ;
  return c;
}

/******************************************************************************/
SOAP_FMAC1
wchar
SOAP_FMAC2
soap_skip(struct soap *soap)
{ register wchar c;
  do c = soap_get(soap);
  while (blank(c));
  return c;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_move(struct soap *soap, int n)
{ DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Moving %d bytes forward\n", n));
  for (; n > 0; n--)
    if (soap_getchar(soap) == EOF)
      return SOAP_EOF;
  return SOAP_OK;
}

/******************************************************************************/
SOAP_FMAC1
size_t
SOAP_FMAC2
soap_tell(struct soap *soap)
{ return soap->count - soap->buflen + soap->bufidx - (soap->ahead != 0);
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_pututf8(struct soap *soap, wchar c)
{ char tmp[16];
  if (c > 0 && c < 0x80)
  { tmp[0] = (char)c;
    tmp[1] = '\0';
  }
  else
    sprintf(tmp, "&#%lu;", (unsigned long)c);
  return soap_send(soap, tmp);
}

/******************************************************************************/
SOAP_FMAC1
wchar
SOAP_FMAC2
soap_getutf8(struct soap *soap)
{ register wchar c;
  c = soap_get(soap);
  if (c < 0x80)
    return c;
  if (c < 0xE0)
    return (wchar)(c & 0x1F) << 6 | (wchar)(soap_get1(soap) & 0x3F);
  if (c < 0xF0)
    return (wchar)(c & 0x0F) << 12 | (wchar)(soap_get1(soap) & 0x3F) << 6 | (wchar)(soap_get1(soap) & 0x3F);
  if (c < 0xF8)
    return (wchar)(c & 0x07) << 18 | (wchar)(soap_get1(soap) & 0x3F) << 12 | (wchar)(soap_get1(soap) & 0x3F) << 6 | (wchar)(soap_get1(soap) & 0x3F);
  if (c < 0xFA)
    return (wchar)(c & 0x07) << 24 | (wchar)(soap_get1(soap) & 0x3F) << 18 | (wchar)(soap_get1(soap) & 0x3F) << 12 | (wchar)(soap_get1(soap) & 0x3F) << 6 | (wchar)(soap_get1(soap) & 0x3F);
  return (wchar)(c & 0x07) << 30 | (wchar)(soap_get1(soap) & 0x3F) << 24 | (wchar)(soap_get1(soap) & 0x3F) << 18 | (wchar)(soap_get1(soap) & 0x3F) << 12 | (wchar)(soap_get1(soap) & 0x3F) << 6 | (wchar)(soap_get1(soap) & 0x3F);
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_puthex(struct soap *soap, int n)
{ char d[2];
  d[0] = (n >> 4) + (n > 159 ? '7' : '0');
  n &= 0x0F;
  d[1] = n + (n > 9 ? '7' : '0');
  return soap_send_raw(soap, d, 2);
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_gethex(struct soap *soap)
{ wchar c;
  char d1, d2;
  if ((c = soap_get(soap)) < 0)
  { soap_unget(soap, c);
    return EOF;
  }
  d1 = (char)c;
  if ((c = soap_get(soap)) < 0)
  { soap_unget(soap, c);
    return EOF;
  }
  d2 = (char)c;
  return ((d1 >= 'A' ? (d1 & 0x7) + 9 : d1 - '0') << 4) + (d2 >= 'A' ? (d2 & 0x7) + 9 : d2 - '0');
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_putbase64(struct soap *soap, const unsigned char *s, size_t n)
{ register size_t i;
  register unsigned long m;
  char d[4];
  if (!s)
    return SOAP_OK;
  for (; n > 2; n -= 3, s += 3)
  { m = s[0] << 16 | s[1] << 8 | s[2];
    for (i = 4; i > 0; m >>= 6)
      d[--i] = base64[m & 0x3F];
    if (soap_send_raw(soap, d, 4))
      return soap->error;
  }
  if (n)
  { m = 0;
    for (i = 0; i < n; i++)
      m = m << 8 | *s++;
    for (; i < 3; i++)
      m <<= 8;
    for (i++; i > 0; m >>= 6)
      d[--i] = base64[m & 0x3F];
    for (i = 3; i > n; i--)
      d[i] = '=';
    if (soap_send_raw(soap, d, 4))
      return soap->error;
  }
  return SOAP_OK;
}

/******************************************************************************/
SOAP_FMAC1
unsigned char*
SOAP_FMAC2
soap_getbase64(struct soap *soap, size_t *n, int malloc_flag)
{ static const char base64c[81] = "\76XXX\77\64\65\66\67\70\71\72\73\74\75XXXXXXX\00\01\02\03\04\05\06\07\10\11\12\13\14\15\16\17\20\21\22\23\24\25\26\27\30\31XXXXXX\32\33\34\35\36\37\40\41\42\43\44\45\46\47\50\51\52\53\54\55\56\57\60\61\62\63";
  register int i, j;
  register wchar c;
  register unsigned long m;
  unsigned char *p;
  char *s;
  if (soap_new_block(soap))
    return NULL;
  for (;;)
  { s = (char*)soap_push_block(soap, 3*SOAP_BLKLEN); /* must be multiple of 3 */
    for (i = 0; i < SOAP_BLKLEN; i++)
    { m = 0;
      j = 0;
      while (j < 4)
      { c = soap_get(soap);
        if (c == '=' || c < 0)
        { i *= 3;
          switch (j)
          { case 2:
              *s++ = (char)((m >> 4) & 0xFF);
              i++;
              break;
            case 3:
              *s++ = (char)((m >> 10) & 0xFF);
              *s++ = (char)((m >> 2) & 0xFF);
              i += 2;
          }
          *n = soap_size_block(soap, i);
          p = (unsigned char*)soap_save_block(soap, NULL);
          if (c >= 0)
            c = soap_advance(soap);
          soap_unget(soap, c);
          return p;
        }
        c -= '+';
        if (c >= 0 && c <= 79)
        { m = (m << 6) + base64c[c];
          j++;
        }
      }
      *s++ = (char)((m >> 16) & 0xFF);
      *s++ = (char)((m >> 8) & 0xFF);
      *s++ = (char)(m & 0xFF);
    }
  }
}

/******************************************************************************/
static char*
soap_strerror(struct soap *soap, int soaperror)
{ if (soaperror)
  {
#ifndef UNDER_CE
    return strerror(soaperror);
#else
    FormatMessage(FORMAT_MESSAGE_FROM_SYSTEM|FORMAT_MESSAGE_IGNORE_INSERTS, NULL, soaperror, 0, (LPTSTR)&soap->werrorstr, 256, NULL);
    wcstombs(soap->errorstr, soap->werrorstr, 256);
    return soap->errorstr;
#endif
  }
  return NULL;
}

/******************************************************************************/
static int
soap_set_error(struct soap *soap, const char *faultcode, const char *faultstring, const char *faultdetail, int soaperror)
{ const char **s = soap_faultdetail(soap);
  *soap_faultcode(soap) = faultcode;
  *soap_faultstring(soap) = faultstring;
  if (s)
    *s = faultdetail;
  return soap->error = soaperror;
}

/******************************************************************************/
static int
soap_set_sender_error(struct soap *soap, const char *faultstring, const char *faultdetail, int soaperror)
{ return soap_set_error(soap, soap->version == 2 ? "SOAP-ENV:Sender" : "SOAP-ENV:Client", faultstring, faultdetail, soaperror);
}

/******************************************************************************/
static int
soap_set_receiver_error(struct soap *soap, const char *faultstring, const char *faultdetail, int soaperror)
{ return soap_set_error(soap, soap->version == 2 ? "SOAP-ENV:Receiver" : "SOAP-ENV:Server", faultstring, faultdetail, soaperror);
}

/******************************************************************************/
static int
soap_copy_fault(struct soap *soap, const char *faultcode, const char *faultstring, const char *faultdetail)
{ char *s = NULL, *t = NULL;
  if (faultstring)
  { s = (char*)soap_malloc(soap, strlen(faultstring)+1);
    if (s)
      strcpy(s, faultstring);
  }
  if (faultdetail)
  { t = (char*)soap_malloc(soap, strlen(faultdetail)+1);
    if (t)
      strcpy(t, faultdetail);
  }
  return soap_set_error(soap, faultcode, s, t, SOAP_FAULT);
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_sender_fault(struct soap *soap, const char *faultstring, const char *faultdetail)
{ return soap_copy_fault(soap, soap->version == 2 ? "SOAP-ENV:Sender" : "SOAP-ENV:Client", faultstring, faultdetail);
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_receiver_fault(struct soap *soap, const char *faultstring, const char *faultdetail)
{ return soap_copy_fault(soap, soap->version == 2 ? "SOAP-ENV:Receiver" : "SOAP-ENV:Server", faultstring, faultdetail);
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_new_block(struct soap *soap)
{ struct soap_blist *p;
  DBGLOG(TEST, SOAP_MESSAGE(fdebug, "New block sequence (prev=%p)\n", soap->blist));
  if (!(p = (struct soap_blist*)malloc(sizeof(struct soap_blist))))
    return SOAP_EOM;   
  p->next = soap->blist; 
  p->ptr = NULL;
  p->size = 0;
  soap->blist = p;
  return SOAP_OK;
}

/******************************************************************************/
SOAP_FMAC1
void*
SOAP_FMAC2
soap_push_block(struct soap *soap, size_t n)
{ char *p;
  DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Push block of %u bytes (%u bytes total)\n", (unsigned int)n, (unsigned int)soap->blist->size));
  if (!(p = (char*)malloc(n + sizeof(char*) + sizeof(size_t))))
    return NULL;
  *((char**)p) = soap->blist->ptr;
  *((size_t*)(p + sizeof(char*))) = n;
  soap->blist->ptr = p;
  soap->blist->size += n;
  return p + sizeof(char*) + sizeof(size_t);
}

/******************************************************************************/
SOAP_FMAC1
void
SOAP_FMAC2
soap_pop_block(struct soap *soap)
{ char *p;
  if (!soap->blist->ptr)
    return;
  DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Pop block\n"));
  p = soap->blist->ptr;
  soap->blist->size -= *(size_t*)(p + sizeof(char*));
  soap->blist->ptr = *(char**)p;
  free(p);
}

/******************************************************************************/
static void
soap_update_ptrs(struct soap *soap, char *start, char *end, long offset)
{ int i;
  struct soap_ilist *ip;
  void *p, **q;
  for (i = 0; i < SOAP_IDHASH; i++)
    for (ip = soap->iht[i]; ip; ip = ip->next)
    { if (ip->ptr && (char*)ip->ptr >= start && (char*)ip->ptr < end)
        ip->ptr = (char*)ip->ptr + offset;
      for (q = &ip->link; q; q = (void**)p)
      { p = *q;
        if (p && (char*)p >= start && (char*)p < end)
        { DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Link update id='%s' %p\n", ip->id, p));
          *q = (char*)p + offset;
        }
      }
      for (q = &ip->copy; q; q = (void**)p)
      { p = *q;
        if (p && (char*)p >= start && (char*)p < end)
        { DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Copy update id='%s' %p\n", ip->id, p));
          *q = (char*)p + offset;
        }
      }
    }
}

/******************************************************************************/
static void
soap_add_copies(struct soap *soap, char *start, char *end, long offset)
{ int i;
  register struct soap_ilist *ip;
  register char *p;
  for (i = 0; i < SOAP_IDHASH; i++)
    for (ip = soap->iht[i]; ip; ip = ip->next)
    { for (p = (char*)ip->copy; p; p = *(char**)p)
      { if (p >= start && p < end)
        { DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Copy added id='%s' %p to %p (prev in chain = %p)\n", ip->id, p, p+offset, ip->copy));
          *(void**)(p + offset) = ip->copy;
          ip->copy = (void*)(p + offset);
        }
      }
    }
}

/******************************************************************************/
SOAP_FMAC1
void
SOAP_FMAC2
soap_resolve_ptr(struct soap_ilist *ip)
{ register void *p, **q, *r;
  q = (void**)ip->link;
  ip->link = NULL;
  r = ip->ptr;
  while (q)
  { p = *q;
    *q = r;
    q = (void**)p;
  }
}

/******************************************************************************/
SOAP_FMAC1
size_t
SOAP_FMAC2
soap_size_block(struct soap *soap, size_t n)
{ if (soap->blist->ptr)
  { soap->blist->size -= *(size_t*)(soap->blist->ptr + sizeof(char*)) - n;
    *(size_t*)(soap->blist->ptr + sizeof(char*)) = n;
  }
  return soap->blist->size;
}

/******************************************************************************/
SOAP_FMAC1
char*
SOAP_FMAC2
soap_first_block(struct soap *soap)
{ char *p, *q, *r;
  if (!(p = soap->blist->ptr))
    return NULL;
  DBGLOG(TEST, SOAP_MESSAGE(fdebug, "First block\n"));
  r = NULL;
  do
  { q = *((char**)p);
    *((char**)p) = r;
    r = p;
    p = q;
  } while (p);
  soap->blist->ptr = r;
  return r + sizeof(char*) + sizeof(size_t);
}

/******************************************************************************/
SOAP_FMAC1
char*
SOAP_FMAC2
soap_next_block(struct soap *soap)
{ char *p;
  if ((p = soap->blist->ptr))
  { DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Next block\n"));
    soap->blist->ptr = *((char**)p);
    free(p);
    if (soap->blist->ptr)
      return soap->blist->ptr + sizeof(char*) + sizeof(size_t);
  }
  return NULL;
}

/******************************************************************************/
SOAP_FMAC1
size_t
SOAP_FMAC2
soap_block_size(struct soap *soap)
{ return *((size_t*)(soap->blist->ptr + sizeof(char*)));
}

/******************************************************************************/
SOAP_FMAC1
void
SOAP_FMAC2
soap_end_block(struct soap *soap)
{ struct soap_blist *bp;
  char *p, *q;
  if ((bp = soap->blist))
  { DBGLOG(TEST, SOAP_MESSAGE(fdebug, "End of block sequence, free all remaining blocks\n"));
    for (p = bp->ptr; p; p = q)
    { q = *((char**)p);
      free(p);
    }
    soap->blist = bp->next;
    free(bp);
  }
  DBGLOG(TEST, if (soap->blist) SOAP_MESSAGE(fdebug, "Restore previous block sequence\n"));
}

/******************************************************************************/
SOAP_FMAC1
char*
SOAP_FMAC2
soap_save_block(struct soap *soap, char *p)
{ size_t n;
  char *q, *s;
  DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Save all blocks in contiguous memory space of %u bytes (%p->%p)\n", (unsigned int)soap->blist->size, soap->blist->ptr, p));
  if (soap->blist->size)
  { if (!p)
      p = (char*)soap_malloc(soap, soap->blist->size);
    if (p)
      for (s = p, q = soap_first_block(soap); q; q = soap_next_block(soap))
      { n = soap_block_size(soap);
        soap_update_ptrs(soap, q, q + n, s - q); /* s and q may or may not be related */
        DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Copy %u bytes from %p to %p\n", (unsigned int)n, q, s));
        memcpy(s, q, n);
        s += n;
      } 
    else
      soap->error = SOAP_EOM;
  }
  soap_end_block(soap);
  return p;
}

/******************************************************************************/
SOAP_FMAC1
char*
SOAP_FMAC2
soap_store_block(struct soap *soap, char *p)
{ p = soap_save_block(soap, p);
  if (!soap->blist)
  { /* get "delayed" backward pointers resolved NOW, instead of later */
    struct soap_ilist *ip;
    int i;
    for (i = 0; i < SOAP_IDHASH; i++)
      for (ip = soap->iht[i]; ip; ip = ip->next)
        if (ip->ptr)
        { DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Resolve link chain to point to %p\n", ip->ptr));
          soap_resolve_ptr(ip);
        }
  }
  return p;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_push_namespace(struct soap *soap, const char *id, const char *ns)
{ int i;
  struct soap_nlist *np;
  if (!soap->local_namespaces)
  { DBGLOG(TEST, SOAP_MESSAGE(fdebug, "No namespace table\n"));
    return SOAP_NAMESPACE;
  }
  np = (struct soap_nlist*)malloc(sizeof(struct soap_nlist) + strlen(id));
  if (!np)
    return soap->error = SOAP_EOM;
  np->next = soap->nlist;
  soap->nlist = np;
  strcpy(np->id, id);
  np->level = soap->level;
  np->index = -1;
  np->ns = NULL;
  DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Push namespace binding (level=%u) '%s' '%s'\n", soap->level, id, ns));
  for (i = 0; soap->local_namespaces[i].id; i++)
  { if (soap->local_namespaces[i].ns)
      if (!soap_tag_cmp(ns, soap->local_namespaces[i].ns))
        break;
    if (soap->local_namespaces[i].in)
      if (!soap_tag_cmp(ns, soap->local_namespaces[i].in))
      { if (soap->local_namespaces[i].out)
          free(soap->local_namespaces[i].out);
        if ((soap->local_namespaces[i].out = (char*)malloc(strlen(ns) + 1)))
          strcpy(soap->local_namespaces[i].out, ns);
        if (i == 0)
        { if (!strcmp(ns, soap_env1))
          { soap->version = 1; /* and make sure we use SOAP 1.1 encoding */
            if (soap->local_namespaces[1].out)
              free(soap->local_namespaces[1].out);
            if ((soap->local_namespaces[1].out = (char*)malloc(sizeof(soap_enc1))))
              strcpy(soap->local_namespaces[1].out, soap_enc1);
          }
          else if (!strcmp(ns, soap_env2))
          { soap->version = 2; /* and make sure we use the SOAP 1.2 encoding */
            if (soap->local_namespaces[1].out)
              free(soap->local_namespaces[1].out);
            if ((soap->local_namespaces[1].out = (char*)malloc(sizeof(soap_enc2))))
              strcpy(soap->local_namespaces[1].out, soap_enc2);
          }
        }
        break;
      }
  }
  if (soap->local_namespaces[i].id)
  { DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Push OK ('%s' matches '%s' in namespace table)\n", id, soap->local_namespaces[i].id));
    np->index = i;
  }
  else
  { DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Push NOT OK: no match found for '%s' in namespace mapping table\n", ns));
    np->ns = (char*)malloc(strlen(ns) + 1);
    strcpy(np->ns, ns);
  }
  return SOAP_OK;
}

/******************************************************************************/
SOAP_FMAC1
void
SOAP_FMAC2
soap_pop_namespace(struct soap *soap)
{ struct soap_nlist *np;
  while (soap->nlist && soap->nlist->level >= soap->level)
  { np = soap->nlist->next;
    DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Popped namespace binding (level=%u) '%s'\n", soap->level, soap->nlist->id));
    if (soap->nlist->ns)
      free(soap->nlist->ns);
    free(soap->nlist);
    soap->nlist = np;
  }
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_match_namespace(struct soap *soap, const char *id1, const char *id2, int n1, int n2) 
{ struct soap_nlist *np = soap->nlist;
  while (np && (strncmp(np->id, id1, n1) || np->id[n1]))
    np = np->next;
  if (np)
  { if (np->index < 0 || (np->index >= 0 && soap->local_namespaces[np->index].id && (strncmp(soap->local_namespaces[np->index].id, id2, n2) || soap->local_namespaces[np->index].id[n2])))
      return SOAP_NAMESPACE;
    return SOAP_OK;
  }
  return SOAP_SYNTAX_ERROR; 
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_match_tag(struct soap *soap, const char *tag1, const char *tag2)
{ const char *s, *t;
  if (!tag1 || !tag2)
    return SOAP_OK;
  s = strchr(tag1, ':');
  t = strchr(tag2, ':');
  if (s)
  { if (t)
    { if (soap_tag_cmp(s+1, t+1) || soap_match_namespace(soap, tag1, tag2, s-tag1, t-tag2))
        return SOAP_TAG_MISMATCH;
      DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Tags and namespaces match: '%s' '%s'\n", tag1, tag2));
      return SOAP_OK;
    } 
    t = tag2;
    s++;
  }
  else if (t)
  { s = tag1;
    t++;
  }
  else
  { s = tag1;
    t = tag2;
  }
  if (soap_tag_cmp(s, t))
    return SOAP_TAG_MISMATCH;
  DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Tags match: '%s' '%s'\n", tag1, tag2));
  return SOAP_OK;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_match_array(struct soap *soap, const char *type)
{ if (*soap->type && *soap->arrayType)
    if (soap_match_tag(soap, soap->arrayType, type)
     && soap_match_tag(soap, soap->arrayType, "xsd:anyType")
     && soap_match_tag(soap, soap->arrayType, "xsd:ur-type")
    )
    { DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Array type mismatch: '%s' '%s'\n", soap->arrayType, type));
      return SOAP_TAG_MISMATCH;
    }
  return SOAP_OK;
}

/******************************************************************************/

#ifdef WITH_OPENSSL
/******************************************************************************/
static int
ssl_init()
{ static int done = 0;
  if (done)
    return 0;
  done = 1;
  SSL_library_init();
  SSL_load_error_strings();
  soap_ssl_ctx = SSL_CTX_new(SSLv23_method());
  return !soap_ssl_ctx;
}

/******************************************************************************/
static const char *
ssl_error(struct soap *soap, int ret)
{ return ERR_error_string(SSL_get_error(soap->ssl, ret), soap->msgbuf);
}

/******************************************************************************/
static int
fpassword(char *buf, int num, int rwflag, void *userdata)
{ if (num < (int)strlen(soap_ssl_pw)+1)
    return 0;
  strcpy(buf, soap_ssl_pw);
  return strlen(soap_ssl_pw);
}

/******************************************************************************/
static int
ssl_auth_init(struct soap *soap)
{ ssl_init();
  if (soap->keyfile)
    if (!(SSL_CTX_use_certificate_chain_file(soap_ssl_ctx, soap->keyfile)))
      return soap_set_receiver_error(soap, "SSL error", "Can't read certificate file", SOAP_SSL_ERROR);
  if (soap->password && soap->keyfile)
  { soap_ssl_pw = soap->password;
    SSL_CTX_set_default_passwd_cb(soap_ssl_ctx, fpassword);
    if (!(SSL_CTX_use_PrivateKey_file(soap_ssl_ctx, soap->keyfile, SSL_FILETYPE_PEM)))
      return soap_set_receiver_error(soap, "SSL error", "Can't read key file", SOAP_SSL_ERROR);
  }
  if (soap->cafile)
    if (!(SSL_CTX_load_verify_locations(soap_ssl_ctx, soap->cafile, 0)))
      return soap_set_receiver_error(soap, "SSL error", "Can't read CA list", SOAP_SSL_ERROR);
  if (soap->rsa)
  { RSA *rsa = RSA_generate_key(512, RSA_F4, NULL, NULL);
    if (!SSL_CTX_set_tmp_rsa(soap_ssl_ctx, rsa))
      return soap_set_receiver_error(soap, "SSL error", "Can't set RSA key", SOAP_SSL_ERROR);
    RSA_free(rsa);
  }
  else if (soap->dhfile)
  { DH *dh = 0;
    BIO *bio;
    bio = BIO_new_file(soap->dhfile, "r");
    if (!bio)
      return soap_set_receiver_error(soap, "SSL error", "Can't read DH file", SOAP_SSL_ERROR);
    dh = PEM_read_bio_DHparams(bio, NULL, NULL, NULL);
    BIO_free(bio);
    if (SSL_CTX_set_tmp_dh(soap_ssl_ctx, dh) < 0)
      return soap_set_receiver_error(soap, "SSL error", "Can't set DH parameters", SOAP_SSL_ERROR);
    DH_free(dh);
  }
#if (OPENSSL_VERSION_NUMBER < 0x00905100L)
  SSL_CTX_set_verify_depth(soap_ssl_ctx, 1); 
#endif  
  if (soap->randfile)
  { if (!RAND_load_file(soap->randfile, -1))
      return soap_set_receiver_error(soap, "SSL error", "Can't load randomness", SOAP_SSL_ERROR);
  }
  else
  { RAND_seed(soap->buf, sizeof(soap->buf));
    while (!RAND_status())
    { int r;
#ifdef HAVE_RAND_R
      unsigned int s;
      r = rand_r(&s);
#else
      r = rand();
#endif
      RAND_seed(&r, sizeof(int));
    }
  }
  return SOAP_OK;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_ssl_accept(struct soap *soap)
{ int r;
  soap->error = ssl_auth_init(soap);
  if (soap->error)
    return soap->error;
  if (soap->socket < 0)
    return soap_set_receiver_error(soap, "SSL error", "No socket in soap_ssl_accept()", SOAP_SSL_ERROR);
  soap->ssl = SSL_new(soap_ssl_ctx);
  if (!soap->ssl)
    return soap_set_receiver_error(soap, "SSL error", "SSL_new() failed in soap_ssl_accept()", SOAP_SSL_ERROR);
  soap->imode |= SOAP_ENC_SSL;
  soap->omode |= SOAP_ENC_SSL;
  soap->bio = BIO_new_socket(soap->socket, BIO_NOCLOSE);
  SSL_set_bio(soap->ssl, soap->bio, soap->bio);
  r = SSL_accept(soap->ssl);
  if (r <= 0)
    return soap_set_receiver_error(soap, ssl_error(soap, r), "SSL_accept() failed in soap_ssl_accept()", SOAP_SSL_ERROR);
  return SOAP_OK;
}

/******************************************************************************/
#endif

/******************************************************************************/
static int
tcp_init(struct soap *soap)
{ soap->errmode = 1;
#ifdef WIN32
  if (tcp_done)
    return 0;
  tcp_done = 1;
  { WSADATA w;
    if (WSAStartup(MAKEWORD(1, 1), &w))
    { tcp_done = 0;
      return -1;
    }
  }
#endif
  return 0;
}

/******************************************************************************/
SOAP_FMAC1
void
SOAP_FMAC2
soap_done(struct soap *soap)
{ int i;
  soap_free(soap);
  while (soap->clist)
  { struct soap_clist *p = soap->clist->next;
    free(soap->clist);
    soap->clist = p;
  }
  soap->keep_alive = 0; /* to force close the socket */
  soap_closesock(soap);
#ifdef WITH_COOKIES
  soap_free_cookies(soap);
#endif
  while (soap->plugins)
  { struct soap_plugin *p = soap->plugins->next;
    if (!soap->copy)
    { DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Removing plugin '%s'\n", soap->plugins->id));
      soap->plugins->fdelete(soap, soap->plugins);
    }
    free(soap->plugins);
    soap->plugins = p;
  }
  soap->fpost = http_post;
  soap->fposthdr = http_post_header;
  soap->fresponse = http_response;
  soap->fparse = http_parse;
  soap->fparsehdr = http_parse_header;
  soap->faccept = tcp_accept;
#ifndef MAC_CARBON
  soap->fopen = tcp_connect;
  soap->fclose = tcp_disconnect;
  soap->fsend = fsend;
  soap->frecv = frecv;
#endif
  soap->fignore = fignore;
  soap->fplugin = fplugin;
  if (!soap->copy && soap->master >= 0)
  { closesocket(soap->master);
    soap->master = -1;
  }
  for (i = 0; i < SOAP_MAXLOGS; i++)
  { soap_close_logfile(soap, i);
    if (!soap->copy && soap->logfile[i])
    { free((void*)soap->logfile[i]);
      soap->logfile[i] = NULL;
    }
  }
}

/******************************************************************************/
SOAP_FMAC1
void
SOAP_FMAC2
soap_cleanup(struct soap *soap)
{ soap_done(soap);
#ifdef WIN32
  if (!tcp_done)
    return;
  tcp_done = 0;
  WSACleanup();
#endif
}

/******************************************************************************/
static const char*
tcp_error(struct soap *soap)
{ const char *msg = NULL;
  switch (soap->errmode)
  { case 0:
      msg = soap_strerror(soap, soap->errnum);
      break;
    case 1:
      msg = "WSAStartup failed";
      break;
    case 2:
    { struct code_map *map = h_errno_codes;
      while (map->code && map->code != soap->errnum)
        map++;
      if (map->code)
        msg = map->string;
      else
      { sprintf(soap->msgbuf, "Unknown TCP error code %d\n", soap->errnum);
        msg = soap->msgbuf;
      }
    }
  }
  return msg;
}

/******************************************************************************/

#ifndef MAC_CARBON
/******************************************************************************/
static int
soap_gethost(struct soap *soap, const char *addr, struct in_addr *inaddr)
{ unsigned long iadd;
  struct hostent hostent, *host = &hostent;
  iadd = inet_addr(addr);
  if ((int)iadd != -1)
  { memcpy(inaddr, &iadd, sizeof(iadd));
    return 0;
  }
#if defined(__GLIBC__)
  if (gethostbyname_r(addr, &hostent, soap->buf, SOAP_BUFLEN, &host, &soap->errnum) < 0)
    host = NULL;
#elif defined(HAVE_GETHOSTBYNAME_R)
  host = gethostbyname_r(addr, &hostent, soap->buf, SOAP_BUFLEN, &soap->errnum);
#else
  if (!(host = gethostbyname(addr)))
    soap->errnum = h_errno;
#endif
  if (!host)
  { DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Host name not found\n"));
    return -1;
  }
  memcpy(inaddr, host->h_addr, host->h_length);
  return 0;
}
/******************************************************************************/
#endif

/******************************************************************************/
static int
tcp_connect(struct soap *soap, const char *endpoint, const char *host, int port)
{ struct sockaddr_in sockaddr;
  int len = SOAP_BUFLEN;
  int set = 1;
  if (tcp_init(soap))
  { soap_set_sender_error(soap, tcp_error(soap), "TCP initialization failed in tcp_connect()", SOAP_TCP_ERROR);
    return -1;
  }
  if (soap->socket >= 0)
    closesocket(soap->socket);
  soap->errmode = 0;
  if ((soap->socket = socket(AF_INET, SOCK_STREAM, 0)) < 0)
  { soap->errnum = soap_socket_errno;
    soap_set_sender_error(soap, tcp_error(soap), "TCP socket failed in tcp_connect()", SOAP_TCP_ERROR);
    return -1;
  }
  if (soap->connect_flags && setsockopt(soap->socket, SOL_SOCKET, soap->connect_flags, (char*)&set, sizeof(int)))
  { soap->errnum = soap_socket_errno;
    soap_set_sender_error(soap, tcp_error(soap), "TCP setsockopt failed in tcp_connect()", SOAP_TCP_ERROR);
    return -1;
  }
  if (soap->keep_alive && setsockopt(soap->socket, SOL_SOCKET, SO_KEEPALIVE, (char*)&set, sizeof(int)))
  { soap->errnum = soap_socket_errno;
    soap_set_sender_error(soap, tcp_error(soap), "TCP setsockopt SO_KEEPALIVE failed in tcp_connect()", SOAP_TCP_ERROR);
    return -1;
  }
#ifndef UNDER_CE
  if (setsockopt(soap->socket, SOL_SOCKET, SO_SNDBUF, (char*)&len, sizeof(int)))
  { soap->errnum = soap_socket_errno;
    soap_set_sender_error(soap, tcp_error(soap), "TCP setsockopt SO_SNDBUF failed in tcp_connect()", SOAP_TCP_ERROR);
    return -1;
  }
  if (setsockopt(soap->socket, SOL_SOCKET, SO_RCVBUF, (char*)&len, sizeof(int)))
  { soap->errnum = soap_socket_errno;
    soap_set_sender_error(soap, tcp_error(soap), "TCP setsockopt SO_RCVBUF failed in tcp_connect()", SOAP_TCP_ERROR);
    return -1;
  }
#endif
#ifdef TCP_NODELAY
  if (setsockopt(soap->socket, IPPROTO_TCP, TCP_NODELAY, (char*)&set, sizeof(int)))
  { soap->errnum = soap_socket_errno;
    soap_set_sender_error(soap, tcp_error(soap), "TCP setsockopt TCP_NODELAY failed in tcp_connect()", SOAP_TCP_ERROR);
    return -1;
  }
#endif
  memset(&sockaddr, 0, sizeof(sockaddr));
  sockaddr.sin_family = AF_INET;
  DBGLOG(TEST,SOAP_MESSAGE(fdebug, "Open socket %d to host='%s'\n", soap->socket, host));
  soap->errmode = 2;
  if (soap->proxy_host)
  { if (soap_gethost(soap, soap->proxy_host, &sockaddr.sin_addr))
    { soap_set_sender_error(soap, tcp_error(soap), "TCP get proxy host by name failed in tcp_connect()", SOAP_TCP_ERROR);
      return -1;
    }
    sockaddr.sin_port = htons((short)soap->proxy_port);
  }
  else
  { if (soap_gethost(soap, host, &sockaddr.sin_addr))
    { soap_set_sender_error(soap, tcp_error(soap), "TCP get host by name failed in tcp_connect()", SOAP_TCP_ERROR);
      return -1;
    }
    sockaddr.sin_port = htons((short)port);
  }
  soap->errmode = 0;
  if (soap->connect_timeout)
#ifdef WIN32
  { u_long nonblocking = 1;
    ioctlsocket(soap->socket, FIONBIO, &nonblocking);
  }
#else
    fcntl(soap->socket, F_SETFL, fcntl(soap->socket, F_GETFL)|O_NONBLOCK);
#endif
  for (;;)
  { if (connect(soap->socket, (struct sockaddr*)&sockaddr, sizeof(sockaddr)))
    { if (soap->connect_timeout && (soap_socket_errno == SOAP_EINPROGRESS || soap_socket_errno == SOAP_EWOULDBLOCK))
      { struct timeval timeout;
#if defined(SOCKLEN_T)
        SOCKLEN_T n = sizeof(struct sockaddr_in);
#elif defined(__socklen_t_defined) || defined(_SOCKLEN_T) || defined(CYGWIN)
        socklen_t n = sizeof(struct sockaddr_in);
#elif defined(WIN32) || defined(__APPLE__) || defined(HP_UX) || defined(SUN_OS) || defined(OPENSERVER) || defined(TRU64)
        int n = sizeof(struct sockaddr_in);
#else
        size_t n = sizeof(struct sockaddr_in);
#endif
        fd_set fd;
        if (soap->connect_timeout > 0)
        { timeout.tv_sec = soap->connect_timeout;
          timeout.tv_usec = 0;
        }
        else
        { timeout.tv_sec = -soap->connect_timeout/1000000;
          timeout.tv_usec = -soap->connect_timeout%1000000;
        }
        FD_ZERO(&fd);
        FD_SET(soap->socket, &fd);
        for (;;)
        { int r = select(soap->socket + 1, NULL, &fd, NULL, &timeout);
          if (r > 0)
	    break;
          if (!r)
          { soap->errnum = 0;
            DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Connect timeout\n"));
            soap_closesock(soap);
            soap_set_sender_error(soap, "Timeout", "TCP connect failed in tcp_connect()", SOAP_TCP_ERROR);
            return -1;
          }
          if (soap_socket_errno != SOAP_EINTR)
          { soap->errnum = soap_socket_errno;
            DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Could not connect to host\n"));
            soap_closesock(soap);
            soap_set_sender_error(soap, tcp_error(soap), "TCP connect failed in tcp_connect()", SOAP_TCP_ERROR);
            return -1;
          }
        }
        if (!getsockopt(soap->socket, SOL_SOCKET, SO_ERROR, (char*)&soap->errnum, &n) && !soap->errnum)
          break;
        DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Could not connect to host\n"));
        soap_closesock(soap);
        soap_set_sender_error(soap, tcp_error(soap), "TCP connect failed in tcp_connect()", SOAP_TCP_ERROR);
        return -1;
      }
      else if (soap_socket_errno != SOAP_EINTR)
      { soap->errnum = soap_socket_errno;
        DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Could not connect to host\n"));
        soap_closesock(soap);
        soap_set_sender_error(soap, tcp_error(soap), "TCP connect failed in tcp_connect()", SOAP_TCP_ERROR);
        return -1;
      }
    }  
    else
      break;
  }
  if (soap->connect_timeout)
#ifdef WIN32
  { u_long blocking = 0;
    ioctlsocket(soap->socket, FIONBIO, &blocking);
  }
#else
    fcntl(soap->socket, F_SETFL, fcntl(soap->socket, F_GETFL)&~O_NONBLOCK);
#endif
#ifdef WITH_OPENSSL
  soap->imode &= ~SOAP_ENC_SSL;
  soap->omode &= ~SOAP_ENC_SSL;
  if (!strncmp(endpoint, "https:", 6))
  { int r;
    size_t n;
    if (soap->proxy_host)
    { soap_begin_send(soap);
      sprintf(soap->tmpbuf, "CONNECT %s:%d HTTP/%s", host, port, soap->http_version);
      DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Connecting to proxy server\n"));
      if ((soap->error = soap->fposthdr(soap, soap->tmpbuf, NULL))
       || (soap->error = soap->fposthdr(soap, NULL, NULL)))
        return -1;
      if (soap_end_send(soap))
        return -1;
      n = soap->count; /* save the content length */
      r = soap->mode; /* make sure we only parse HTTP */
      soap->mode &= 0xFF00;
      if (soap_begin_recv(soap))
        return -1;
      soap->mode = (short)r;
      soap->count = n;
      soap_begin_send(soap);
    }
    if (ssl_auth_init(soap))
    { soap_set_sender_error(soap, "SSL error", "SSL initialization failed in tcp_connect()", SOAP_SSL_ERROR);
      return -1;
    }
    soap->ssl = SSL_new(soap_ssl_ctx);
    if (!soap->ssl)
    { soap->error = SOAP_SSL_ERROR;
      return -1;
    }
    soap->imode |= SOAP_ENC_SSL;
    soap->omode |= SOAP_ENC_SSL;
    soap->bio = BIO_new_socket(soap->socket, BIO_NOCLOSE);
    SSL_set_bio(soap->ssl, soap->bio, soap->bio);
    if ((r = SSL_connect(soap->ssl)) <= 0)
    { soap_set_sender_error(soap, ssl_error(soap, r), "SSL connect failed in tcp_connect()", SOAP_SSL_ERROR);
      return -1;
    }
    if (soap->require_server_auth)
    { X509 *peer;
      if (SSL_get_verify_result(soap->ssl) != X509_V_OK)
      { soap_set_sender_error(soap, "SSL error", "SSL certificate cannot be verified in tcp_connect()", SOAP_SSL_ERROR);
        return -1;
      }
      peer = SSL_get_peer_certificate(soap->ssl);
      X509_NAME_get_text_by_NID(X509_get_subject_name(peer), NID_commonName, soap->msgbuf, sizeof(soap->msgbuf));
      if (soap_tag_cmp(soap->msgbuf, host))
      { soap_set_sender_error(soap, "SSL error", "SSL certificate host name mismatch in tcp_connect()", SOAP_SSL_ERROR);
        return -1;
      }
    }
  }
#endif
  return soap->socket;
}

/******************************************************************************/
static int
tcp_accept(struct soap *soap, SOAP_SOCKET s, struct sockaddr *a, int *n)
{
#if defined(SOCKLEN_T)
  return accept(s, a, (SOCKLEN_T*)n);
#elif defined(__socklen_t_defined) || defined(_SOCKLEN_T) || defined(CYGWIN)
  return accept(s, a, (socklen_t*)n);
#elif defined(WIN32) || defined(__APPLE__) || defined(HP_UX) || defined(SUN_OS) || defined(OPENSERVER) || defined(TRU64)
  return accept(s, a, n);
#else
  return accept(s, a, (size_t*)n);
#endif
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_bind(struct soap *soap, const char *host, int port, int backlog)
{ struct sockaddr_in sockaddr;
  int len = SOAP_BUFLEN;
  int set = 1;
  if (soap->master >= 0)
  { closesocket(soap->master);
    soap->master = -1;
  }
  soap->socket = -1;
  soap->errmode = 1;
  if (tcp_init(soap))
  { soap_set_receiver_error(soap, tcp_error(soap), "TCP init failed in soap_bind()", SOAP_TCP_ERROR);
    return -1;
  }
  soap->errmode = 0;
  if ((soap->master = socket(AF_INET, SOCK_STREAM, 0)) < 0)
  { soap->errnum = soap_socket_errno;
    soap_set_receiver_error(soap, tcp_error(soap), "TCP socket failed in soap_bind()", SOAP_TCP_ERROR);
    return -1;
  }
  if (soap->bind_flags && setsockopt(soap->master, SOL_SOCKET, soap->bind_flags, (char*)&set, sizeof(int)))
  { soap->errnum = soap_socket_errno;
    soap_set_receiver_error(soap, tcp_error(soap), "TCP setsockopt failed in soap_bind()", SOAP_TCP_ERROR);
    return -1;
  }
  if (soap->keep_alive && setsockopt(soap->master, SOL_SOCKET, SO_KEEPALIVE, (char*)&set, sizeof(int)))
  { soap->errnum = soap_socket_errno;
    soap_set_receiver_error(soap, tcp_error(soap), "TCP setsockopt SO_KEEPALIVE failed in soap_bind()", SOAP_TCP_ERROR);
    return -1;
  }
#ifndef UNDER_CE
  if (setsockopt(soap->master, SOL_SOCKET, SO_SNDBUF, (char*)&len, sizeof(int)))
  { soap->errnum = soap_socket_errno;
    soap_set_receiver_error(soap, tcp_error(soap), "TCP setsockopt SO_SNDBUF failed in soap_bind()", SOAP_TCP_ERROR);
    return -1;
  }
  if (setsockopt(soap->master, SOL_SOCKET, SO_RCVBUF, (char*)&len, sizeof(int)))
  { soap->errnum = soap_socket_errno;
    soap_set_receiver_error(soap, tcp_error(soap), "TCP setsockopt SO_RCVBUF failed in soap_bind()", SOAP_TCP_ERROR);
    return -1;
  }
#endif
#ifdef TCP_NODELAY
  if (setsockopt(soap->master, IPPROTO_TCP, TCP_NODELAY, (char*)&set, sizeof(int)))
  { soap->errnum = soap_socket_errno;
    soap_set_receiver_error(soap, tcp_error(soap), "TCP setsockopt TCP_NODELAY failed in soap_bind()", SOAP_TCP_ERROR);
    return -1;
  }
#endif
  memset(&sockaddr, 0, sizeof(sockaddr));
  sockaddr.sin_family = AF_INET;
  soap->errmode = 2;
  if (host)
  { if (soap_gethost(soap, host, &sockaddr.sin_addr))
    { soap_set_receiver_error(soap, tcp_error(soap), "TCP get host by name failed in soap_bind()", SOAP_TCP_ERROR);
      return -1;
    }
  }
  else
    sockaddr.sin_addr.s_addr = htonl(INADDR_ANY);
  sockaddr.sin_port = htons((short)port);
  soap->errmode = 0;
  if (bind(soap->master, (struct sockaddr*)&sockaddr, sizeof(sockaddr)) || listen(soap->master, backlog))
  { soap->errnum = soap_socket_errno;
    DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Could not bind to host\n"));
    soap_closesock(soap);
    soap_set_receiver_error(soap, tcp_error(soap), "TCP bind failed in soap_bind()", SOAP_TCP_ERROR);
    return -1;
  }  
  return soap->master;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_poll(struct soap *soap)
{ struct timeval timeout;
  fd_set fd;
  int r;
  timeout.tv_sec = 0;
  timeout.tv_usec = 0;
  FD_ZERO(&fd);
  if (soap->socket >= 0)
  { FD_SET(soap->socket, &fd);
    r = select(soap->socket + 1, &fd, &fd, &fd, &timeout);
  }
  else if (soap->master >= 0)
  { FD_SET(soap->master, &fd);
    r = select(soap->master + 1, &fd, &fd, &fd, &timeout);
  }
  else
  { FD_SET(soap->sendfd, &fd);
    FD_SET(soap->recvfd, &fd);
    r = select((soap->sendfd > soap->recvfd ? soap->sendfd : soap->recvfd) + 1, &fd, &fd, &fd, &timeout);
  }
  if (r > 0)
    return SOAP_OK;
  if (r < 0 && (soap->master >= 0 || soap->socket >= 0) && soap_socket_errno != SOAP_EINTR)
  { soap->errnum = soap_socket_errno;
    soap_set_receiver_error(soap, tcp_error(soap), "select failed in soap_poll()", SOAP_TCP_ERROR);
    return soap->error = SOAP_TCP_ERROR;
  }
  else
    soap->errnum = soap_errno;
  return SOAP_EOF;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_accept(struct soap *soap)
{ struct sockaddr_in sockaddr;
  int len = SOAP_BUFLEN;
  int set = 1;
  int n = sizeof(struct sockaddr_in);
  memset(&sockaddr, 0, sizeof(sockaddr));
  soap->socket = -1;
  soap->errmode = 0;
  if (soap->master >= 0)
  { for (;;)
    { if (soap->accept_timeout)
      { struct timeval timeout;
        fd_set fd;
        if (soap->accept_timeout > 0)
        { timeout.tv_sec = soap->accept_timeout;
          timeout.tv_usec = 0;
        }
        else
        { timeout.tv_sec = -soap->accept_timeout/1000000;
          timeout.tv_usec = -soap->accept_timeout%1000000;
        }
        FD_ZERO(&fd);
        FD_SET(soap->master, &fd);
        for (;;)
        { int r = select(soap->master + 1, &fd, &fd, NULL, &timeout);
          if (r > 0)
            break;
          if (!r)
          { soap->errnum = 0;
            soap_set_receiver_error(soap, "Timeout", "TCP accept failed in soap_accept()", SOAP_TCP_ERROR);
            return -1;
          }
          if (soap_socket_errno != SOAP_EINTR)
          { soap->errnum = soap_socket_errno;
            soap_closesock(soap);
            soap_set_sender_error(soap, tcp_error(soap), "TCP accept failed in soap_accept()", SOAP_TCP_ERROR);
            return -1;
          }
        }
#ifdef WIN32
#ifdef WITH_OPENSSL
	if (!soap->ssl) /* fixes win32+OpenSSL problem */
#endif
        { u_long nonblocking = 1;
          ioctlsocket(soap->master, FIONBIO, &nonblocking);
        }
#else
        fcntl(soap->master, F_SETFL, fcntl(soap->master, F_GETFL)|O_NONBLOCK);
#endif
      }
      if ((soap->socket = soap->faccept(soap, soap->master, (struct sockaddr*)&sockaddr, &n)) >= 0)
      { soap->ip = ntohl(sockaddr.sin_addr.s_addr);
        if (soap->accept_flags && setsockopt(soap->socket, SOL_SOCKET, soap->accept_flags, (char*)&set, sizeof(int)))
        { soap->errnum = soap_socket_errno;
          soap_set_receiver_error(soap, tcp_error(soap), "TCP setsockopt failed in soap_accept()", SOAP_TCP_ERROR);
          return -1;
        }
        soap->keep_alive = ((soap->imode & SOAP_IO_KEEPALIVE) != 0);
        if (soap->keep_alive && setsockopt(soap->socket, SOL_SOCKET, SO_KEEPALIVE, (char*)&set, sizeof(int)))
        { soap->errnum = soap_socket_errno;
          soap_set_receiver_error(soap, tcp_error(soap), "TCP setsockopt SO_KEEPALIVE failed in soap_accept()", SOAP_TCP_ERROR);
          return -1;
        }
#ifndef UNDER_CE
        if (setsockopt(soap->socket, SOL_SOCKET, SO_SNDBUF, (char*)&len, sizeof(int)))
        { soap->errnum = soap_socket_errno;
          soap_set_receiver_error(soap, tcp_error(soap), "TCP setsockopt SO_SNDBUF failed in soap_accept()", SOAP_TCP_ERROR);
          return -1;
        }
        if (setsockopt(soap->socket, SOL_SOCKET, SO_RCVBUF, (char*)&len, sizeof(int)))
        { soap->errnum = soap_socket_errno;
          soap_set_receiver_error(soap, tcp_error(soap), "TCP setsockopt SO_RCVBUF failed in soap_accept()", SOAP_TCP_ERROR);
          return -1;
        }
#endif
#ifdef TCP_NODELAY
        if (setsockopt(soap->socket, IPPROTO_TCP, TCP_NODELAY, (char*)&set, sizeof(int)))
        { soap->errnum = soap_socket_errno;
          soap_set_receiver_error(soap, tcp_error(soap), "TCP setsockopt TCP_NODELAY failed in soap_accept()", SOAP_TCP_ERROR);
          return -1;
        }
#endif
        if (soap->accept_timeout)
#ifdef WIN32
        { u_long blocking = 0;
          ioctlsocket(soap->master, FIONBIO, &blocking);
        }
#else
          fcntl(soap->master, F_SETFL, fcntl(soap->master, F_GETFL)&~O_NONBLOCK);
#endif
        return soap->socket;
      }
      else if (soap_socket_errno != SOAP_EINTR && soap_socket_errno != SOAP_EAGAIN)
      { soap->errnum = soap_socket_errno;
        soap_set_receiver_error(soap, tcp_error(soap), "TCP accept failed in soap_accept()", SOAP_TCP_ERROR);
        return -1;
      }
    }
  }
  else
  { soap_set_receiver_error(soap, "", "TCP no master socket in soap_accept()", SOAP_TCP_ERROR);
    return -1;
  }
}

/******************************************************************************/
static int
tcp_disconnect(struct soap *soap)
{
#ifdef WITH_OPENSSL
  if (soap->ssl)
  { int r = SSL_shutdown(soap->ssl);
    int s = 0;
    if (r != 1)
    { s = ERR_get_error();
      if (s)
      { if (soap->socket >= 0)
        { shutdown(soap->socket, 1);
          soap->socket = -1;
        }
        r = SSL_shutdown(soap->ssl);
      }
    }
    DBGLOG(TEST, if (s) SOAP_MESSAGE(fdebug, "Shutdown failed: %d\n", SSL_get_error(soap->ssl, r)));
    SSL_free(soap->ssl);
    soap->ssl = NULL;
    if (s)
      return SOAP_SSL_ERROR;
  }
#endif
  if (soap->socket >= 0)
  { DBGLOG(TEST,SOAP_MESSAGE(fdebug, "Closing socket %d\n", soap->socket));
    closesocket(soap->socket);
  }
  return SOAP_OK;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_closesock(struct soap *soap)
{
#ifndef MAC_CARBON
  if (!soap->keep_alive)
  { soap->error = soap->fclose(soap);
    soap->socket = -1;
  }
#endif
  return soap->error;
}

/******************************************************************************/
static int
hash_id(register const char *s)
{ register int h = 0;
  while (*s)
    h += *s++ & 0x1F;
  return h % SOAP_IDHASH;
}

/******************************************************************************/
static void
soap_init_pht(struct soap *soap)
{ int i;
  DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Initializing pointer hashtable\n"));
  for (i = 0; i < SOAP_PTRHASH; i++)
    soap->pht[i] = NULL;
}

/******************************************************************************/
SOAP_FMAC1
struct soap*
SOAP_FMAC2
soap_new()
{ struct soap *soap = (struct soap*)malloc(sizeof(struct soap));
  if (soap)
    soap_init(soap);
  return soap;
}

/******************************************************************************/
static void
soap_free_pht(struct soap *soap)
{ struct soap_plist *pp, *next;
  int i;
  for (i = 0; i < SOAP_PTRHASH; i++)
  { for (pp = soap->pht[i]; pp; pp = next)
    { next = pp->next;
      free(pp);
    }
    soap->pht[i] = NULL;
  }
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_pointer_lookup(struct soap *soap, const void *p, int type, struct soap_plist **ppp)
{ struct soap_plist *pp;
  *ppp = NULL;
  if (!p)
    return 0;
  for (pp = soap->pht[hash_ptr(p)]; pp; pp = pp->next)
    if (pp->ptr == p && pp->type == type)
    { *ppp = pp;
      DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Lookup location=%p type=%d id=%d\n", p, type, pp->id));
      return pp->id;
    }
  DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Lookup location=%p type=%d: not found\n", p, type));
  return 0;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_array_pointer_lookup(struct soap *soap, const void *p, const struct soap_array *a, int n, int type, struct soap_plist **ppp)
{ struct soap_plist *pp;
  *ppp = NULL;
  if (!p || !a->__ptr)
    return 0;
  for (pp = soap->pht[hash_ptr(a->__ptr)]; pp; pp = pp->next)
    if (pp->type == type && pp->array && pp->array->__ptr == a->__ptr && pp->array->__size == n)
    { *ppp = pp;
      DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Array lookup location=%p type=%d id=%d\n", a->__ptr, type, pp->id));
      return pp->id;
    }
  DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Array lookup location=%p type=%d: not found\n", a->__ptr, type));
  return 0;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_pointer_enter(struct soap *soap, const void *p, int type, struct soap_plist **ppp)
{ int h;
  struct soap_plist *pp;
  if (!p)
  { *ppp = NULL;
    return 0;
  }
  DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Enter location=%p type=%d id=%d\n", p, type, soap->idnum+1));
  *ppp = pp = (struct soap_plist*)malloc(sizeof(struct soap_plist));
  if (pp)
  { h = hash_ptr(p);
    pp->next = soap->pht[h];
    pp->type = type;
    if ((soap->mode & SOAP_XML_TREE) || soap->is_in_header)
    { pp->mark1 = 0;
      pp->mark2 = 0;
    }
    else
    { pp->mark1 = 1;
      pp->mark2 = 1;
    }
    pp->ptr = p;
    pp->array = NULL;
    pp->id = ++soap->idnum;
    soap->pht[h] = pp;
  }
  else
    return 0;
  return pp->id;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_array_pointer_enter(struct soap *soap, const void *p, const struct soap_array *a, int type, struct soap_plist **ppp)
{ int h;
  struct soap_plist *pp;
  *ppp = NULL;
  if (!p || !a->__ptr)
    return 0;
  DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Array enter location=%p type=%d id=%d\n", a->__ptr, type, soap->idnum+1));
  pp = (struct soap_plist*)malloc(sizeof(struct soap_plist));
  *ppp = pp;
  if (pp)
  { h = hash_ptr(a->__ptr);
    pp->next = soap->pht[h];
    pp->type = type;
    if ((soap->mode & SOAP_XML_TREE) || soap->is_in_header)
    { pp->mark1 = 0;
      pp->mark2 = 0;
    }
    else
    { pp->mark1 = 1;
      pp->mark2 = 1;
    }
    pp->ptr = p;
    pp->array = a;
    pp->id = ++soap->idnum;
    soap->pht[h] = pp;
  }
  else
    return 0;
  return pp->id;
}

/******************************************************************************/
SOAP_FMAC1
void
SOAP_FMAC2
soap_begin_count(struct soap *soap)
{ soap_clr_attr(soap);
  soap_set_local_namespaces(soap);
  if (soap->mode & SOAP_ENC_DIME)
    soap->mode = soap->omode | SOAP_IO_LENGTH | SOAP_ENC_DIME;
  else
  { soap->mode = soap->omode;
    if ((soap->mode & (SOAP_IO_STORE | SOAP_IO_CHUNK)) || (soap->mode & SOAP_ENC_XML))
      soap->mode &= ~SOAP_IO_LENGTH;
    else
      soap->mode |= SOAP_IO_LENGTH;
  }
  if ((soap->mode & SOAP_ENC_ZLIB) && (soap->mode & SOAP_IO) == SOAP_IO_FLUSH)
  { if (!(soap->mode & SOAP_ENC_DIME))
      soap->mode &= ~SOAP_IO_LENGTH;
    if (soap->mode & SOAP_ENC_XML)
      soap->mode |= SOAP_IO_BUFFER;
    else
      soap->mode |= SOAP_IO_STORE;
  }
  soap->count = 0;
  soap->ns = 0;
  soap->null = 0;
  soap->position = 0;
  soap->mustUnderstand = 0;
  soap->is_in_header = 0;
  soap->dime_count = 0; /* count # of attachments */
  soap->dime_size = 0; /* accumulate total size of attachments */
  DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Begin count phase\n"));
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_begin_send(struct soap *soap)
{ soap_clr_attr(soap);
  soap_set_local_namespaces(soap);
  soap->mode = (soap->omode & ~SOAP_IO_LENGTH) | (soap->mode & SOAP_ENC_DIME);
  if ((soap->mode & SOAP_ENC_ZLIB) && (soap->mode & SOAP_IO) == SOAP_IO_FLUSH)
  { if (soap->mode & SOAP_ENC_XML)
      soap->mode |= SOAP_IO_BUFFER;
    else
      soap->mode |= SOAP_IO_STORE;
  }
  if ((soap->mode & SOAP_IO) == SOAP_IO_STORE)
    soap_new_block(soap);
  else if ((soap->mode & SOAP_IO) == SOAP_IO_FLUSH && soap->socket >= 0)
    soap->mode |= SOAP_IO_BUFFER;
  if (!(soap->mode & SOAP_IO_KEEPALIVE))
    soap->keep_alive = 0;
#ifdef WIN32
#ifndef UNDER_CE
  if (soap->socket < 0) /* Set win32 stdout or soap->sendfd to BINARY, e.g. to support DIME */
#ifndef WITH_FASTCGI
#ifdef __BORLANDC__
    setmode(soap->sendfd, O_BINARY);
#else
    _setmode(soap->sendfd, _O_BINARY);
#endif
#endif
#endif
#endif
  soap->bufidx = 0;
  soap->buflen = 0;
  soap->chunksize = 0;
  soap->ns = 0;
  soap->null = 0;
  soap->position = 0;
  soap->mustUnderstand = 0;
  soap->is_in_header = 0;
  soap->idnum = 0;
#ifdef WITH_ZLIB
  soap->z_ratio_out = 1.0;
  if (soap->mode & SOAP_ENC_ZLIB)
  {
#ifdef WITH_GZIP
    memcpy(soap->z_buf, "\37\213\10\0\0\0\0\0\0\377", 10);
    soap->d_stream.next_out = (Byte*)soap->z_buf + 10;
    soap->d_stream.avail_out = SOAP_BUFLEN - 10;
    soap->z_crc = crc32(0L, NULL, 0);
    if (deflateInit2(&soap->d_stream, soap->z_level, Z_DEFLATED, -MAX_WBITS, 8, Z_DEFAULT_STRATEGY) != Z_OK)
#else
    soap->d_stream.next_out = (Byte*)soap->z_buf;
    soap->d_stream.avail_out = SOAP_BUFLEN;
    if (deflateInit(&soap->d_stream, soap->z_level) != Z_OK)
#endif
      return soap->error = SOAP_ZLIB_ERROR;
    DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Deflate initialized\n"));
  }
#endif
  DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Begin send phase (socket=%d mode=%hd count=%lu)\n", soap->socket, soap->mode, (unsigned long)soap->count));
  return SOAP_OK;
}

/******************************************************************************/
SOAP_FMAC1
void
SOAP_FMAC2
soap_embedded(struct soap *soap, const void *p, int t)
{ struct soap_plist *pp;
  DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Embedded %p type=%d\n", p, t));
  if (soap_pointer_lookup(soap, p, t, &pp))
  { pp->mark1 = 1;
    pp->mark2 = 1;
    DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Embedded %p type=%d set to %d\n", p, t, (int)pp->mark1));
  }
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_reference(struct soap *soap, const void *p, int t)
{ int i;
  struct soap_plist *pp;
  if (!p)
    return 1;
  i = soap_pointer_lookup(soap, p, t, &pp);
  if (i)
  { if (pp->mark1 == 0)
    { pp->mark1 = 2;
      pp->mark2 = 2;
    }
  }
  else
  { soap_pointer_enter(soap, p, t, &pp);
    pp->mark1 = 0;
    pp->mark2 = 0;
  }
  DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Reference %p type = %d (%d %d)\n", p, t, (int)pp->mark1, (int)pp->mark2));
  return pp->mark1;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_array_reference(struct soap *soap, const void *p, const struct soap_array *a, int n, int t)
{ int i;
  struct soap_plist *pp;
  if (!p)
    return 1;
  i = soap_array_pointer_lookup(soap, p, a, n, t, &pp);
  if (i)
  { if (pp->mark1 == 0)
    { pp->mark1 = 2;
      pp->mark2 = 2;
    }
  }
  else if (!soap_array_pointer_enter(soap, p, a, t, &pp))
    return 1;
  pp->mark1 = 0;
  pp->mark2 = 0;
  DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Array reference %p type = %d (%d %d)\n", p, t, (int)pp->mark1, (int)pp->mark2));
  return pp->mark1;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_embedded_id(struct soap *soap, int id, const void *p, int t)
{ struct soap_plist *pp;
  DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Embedded_id %p type=%d id=%d\n", p, t, id));
  if (soap->version == 1 && !(soap->mode & (SOAP_XML_TREE | SOAP_XML_GRAPH)) && !soap->is_in_header)
  { if (id < 0)
    { id = soap_pointer_lookup(soap, p, t, &pp);
      if (id > 0 && pp)
      { if (soap->mode & SOAP_IO_LENGTH)
          pp->mark1 = 2;
        else
          pp->mark2 = 2;
      }
      return -1;
    }
    return id;
  }
  if (id < 0)
    id = soap_pointer_lookup(soap, p, t, &pp);
  else
    soap_pointer_lookup(soap, p, t, &pp);
  if (id > 0 && pp)
  { if (soap->mode & SOAP_IO_LENGTH)
      pp->mark1 = 1;
    else
      pp->mark2 = 1;
  DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Embedded_id id=%d %p type=%d = (%d %d)\n", id, p, t, (int)pp->mark1, (int)pp->mark2));
  }
  return id;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_is_embedded(struct soap *soap, struct soap_plist *pp)
{ DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Is embedded? %d %d\n", (int)pp->mark1, (int)pp->mark2));
  if (soap->version == 1 && !(soap->mode & (SOAP_XML_TREE | SOAP_XML_GRAPH)) && !soap->is_in_header)
  { if (soap->mode & SOAP_IO_LENGTH)
      return pp->mark1 != 0;
    return pp->mark2 != 0;
  }
  if (soap->mode & SOAP_IO_LENGTH)
    return pp->mark1 == 1;
  return pp->mark2 == 1;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_is_single(struct soap *soap, struct soap_plist *pp)
{ if ((soap->mode & SOAP_XML_TREE) || soap->is_in_header)
    return 1;
  if (soap->mode & SOAP_IO_LENGTH)
    return pp->mark1 == 0;
  return pp->mark2 == 0;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_is_multi(struct soap *soap, struct soap_plist *pp)
{ if (soap->mode & SOAP_IO_LENGTH)
    return pp->mark1 == 2;
  return pp->mark2 == 2;
}

/******************************************************************************/
SOAP_FMAC1
void
SOAP_FMAC2
soap_set_embedded(struct soap *soap, struct soap_plist *pp)
{ if (soap->mode & SOAP_IO_LENGTH)
    pp->mark1 = 1;
  else
    pp->mark2 = 1;
}

/******************************************************************************/
SOAP_FMAC1
void
SOAP_FMAC2
soap_set_attached(struct soap *soap, struct soap_plist *pp, const char *id, const char *type, const char *options, size_t size)
{ if (soap->mode & SOAP_IO_LENGTH)
  { if (pp->mark1 != 3)
    { pp->mark1 = 3;
      soap->dime_count++; /* one more attachment found */
      soap->dime_size += 12; /* increase total size (DIME fields) */
      if (id)
        soap->dime_size += (strlen(id)+3)&-4;
      if (type)
        soap->dime_size += (strlen(type)+3)&-4;
      if (options)
        soap->dime_size += 4 + (((((unsigned char)options[2] << 8 | (unsigned char)options[3]))+3)&-4);
      soap->dime_size += (size+3)&-4;
    }
  }
  else if (pp->mark2 != 3)
    pp->mark2 = 3;
}

/******************************************************************************/
static void
soap_init_iht(struct soap *soap)
{ int i;
  DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Initializing ID hashtable\n"));
  for (i = 0; i < SOAP_IDHASH; i++)
    soap->iht[i] = NULL;
}

/******************************************************************************/
static void
soap_free_iht(struct soap *soap)
{ int i;
  struct soap_ilist *ip, *next;
  for (i = 0; i < SOAP_IDHASH; i++)
  { for (ip = soap->iht[i]; ip; ip = next)
    { next = ip->next;
      free(ip);
    }
    soap->iht[i] = NULL;
  }
}

/******************************************************************************/
SOAP_FMAC1
struct soap_ilist *
SOAP_FMAC2
soap_lookup(struct soap *soap, const char *id)
{ struct soap_ilist *ip;
  for (ip = soap->iht[hash_id(id)]; ip; ip = ip->next)
    if (!strcmp(ip->id, id))
      return ip;
  return NULL;
}

/******************************************************************************/
SOAP_FMAC1
struct soap_ilist *
SOAP_FMAC2
soap_enter(struct soap *soap, const char *id)
{ int h;
  struct soap_ilist *ip;
  ip = (struct soap_ilist*)malloc(sizeof(struct soap_ilist) + strlen(id));
  if (ip)
  { h = hash_id(id);
    strcpy(ip->id, id);
    ip->next = soap->iht[h];
    soap->iht[h] = ip;
    return ip;
  }
  return NULL;
}

/******************************************************************************/
SOAP_FMAC1
void*
SOAP_FMAC2
soap_malloc(struct soap *soap, size_t n)
{ char *p;
  if (!n)
    return NULL;
  if (!soap)
    return malloc(n);
  n += (-(int)n) & 7;
  if (!(p = (char*)malloc(n + 16)))
  { soap->error = SOAP_EOM;
    return NULL;
  }
  /* keep chain of alloced cells for later destruction */
  soap->alloced = 1;
  *(void**)(p + n) = soap->alist;
  *(size_t*)(p + n + 8) = n;
  soap->alist = p + n;
  DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Malloc %u bytes at location %p\n", (unsigned int)n, p));
  return p;
}

/******************************************************************************/
SOAP_FMAC1
void
SOAP_FMAC2
soap_dealloc(struct soap *soap, void *p)
{ if (!soap)
    return;
  if (p)
  { char **q;
    for (q = (char**)&soap->alist; *q; q = *(char***)q)
    { if (p == (void*)(*q - *(size_t*)(*q + 8)))
      { *q = **(char***)q;
        free(p);
        DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Freed data %p\n", p));
        return;
      }
    }
    DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Could not dealloc data %p: address not in list\n", p));
  }
  else
  { char *q;
    while (soap->alist)
    { q = (char*)soap->alist;
      soap->alist = *(void**)q;
      q -= *(size_t*)(q + 8);
      if (q == (char*)soap->fault)
        soap->fault = NULL; /* this was deallocated */
      else if (q == (char*)soap->header)
        soap->header = NULL; /* this was deallocated */
      free(q);
    }
    DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Dealloc all data done\n"));
  }
}

/******************************************************************************/
SOAP_FMAC1
void
SOAP_FMAC2
soap_unlink(struct soap *soap, void *p)
{ char **q;
  struct soap_clist **cp;
  if (!soap)
    return;
  for (q = (char**)&soap->alist; *q; q = *(char***)q)
  { if (p == (void*)(*q - *(size_t*)(*q + 8)))
    { *q = **(char***)q;
      DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Unlinked data %p\n", p));
      return;
    }
  }
  for (cp = &soap->clist; *cp; cp = &(*cp)->next)
  { if (p == (*cp)->ptr)
    { DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Unlinked class instance %p\n", p));
      p = (void*)*cp;
      *cp = (*cp)->next;
      free(p);
      return;
    }
  }
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_lookup_type(struct soap *soap, const char *id)
{ struct soap_ilist *ip;
  if (*id)
  { ip = soap_lookup(soap, id);
    if (ip)
    { DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Lookup id='%s' type=%d\n", id, ip->type));
      return ip->type;
    }
    if (*id != '#' && !strchr(id, ':')) /* try content id "cid:" with DIME attachments */
    { char cid[SOAP_TAGLEN];
      strcpy(cid, "cid:");
      strncat(cid + 4, id, SOAP_TAGLEN - 5);
      cid[SOAP_TAGLEN - 1] = '\0';
      ip = soap_lookup(soap, cid);
      if (ip)
      { DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Lookup id='%s' type=%d\n", cid, ip->type));
        return ip->type;
      }
    }
  }
  DBGLOG(TEST, SOAP_MESSAGE(fdebug, "lookup type id='%s' NOT FOUND! Need to get it from xsi:type\n", id));
  return 0;
}

/******************************************************************************/
SOAP_FMAC1
void*
SOAP_FMAC2
soap_id_lookup(struct soap *soap, const char *id, void **p, int t, size_t n, int k)
{ struct soap_ilist *ip;
  void *q;
  if (*id == '\0')
    return p;
  soap->alloced = 0;
  if (!p)
    p = (void**)soap_malloc(soap, sizeof(void*));
  ip = soap_lookup(soap, id); /* lookup pointer to hash table entry for string id */
  if (!ip)
  { ip = soap_enter(soap, id); /* new hash table entry for string id */
    DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Forwarding first href='%s' %p (%u bytes)\n", id, p, (unsigned int)n));
    ip->type = t;
    ip->size = n; 
    ip->link = p;
    ip->copy = NULL;
    ip->ptr = NULL;
    ip->level = k;
    *p = NULL;
  }
  else if (!soap->blist && ip->ptr)
  { DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Resolved href='%s' (%u bytes)\n", id, (unsigned int)n));
    if (ip->type != t)
    { soap->error = SOAP_HREF;
      return NULL;
    }
    while (ip->level < k)
    { q = soap_malloc(soap, sizeof(void*));  
      *p = q;
      p = (void**)q;
      k--;
      DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Descending one level...\n"));
    }
    *p = ip->ptr;
  }
  else if (ip->level > k)
  { DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Resolving level %d pointers to href='%s'\n", ip->level, id));
    while (ip->level > k)
    { void *s, **r = &ip->link;
      q = ip->link;
      while (q)
      { *r = soap_malloc(soap, sizeof(void*));
        s = *(void**)q;
        *(void**)q = *r;
        r = *(void***)q;
        q = s;
      }
      *r = NULL;
      ip->size = n; 
      ip->copy = NULL;
      ip->level = ip->level-1;
      DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Descending one level...\n"));
    }
    q = ip->link;
    ip->link = p;
    *p = q;
  }
  else
  { DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Forwarded href='%s' (%u bytes)\n", id, (unsigned int)n));
    while (ip->level < k)
    { q = soap_malloc(soap, sizeof(void*));  
      *p = q;
      p = (void**)q;
      k--;
      DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Descending one level...\n"));
    }
    q = ip->link;
    ip->link = p;
    *p = q;
  }
  return p;
}

/******************************************************************************/
SOAP_FMAC1
void*
SOAP_FMAC2
soap_id_forward(struct soap *soap, const char *href, void *p, int t, size_t n)
{ struct soap_ilist *ip;
  if (!*href)
    return p;
  ip = soap_lookup(soap, soap->href); /* lookup pointer to hash table entry for string id */
  if (!ip)
  { if (n >= sizeof(void*))
    { ip = soap_enter(soap, href); /* new hash table entry for string id */
      ip->type = t;
      ip->size = n;
      ip->link = NULL;
      ip->copy = p;
      *(void**)p = NULL;
      ip->ptr = NULL;
      ip->level = 0;
      DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Forwarding first copying address %p for type %d href='%s'\n", p, t, href));
      return p;
    }
    DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Forwarding problem: copying location %p too small (%u) for href='%s'\n", p, (unsigned int)n, href));
    soap->error = SOAP_HREF;
    return NULL;
  }
  else if (ip->ptr)
  { if (ip->size == n)
      memcpy(p, ip->ptr, n);
    else
    { DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Reference to object of different size (ref=%u, object=%u)\n", (unsigned int)n, (unsigned int)ip->size));
      soap->error = SOAP_HREF;
      return NULL;
    }
  }
  else if (n >= sizeof(void*))
  { DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Forwarding copying address %p for type %d href='%s' (prev in chain = %p)\n", p, t, href, ip->copy));
    *(void**)p = ip->copy;
    ip->copy = p;
    return p;
  }
  else
  { DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Forwarding problem: copying location %p too small (%u) for href='%s'\n", p, (unsigned int)n, href));
    soap->error = SOAP_HREF; /* href to object too small to hold pointer */
    return NULL;
  }
  return ip->ptr;
}

/******************************************************************************/
SOAP_FMAC1
void*
SOAP_FMAC2
soap_id_enter(struct soap *soap, const char *id, void *p, int t, size_t n, int k)
{ struct soap_ilist *ip;
  DBGLOG(TEST,SOAP_MESSAGE(fdebug, "Enter id='%s' type=%d loc=%p size=%d level=%d\n", id, t, p, (int)n, k));
  soap->alloced = 0;
  if (*id == '\0')
  { if (!p)
      return soap_malloc(soap, n);
    else
      return p;
  }
  ip = soap_lookup(soap, id); /* lookup pointer to hash table entry for string id */
  DBGLOG(TEST,SOAP_MESSAGE(fdebug, "Lookup entry id='%s'\n", id));
  if (!ip)
  { ip = soap_enter(soap, id); /* new hash table entry for string id */
    DBGLOG(TEST,SOAP_MESSAGE(fdebug, "New entry id='%s' type=%d size=%u\n", id, t, (unsigned int)n));
    ip->type = t;
    ip->size = n;
    ip->link = NULL;
    ip->copy = NULL;
    if (!p)
      p = soap_malloc(soap, n);
    ip->ptr = p;
    ip->level = k;
  }
  else if (ip->ptr) /* storage address was forwarded */
  { DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Multiply defined id='%s'\n", id));
    if (p)
    { soap->error = SOAP_MULTI_ID;
      return NULL;
    }
  }
  else 
  { if (!p)
      p = soap_malloc(soap, n);
    ip->ptr = p;
    if (!soap->blist)
    { DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Resolve link chain to point to %p\n", ip->ptr));
      soap_resolve_ptr(ip);
    }
  }
  return ip->ptr;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_end_send(struct soap *soap)
{ DBGLOG(TEST, SOAP_MESSAGE(fdebug, "End send\n"));
  if (soap->mode & SOAP_IO) /* need to flush the remaining data in buffer */
  { if (soap_flush(soap))
      return soap->error;
#ifdef WITH_ZLIB
    if (soap->mode & SOAP_ENC_ZLIB)
    { int r;
      soap->d_stream.avail_in = 0;
      do
      { DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Deflating remainder\n"));
        r = deflate(&soap->d_stream, Z_FINISH);
        if (soap->d_stream.avail_out != SOAP_BUFLEN)
        { if (soap_flush_raw(soap, soap->z_buf, SOAP_BUFLEN - soap->d_stream.avail_out))
            return soap->error;
          soap->d_stream.next_out = (Byte*)soap->z_buf;
          soap->d_stream.avail_out = SOAP_BUFLEN;
        }
      } while (r == Z_OK);
      DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Deflated %lu->%lu bytes\n", soap->d_stream.total_in, soap->d_stream.total_out));
      soap->z_ratio_out = (float)soap->d_stream.total_out / (float)soap->d_stream.total_in;
      soap->mode &= ~SOAP_ENC_ZLIB;
      if (deflateEnd(&soap->d_stream) != Z_OK || r != Z_STREAM_END)
      { DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Unable to end deflate: %s\n", soap->d_stream.msg?soap->d_stream.msg:""));
        return soap->error = SOAP_ZLIB_ERROR;
      }
#ifdef WITH_GZIP
      soap->z_buf[0] = soap->z_crc & 0xFF;
      soap->z_buf[1] = soap->z_crc >> 8 & 0xFF;
      soap->z_buf[2] = soap->z_crc >> 16 & 0xFF;
      soap->z_buf[3] = soap->z_crc >> 24 & 0xFF;
      soap->z_buf[4] = soap->d_stream.total_in & 0xFF;
      soap->z_buf[5] = soap->d_stream.total_in >> 8 & 0xFF;
      soap->z_buf[6] = soap->d_stream.total_in >> 16 & 0xFF;
      soap->z_buf[7] = soap->d_stream.total_in >> 24 & 0xFF;
      if (soap_flush_raw(soap, soap->z_buf, 8))
        return soap->error;
      DBGLOG(TEST, SOAP_MESSAGE(fdebug, "gzip crc32=%lu\n", soap->z_crc));
#endif
    }
#endif
    if ((soap->mode & SOAP_IO) == SOAP_IO_STORE)
    { char *p;
      if (!(soap->mode & SOAP_ENC_XML))
      { soap->mode--;
        DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Sending buffered message of length %u\n", (unsigned int)soap->blist->size));
        if (soap->status >= SOAP_POST)
          soap->error = soap->fpost(soap, soap->endpoint, soap->host, soap->port, soap->path, soap->action, soap->blist->size);
        else
          soap->error = soap->fresponse(soap, soap->status, soap->blist->size);
        if (soap->error || soap_flush(soap))
          return soap->error;
        soap->mode++;
      }
      for (p = soap_first_block(soap); p; p = soap_next_block(soap))
      { DBGMSG(SENT, p, soap_block_size(soap));
        if ((soap->error = soap->fsend(soap, p, soap_block_size(soap))))
          return soap->error;
      }
      soap_end_block(soap);
    }
    else if ((soap->mode & SOAP_IO) == SOAP_IO_CHUNK)
    { DBGMSG(SENT, "\r\n0\r\n\r\n", 7);
      if ((soap->error = soap->fsend(soap, "\r\n0\r\n\r\n", 7)))
        return soap->error;
    }
  }
  DBGLOG(TEST, SOAP_MESSAGE(fdebug, "End of send message ok\n"));
  return SOAP_OK;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_end_recv(struct soap *soap)
{ int flag1 = 0, flag2;
  register int i;
  register struct soap_ilist *ip;
  for (i = 0; i < SOAP_IDHASH; i++)
    for (ip = soap->iht[i]; ip; ip = ip->next)
    { if (ip->ptr)
        soap_resolve_ptr(ip);
      else if (*ip->id == '#')
        flag1 = 1;
    }
  do
  { flag2 = 0;
    DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Resolving forwarded data\n"));
    for (i = 0; i < SOAP_IDHASH; i++)
      for (ip = soap->iht[i]; ip; ip = ip->next)
        if (ip->copy && ip->ptr)
        { register void *p, **q = (void**)ip->copy;
          DBGLOG(TEST, if (q) SOAP_MESSAGE(fdebug, "Traversing copy chain to resolve id='%s'\n", ip->id));
          ip->copy = NULL;
          do
          { DBGLOG(TEST, SOAP_MESSAGE(fdebug, "... copy %p -> %p (%u bytes)\n", ip->ptr, q, (unsigned int)ip->size));
            p = *q;
            memcpy(q, ip->ptr, ip->size);
            soap_add_copies(soap, (char*)ip->ptr, (char*)ip->ptr + ip->size, (char*)q - (char*)ip->ptr);
            q = (void**)p;
          } while (q);
	  flag2 = 1;
        }
  } while (flag2);
  DBGLOG(TEST,SOAP_MESSAGE(fdebug, "End of receive message ok\n"));
#ifdef WITH_ZLIB
  if (soap->mode & SOAP_ENC_ZLIB)
  { soap->mode &= ~SOAP_ENC_ZLIB;
    memcpy(soap->buf, soap->z_buf, SOAP_BUFLEN);
    soap->bufidx = (char*)soap->d_stream.next_in - soap->z_buf;
    soap->buflen = soap->z_buflen;
    if (inflateEnd(&soap->d_stream) != Z_OK)
      return soap->error = SOAP_ZLIB_ERROR;
#ifdef WITH_GZIP
    if (soap->zlib_in == SOAP_ZLIB_GZIP)
    { wchar c;
      for (i = 0; i < 8; i++)
      { if ((c = soap_getchar(soap)) == EOF)
          return soap->error = SOAP_EOF;
        soap->z_buf[i] = (char)c;
      }
      if (soap->z_crc != ((unsigned char)soap->z_buf[0] | (unsigned long)((unsigned char)soap->z_buf[1] << 8) | (unsigned long)((unsigned char)soap->z_buf[2] << 16) | (unsigned long)((unsigned char)soap->z_buf[3] << 24)))
      { DBGLOG(TEST,SOAP_MESSAGE(fdebug, "Gzip error: crc check failed, message corrupted? (crc32=%lu)\n", soap->z_crc));
        return soap->error = SOAP_ZLIB_ERROR;
      }
      if (soap->d_stream.total_out != ((unsigned char)soap->z_buf[4] | (unsigned long)((unsigned char)soap->z_buf[5] << 8) | (unsigned long)((unsigned char)soap->z_buf[6] << 16) | (unsigned long)((unsigned char)soap->z_buf[7] << 24)))
      { DBGLOG(TEST,SOAP_MESSAGE(fdebug, "Gzip error: incorrect message length\n"));
        return soap->error = SOAP_ZLIB_ERROR;
      }
    }
#endif
  }
#endif
  if ((soap->mode & SOAP_IO) == SOAP_IO_CHUNK)
    while (soap_getchar(soap) != EOF) /* advance to last chunk */
      ;
  if (flag1)
    return soap->error = SOAP_MISSING_ID;
  return SOAP_OK;
}

/******************************************************************************/
SOAP_FMAC1
void
SOAP_FMAC2
soap_free(struct soap *soap)
{ struct soap_nlist *np;
  struct soap_attribute *tp;
  struct Namespace *ns;
  DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Free namespace stack\n"));
  while (soap->nlist)
  { np = soap->nlist->next;
    if (soap->nlist->ns)
      free(soap->nlist->ns);
    free(soap->nlist);
    soap->nlist = np;
  }
  DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Free any remaining temp blocks\n"));
  while (soap->blist)
    soap_end_block(soap);
  DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Free attributes\n"));
  while (soap->attributes)
  { tp = soap->attributes->next;
    free(soap->attributes->value);
    free(soap->attributes);
    soap->attributes = tp;
  }
  DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Free pointer hashtable\n"));
  soap_free_pht(soap);
  DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Free ID hashtable\n"));
  soap_free_iht(soap);
  if (soap->action)
  { free(soap->action);
    soap->action = NULL;
  }
  ns = soap->local_namespaces;
  if (ns)
  { for (; ns->id; ns++)
    { if (ns->out)
      { free(ns->out);
	if (soap->encodingStyle == ns->out)
          soap->encodingStyle = "";
        ns->out = NULL;
      }
      if (soap->encodingStyle == ns->ns)
        soap->encodingStyle = "";
    }
    free(soap->local_namespaces);
    soap->local_namespaces = NULL;
  }
}

/******************************************************************************/
static void
soap_init_logs(struct soap *soap)
{ int i;
  for (i = 0; i < SOAP_MAXLOGS; i++)
  { soap->logfile[i] = NULL;
    soap->fdebug[i] = NULL;
  }
#ifdef SOAP_DEBUG  
  soap_set_recv_logfile(soap, "RECV.log");
  soap_set_sent_logfile(soap, "SENT.log");
  soap_set_test_logfile(soap, "TEST.log");
#endif
}

/******************************************************************************/
SOAP_FMAC1
void
SOAP_FMAC2
soap_open_logfile(struct soap *soap, int i)
{ if (soap->logfile[i])
    soap->fdebug[i] = fopen(soap->logfile[i], i < 2 ? "ab" : "a");
}

/******************************************************************************/
static void
soap_close_logfile(struct soap *soap, int i)
{ if (soap->fdebug[i])
  { fclose(soap->fdebug[i]);
    soap->fdebug[i] = NULL;
  }
}

/******************************************************************************/
SOAP_FMAC1
void
SOAP_FMAC2
soap_close_logfiles(struct soap *soap)
{ int i;
  for (i = 0; i < SOAP_MAXLOGS; i++)
    soap_close_logfile(soap, i);
}

/******************************************************************************/
static void
soap_set_logfile(struct soap *soap, int i, const char *logfile)
{ char *s = NULL;
  soap_close_logfile(soap, i);
  if (logfile)
  { s = (char*)malloc(strlen(logfile)+1);
    if (s)
      strcpy(s, logfile);
  }
  if (soap->logfile[i])
    free((void*)soap->logfile[i]);
  soap->logfile[i] = s;
}

/******************************************************************************/
SOAP_FMAC1
void
SOAP_FMAC2
soap_set_recv_logfile(struct soap *soap, const char *logfile)
{ soap_set_logfile(soap, SOAP_INDEX_RECV, logfile);
}

/******************************************************************************/
SOAP_FMAC1
void
SOAP_FMAC2
soap_set_sent_logfile(struct soap *soap, const char *logfile)
{ soap_set_logfile(soap, SOAP_INDEX_SENT, logfile);
}

/******************************************************************************/
SOAP_FMAC1
void
SOAP_FMAC2
soap_set_test_logfile(struct soap *soap, const char *logfile)
{ soap_set_logfile(soap, SOAP_INDEX_TEST, logfile);
}

/******************************************************************************/
SOAP_FMAC1
struct soap*
SOAP_FMAC2
soap_copy(struct soap *soap)
{ struct soap *copy = (struct soap*)malloc(sizeof(struct soap));
  if (copy)
  { struct soap_plugin *p;
    memcpy(copy, soap, sizeof(struct soap));
    copy->copy = 1;
    copy->user = NULL;
    copy->userid = NULL;
    copy->passwd = NULL;
    copy->nlist = NULL;
    copy->blist = NULL;
    copy->clist = NULL;
    copy->alist = NULL;
    copy->attributes = NULL;
    copy->local_namespaces = NULL;
    soap_init_iht(copy);
    soap_init_pht(copy);
    soap_init_logs(copy);
    copy->header = NULL;
    copy->fault = NULL;
    copy->action = NULL;
    *copy->host = '\0';
    copy->port = 0;
#ifdef WITH_COOKIES
    copy->cookies = soap_copy_cookies(soap);
#else
    copy->cookies = NULL;
#endif
    copy->plugins = NULL;
    for (p = soap->plugins; p; p = p->next)
    { struct soap_plugin *q = (struct soap_plugin*)malloc(sizeof(struct soap_plugin));
      if (!q)
        return NULL;
      DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Copying plugin '%s'\n", p->id));
      *q = *p;
      if ((soap->error = p->fcopy(soap, q, p)))
      { free(q);
        return NULL;
      }
      q->next = copy->plugins;
      copy->plugins = q;
    }
  }
  else
    soap->error = SOAP_EOM;
  return copy;
}

/******************************************************************************/
SOAP_FMAC1
void
SOAP_FMAC2
soap_init(struct soap *soap)
{ soap->version = 1; /* default SOAP 1.1 */
  soap_imode(soap, SOAP_IO_DEFAULT);
  soap_omode(soap, SOAP_IO_DEFAULT);
  soap->mode = 0;
  soap->copy = 0;
  soap->plugins = NULL;
  soap->user = NULL;
  soap->userid = NULL;
  soap->passwd = NULL;
  soap->fpost = http_post;
  soap->fposthdr = http_post_header;
  soap->fresponse = http_response;
  soap->fparse = http_parse;
  soap->fparsehdr = http_parse_header;
  soap->faccept = tcp_accept;
#ifndef MAC_CARBON
  soap->fopen = tcp_connect;
  soap->fclose = tcp_disconnect;
  soap->fsend = fsend;
  soap->frecv = frecv;
#endif
  soap->fignore = fignore;
  soap->fplugin = fplugin;
  soap->fdimereadopen = NULL;
  soap->fdimewriteopen = NULL;
  soap->fdimereadclose = NULL;
  soap->fdimewriteclose = NULL;
  soap->fdimeread = NULL;
  soap->fdimewrite = NULL;
  soap->float_format = "%.9G"; /* .9G preserves single FP precision */
  soap->double_format = "%.18G"; /* .18G preserves double FP precision */
  soap->dime_id_format = "cid:id%d"; /* default DIME id format */
  soap->http_version = "1.0";
  soap->encodingStyle = "";
  soap->defaultNamespace = NULL;
  soap->actor = NULL;
  soap->keep_alive = 0;
  soap->recv_timeout = 0;
  soap->send_timeout = 0;
  soap->connect_timeout = 0;
  soap->accept_timeout = 0;
  soap->socket_flags = 0;
  soap->connect_flags = 0;
  soap->bind_flags = 0;
  soap->accept_flags = 0;
  soap->ip = 0;
#ifndef WITH_NONAMESPACES
  soap->namespaces = namespaces;
#else
  soap->namespaces = NULL;
#endif
  soap->local_namespaces = NULL;
  soap->nlist = NULL;
  soap->blist = NULL;
  soap->clist = NULL;
  soap->alist = NULL;
  soap->attributes = NULL;
  soap->header = NULL;
  soap->fault = NULL;
  soap->master = -1;
  soap->socket = -1;
#ifndef UNDER_CE
  soap->recvfd = 0;
  soap->sendfd = 1;
#else
  soap->recvfd = stdin;
  soap->sendfd = stdout;
#endif 
  *soap->host = '\0';
  soap->port = 0;
  *soap->endpoint = '\0';
  *soap->path = '\0';
  soap->action = NULL;
  soap->proxy_host = NULL;
  soap->proxy_port = 8080;
  soap->proxy_userid = NULL;
  soap->proxy_passwd = NULL;
  soap->cookies = NULL;
  soap->cookie_domain = NULL;
  soap->cookie_path = NULL;
  soap->cookie_max = 32;
#ifdef WITH_OPENSSL
  soap->bio = NULL;
  soap->ssl = NULL;
  soap->require_server_auth = 0;
  soap->rsa = 0;
  soap->keyfile = NULL;
  soap->password = NULL;
  soap->dhfile = NULL;
  soap->cafile = NULL;
  soap->randfile = NULL;
#endif
#ifdef WITH_ZLIB
  soap->zlib_in = SOAP_ZLIB_NONE;
  soap->zlib_out = SOAP_ZLIB_NONE;
  soap->d_stream.zalloc = NULL;
  soap->d_stream.zfree = NULL;
  soap->d_stream.opaque = NULL;
  soap->z_level = 6;
#endif
  soap_init_logs(soap);
  soap_init_iht(soap);
  soap_init_pht(soap);
  soap_begin(soap);
}

/******************************************************************************/
SOAP_FMAC1
void
SOAP_FMAC2
soap_init2(struct soap *soap, int imode, int omode)
{ soap_init(soap);
  soap_imode(soap, imode);
  soap_omode(soap, omode);
}

/******************************************************************************/
SOAP_FMAC1
void
SOAP_FMAC2
soap_begin(struct soap *soap)
{ DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Initializing\n"));
  if (!soap->keep_alive)
  { soap->buflen = 0;
    soap->bufidx = 0;
  }
  soap->null = 0;
  soap->position = 0;
  soap->mustUnderstand = 0;
  soap->mode = 0;
  soap->ns = 0;
  soap->is_in_header = 0;
  soap->alloced = 0;
  soap->count = 0;
  soap->length = 0;
  soap->cdata = 0;
  soap->status = SOAP_POST;
  soap->error = SOAP_OK;
  soap->errnum = 0;
  soap->peeked = 0;
  soap->ahead = 0;
  soap->idnum = 0;
  soap->level = 0;
  *soap->endpoint = '\0';
  *soap->path = '\0';
  soap->dime_chunksize = 0;
  soap->dime_buflen = 0;
  soap->dot_net_bug = 0;
  soap_free(soap);
}

/******************************************************************************/
static void
soap_set_local_namespaces(struct soap *soap)
{ if (soap->namespaces && !soap->local_namespaces)
  { struct Namespace *ns;
    size_t n = 1;
    for (ns = soap->namespaces; ns->id; ns++)
      n++;
    if (n > 3)
    { n *= sizeof(struct Namespace);
      ns = (struct Namespace*)malloc(n);
      if (ns)
      { memcpy(ns, soap->namespaces, n);
        ns[0].id = "SOAP-ENV";
        ns[1].id = "SOAP-ENC";
        ns[2].id = "xsi";
        if (ns[0].ns)
        { if (!strcmp(ns[0].ns, soap_env1))
            soap->version = 1;
          else if (!strcmp(ns[0].ns, soap_env2))
            soap->version = 2;
        }
        soap->local_namespaces = ns;
      }
    }
  }
}

/******************************************************************************/
SOAP_FMAC1
void
SOAP_FMAC2
soap_end(struct soap *soap)
{ struct soap_clist *cp;
  soap_free(soap);
  soap_dealloc(soap, NULL);
  while (soap->clist)
  { cp = soap->clist->next;
    free(soap->clist);
    soap->clist = cp;
  }
  soap_closesock(soap);
  soap_close_logfiles(soap);
}

/******************************************************************************/
static int
soap_element(struct soap *soap, const char *tag, int id, const char *type)
{ struct soap_attribute *tp;
  DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Element begin tag='%s' id='%d' type='%s'\n", tag, id, type?type:""));
  if (!soap->ns)
  { struct Namespace *ns = soap->local_namespaces;
    if (soap_send(soap, "<?xml version=\"1.0\" encoding=\"UTF-8\"?><") || soap_send(soap, tag))
      return soap->error;
    for (; ns && ns->id; ns++)
    { *soap->tmpbuf = '\0';
      if (*ns->id && (ns->out || ns->ns))
        sprintf(soap->tmpbuf," xmlns:%s=\"%s\"", ns->id, ns->out?ns->out:ns->ns);
      else if (!soap->defaultNamespace && ns->ns)
        sprintf(soap->tmpbuf," xmlns=\"%s\"", ns->ns);
      if (soap_send(soap, soap->tmpbuf))
        return soap->error;
    }   
    if (soap->defaultNamespace)
    { sprintf(soap->tmpbuf, " xmlns=\"%s\"", soap->defaultNamespace);
      if (soap_send(soap, soap->tmpbuf))
        return soap->error;
    }
    soap->ns = 1;
  }
  else if (soap_send(soap, "<") || soap_send(soap, tag))
    return soap->error;
  if (id > 0)
  { sprintf(soap->tmpbuf, " id=\"_%d\"", id);
    if (soap_send(soap, soap->tmpbuf))
      return soap->error;
  }
  if (type && *type)
  { if (soap_send(soap, " xsi:type=\"") || soap_send(soap, type) || soap_send(soap, "\""))
      return soap->error;
  }
  if (soap->null && soap->position > 0)
  { int i;
    sprintf(soap->tmpbuf, " SOAP-ENC:position=\"[%d", soap->positions[0]);
    for (i = 1; i < soap->position; i++)
      sprintf(soap->tmpbuf + strlen(soap->tmpbuf), ",%d", soap->positions[i]);
    strcat(soap->tmpbuf, "]\"");
    if (soap_send(soap, soap->tmpbuf))
      return soap->error;
  }
  if (soap->mustUnderstand)
  { if (soap->actor)
    { if (soap->version == 2)
      { if (soap_send(soap, " SOAP-ENV:role=\"") || soap_send(soap, soap->actor) || soap_send(soap, "\""))
          return soap->error;
      }
      else if (soap_send(soap, " SOAP-ENV:actor=\"") || soap_send(soap, soap->actor) || soap_send(soap, "\""))
        return soap->error;
    }
    if (soap->version == 2)
    { if (soap_send(soap, " SOAP-ENV:mustUnderstand=\"true\""))
        return soap->error;
    }
    else if (soap_send(soap, " SOAP-ENV:mustUnderstand=\"1\""))
      return soap->error;
    soap->mustUnderstand = 0;
  }
  for (tp = soap->attributes; tp; tp = tp->next)
  { if (tp->visible)
    { if (soap_send(soap, " ") || soap_send(soap, tp->name))
        return soap->error;
      if (tp->value)
        if (soap_send(soap, "=\"") || soap_string_out(soap, tp->value) || soap_send(soap, "\""))
          return soap->error;
      tp->visible = 0;
    }
  }
  soap->null = 0;
  return SOAP_OK;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_element_begin_out(struct soap *soap, const char *tag, int id, const char *type)
{ if (soap_element(soap, tag, id, type))
    return soap->error;
  return soap_send(soap, ">");
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_array_begin_out(struct soap *soap, const char *tag, int id, const char *type, const char *offset)
{ if (soap_element(soap, tag, id, "SOAP-ENC:Array"))
    return soap->error;
  if (offset && soap->version != 2)
  { if (soap_send(soap, " SOAP-ENC:offset=\"") || soap_send(soap, offset) || soap_send(soap, "\""))
      return soap->error;
  }
  if (type && *type)
  { if (soap->version == 2)
    { const char *s;
      s = strrchr(type, '[');
      if (s-type < SOAP_TAGLEN)
      { strncpy(soap->tmpbuf, type, s-type);
        soap->tmpbuf[s-type] = '\0';
        if (soap_send(soap, " SOAP-ENC:itemType=\"") || soap_send(soap, soap->tmpbuf) || soap_send(soap, "\">"))
          return soap->error;
      }
      if (s && (soap_send(soap, " SOAP-ENC:arraySize=\"") || soap_send(soap, s+1) || soap_send(soap, "\">")))
        return soap->error;
    }
    else if (soap_send(soap, " SOAP-ENC:arrayType=\"") || soap_send(soap, type) || soap_send(soap, "\">"))
      return soap->error;
  }
  return SOAP_OK;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_element_end_out(struct soap *soap, const char *tag)
{ DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Element ending tag='%s'\n", tag));
  if (soap_send(soap, "</") || soap_send(soap, tag) || soap_send(soap, ">"))
    return soap->error;
  return SOAP_OK;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_element_ref(struct soap *soap, const char *tag, int id, int href)
{ if (soap_element(soap, tag, id, NULL))
    return soap->error;
  if (soap->version == 2)
    sprintf(soap->tmpbuf, " ref=\"_%d\"/>", href);
  else
    sprintf(soap->tmpbuf, " href=\"#_%d\"/>", href);
  return soap_send(soap, soap->tmpbuf);
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_element_href(struct soap *soap, const char *tag, int id, const char *href)
{ if (soap_element(soap, tag, id, NULL))
    return soap->error;
  if (soap_send(soap, " href=\"") || soap_send(soap, href) || soap_send(soap, "\"/>"))
    return soap->error;
  return SOAP_OK;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_element_null(struct soap *soap, const char *tag, int id, const char *type)
{ if ((soap->version == 2 && soap->position > 0) || id > 0 || (soap->mode & SOAP_XML_NIL))
  { if (soap_element(soap, tag, id, type))
      return soap->error;
    return soap_send(soap, " xsi:nil=\"true\"/>");
  }
  soap->null = 1;
  soap->mustUnderstand = 0;
  return SOAP_OK;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_element_begin_in(struct soap *soap, const char *tag)
{ if (!soap_peek_element(soap))
  { if (soap->other)
      return soap->error = SOAP_TAG_MISMATCH;
    if (!(soap->error = soap_match_tag(soap, soap->tag, tag)))
    { soap->peeked = 0;
      DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Begin element found (level=%u) '%s'='%s'\n", soap->level, soap->tag, tag?tag:"" ));
      if (soap->body)
        soap->level++;
    }
  }
  return soap->error;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_element_end_in(struct soap *soap, const char *tag)  
{ int c;
  char *s;
  const char *t;
  soap->error = SOAP_OK;
  soap->level--;
  soap_pop_namespace(soap);
  if (soap->peeked)
  { if (*soap->tag == '\0')
      soap->peeked = 0;
    else
      return soap->error = SOAP_SYNTAX_ERROR;
  }
  else
  { c = soap_advance(soap);
    if (c == EOF)
      return soap->error = SOAP_EOF;
    if (c != TT)
      return soap->error = SOAP_SYNTAX_ERROR;
  }
  s = soap->tag;
  c = soap_skip(soap);
  do
  { *s++ = c;
    c = soap_get(soap);
  } while (notblank(c));
  if (c == EOF)
    return soap->error = SOAP_EOF;
  *s = '\0';
  if ((s = strchr(soap->tag, ':')))
    s++;
  else
    s = soap->tag;
  t = tag;
  if (t && (t = strchr(t, ':')))
    t++;
  else if (t && (t = strchr(t, '_')) && t[1] == '_')
    t += 2;
  if (blank(c))
    c = soap_skip(soap);
  if (c != GT)
    return soap->error = SOAP_SYNTAX_ERROR;
  DBGLOG(TEST, SOAP_MESSAGE(fdebug, "End element found (level=%u) '%s'='%s'\n", soap->level, soap->tag, tag?tag:"" ));
  if (!t || !soap_tag_cmp(s, t))
    return SOAP_OK;
  return soap->error = SOAP_SYNTAX_ERROR;
}

/******************************************************************************/
SOAP_FMAC1
struct soap_attribute *
SOAP_FMAC2
soap_attr(struct soap *soap, const char *name)
{ struct soap_attribute *tp;
  for (tp = soap->attributes; tp; tp = tp->next)
    if (!soap_match_tag(soap, tp->name, name))
      return tp;
  return NULL;
}

/******************************************************************************/
SOAP_FMAC1
const char *
SOAP_FMAC2
soap_attr_value(struct soap *soap, const char *name)
{ struct soap_attribute *tp = soap_attr(soap, name);
  if (tp && tp->visible)
    return tp->value;
  return NULL;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_set_attr(struct soap *soap, const char *name, const char *value)
{ struct soap_attribute *tp = soap_attr(soap, name);
  if (!tp)
  { if (!(tp = (struct soap_attribute*)malloc(sizeof(struct soap_attribute))))
      return soap->error = SOAP_EOM;
    tp->next = soap->attributes;
    soap->attributes = tp;
    tp->value = NULL;
  }
  else if (tp->value && tp->size <= strlen(value))
  { free(tp->value);
    tp->value = NULL;
  }
  tp->visible = 1;
  tp->name = name;
  if (!tp->value)
  { tp->size = strlen(value) + 1;
    if (!(tp->value = (char*)malloc(tp->size)))
      return soap->error = SOAP_EOM;
  }
  strcpy(tp->value, value);
  return SOAP_OK;
}

/******************************************************************************/
SOAP_FMAC1
void
SOAP_FMAC2
soap_clr_attr(struct soap *soap)
{ struct soap_attribute *tp;
  for (tp = soap->attributes; tp; tp = tp->next)
    tp->visible = 0;
}

/******************************************************************************/
static int
soap_getattrval(struct soap *soap, char *s, size_t n, wchar d)
{ size_t i;
  wchar c;
  for (i = 0; i < n; i++)
  { c = soap_getutf8(soap);
    switch (c)
    {
    case EOF:
      return soap->error = SOAP_EOF;
    case TT:
      *s++ = '<';
      soap_unget(soap, '/');
      break;
    case LT:
      *s++ = '<';
      break;
    case GT:
      if (d == ' ')
      { soap_unget(soap, c);
        *s = '\0';
        return SOAP_OK;
      }
      *s++ = '>';
      break;
    case QT:
      if (c == d)
      { *s = '\0';
        return SOAP_OK;
      }
      *s++ = '"';
      break;
    case AP:
      if (c == d)
      { *s = '\0';
        return SOAP_OK;
      }
      *s++ = '\'';
      break;
    case '\t':
    case '\n':
    case '\r':
    case ' ':
    case '/':
      if (d == ' ')
      { soap_unget(soap, c);
        *s = '\0';
        return SOAP_OK;
      }
    default:
      *s++ = (char)c;
    }
  }
  return soap->error = SOAP_EOM;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_peek_element(struct soap *soap)
{ struct soap_attribute *tp;
  char *s, *t;
  wchar c;
  int i;
  if (soap->peeked)
  { if (*soap->tag == '\0')
      return soap->error = SOAP_NO_TAG;
    return SOAP_OK;
  }
  soap->peeked = 1;
  if ((c = soap_advance(soap)) == EOF)
    return soap->error = SOAP_EOF;
  if (c == TT)
  { *soap->tag = '\0';
    return soap->error = SOAP_NO_TAG; /* ending tag found */
  }
  s = soap->tag;
  c = soap_skip(soap);
  i = SOAP_TAGLEN;
  while (c != '/' && notblank(c))
  { if (--i > 0)
      *s++ = (char)c;
    c = soap_get(soap);
  }
  while (blank(c))
    c = soap_get(soap);
  *s = '\0';
  *soap->id = '\0';
  *soap->href = '\0';
  *soap->type = '\0';
  *soap->arrayType = '\0';
  *soap->arraySize = '\0';
  *soap->arrayOffset = '\0';
  soap->other = 0;
  soap->root = -1;
  soap->position = 0;
  soap->null = 0;
  soap->mustUnderstand = 0;
  soap_clr_attr(soap);
  soap_pop_namespace(soap);
  do
  { s = soap->tmpbuf;
    *s = '\0';
    if (c == EOF || c == GT || c == '/')
      break;
    i = SOAP_TAGLEN;
    while (c != '=' && c != GT && c != '/' && notblank(c))
    { if (--i > 0)
        *s++ = (char)c;
      c = soap_get(soap);
    }
    *s = '\0';
    if (!strncmp(soap->tmpbuf, "xmlns:", 6))
    { soap->tmpbuf[5] = '\0';
      t = soap->tmpbuf + 6;
    }
    else
      t = NULL;
    tp = soap_attr(soap, soap->tmpbuf);
    if (!tp)
    { tp = (struct soap_attribute*)malloc(sizeof(struct soap_attribute) + strlen(soap->tmpbuf) + 1); /* reserve node + space for name */
      if (!tp)
        return soap->error = SOAP_EOM;
      s = (char*)tp + sizeof(struct soap_attribute);
      strcpy(s, soap->tmpbuf);
      tp->name = s;
      tp->value = NULL;
      tp->size = 0;
      tp->next = soap->attributes;
      soap->attributes = tp;
    }
    tp->visible = 1; /* seen this attribute */
    if (blank(c))
      c = soap_skip(soap);
    if (c == '=')
    { wchar d = soap_skip(soap);
      if (d != QT && d != AP)
      { soap_unget(soap, d);
        d = ' '; /* blank delimiter */
      }
      if (soap_getattrval(soap, tp->value, tp->size, d))
      { size_t size;
        if (soap->error == SOAP_EOM)
          soap->error = SOAP_OK;
        else
          return soap->error;
        if (soap_new_block(soap))
          return soap->error;
        for (;;)
        { if (!(s = (char*)soap_push_block(soap, SOAP_BLKLEN)))
            return soap->error;
          if (soap_getattrval(soap, s, SOAP_BLKLEN, d))
          { if (soap->error == SOAP_EOM)
              soap->error = SOAP_OK;
            else
              return soap->error;
          }
          else
            break;
        }
        size = tp->size + soap->blist->size;
        if (!(s = (char*)malloc(size)))
          return soap->error = SOAP_EOM;
        soap_save_block(soap, s + tp->size);
        if (tp->value)
        { memcpy(s, tp->value, tp->size);
          free(tp->value);
        }
        tp->value = s;
        tp->size = size;
      }
      c = soap_skip(soap);
    }
    if (t)
    { if (soap_push_namespace(soap, t, tp->value))
        return soap->error;
      tp->visible = 0;
    }
  } while (c != EOF && c != GT && c != '/');
  if (c == EOF)
    return soap->error = SOAP_EOF;
  for (tp = soap->attributes; tp; tp = tp->next)
  { if (tp->visible)
    { if (!soap_match_tag(soap, tp->name, "id"))
      { if (!soap->is_in_header || !soap->dot_net_bug)
        { *soap->id = '#';
          strncpy(soap->id + 1, tp->value, SOAP_TAGLEN - 2);
        }
      }
      else if (!soap_match_tag(soap, tp->name, "href"))
        strncpy(soap->href, tp->value, SOAP_TAGLEN - 1);
      else if (soap->version == 2 && !soap_match_tag(soap, tp->name, "ref"))
      { *soap->href = '#';
        strncpy(soap->href + 1, tp->value, SOAP_TAGLEN - 2);
      }
      else if (!soap_match_tag(soap, tp->name, "xsi:type"))
        strncpy(soap->type, tp->value, SOAP_TAGLEN-1);
      else if (soap->version == 1 && !soap_match_tag(soap, tp->name, "SOAP-ENC:arrayType"))
      { s = strrchr(tp->value, '[');
        if (s && s-tp->value < SOAP_TAGLEN)
        { strncpy(soap->arrayType, tp->value, s-tp->value);
          soap->arrayType[s-tp->value] = '\0';
          strncpy(soap->arraySize, s, SOAP_TAGLEN-1);
        }
        else
          strncpy(soap->arrayType, tp->value, SOAP_TAGLEN-1);
      }
      else if (soap->version == 2 && !soap_match_tag(soap, tp->name, "SOAP-ENC:itemType"))
        strncpy(soap->arrayType, tp->value, SOAP_TAGLEN-1);
      else if (soap->version == 2 && !soap_match_tag(soap, tp->name, "SOAP-ENC:arraySize"))
        strncpy(soap->arraySize, tp->value, SOAP_TAGLEN-1);
      else if (soap->version == 1 && !soap_match_tag(soap, tp->name, "SOAP-ENC:offset"))
        strncpy(soap->arrayOffset, tp->value, SOAP_TAGLEN);
      else if (soap->version == 1 && !soap_match_tag(soap, tp->name, "SOAP-ENC:position"))
        soap->position = soap_getposition(tp->value, soap->positions);
      else if (soap->version == 1 && !soap_match_tag(soap, tp->name, "SOAP-ENC:root"))
        soap->root = ((!strcmp(tp->value, "1") || !strcmp(tp->value, "true")));
      else if (!soap_match_tag(soap, tp->name, "SOAP-ENV:actor")
            || !soap_match_tag(soap, tp->name, "SOAP-ENV:role"))
      { if ((!soap->actor || strcmp(soap->actor, tp->value))
         && strcmp(tp->value, "http://schemas.xmlsoap.org/soap/actor/next")
         && strcmp(tp->value, "http://www.w3.org/2002/06/soap-envelope/role/next"))
          soap->other = 1;
      }
      else if (!soap_match_tag(soap, tp->name, "SOAP-ENV:mustUnderstand")
            && (!strcmp(tp->value, "1") || !strcmp(tp->value, "true")))
        soap->mustUnderstand = 1;
      else if ((!soap_match_tag(soap, tp->name, "xsi:null")
             || !soap_match_tag(soap, tp->name, "xsi:nil"))
            && (!strcmp(tp->value, "1")
             || !strcmp(tp->value, "true")))
        soap->null = 1;
    }
  }
  if (!(soap->body = (c != '/')))
    soap_skip(soap);
  return SOAP_OK;
}

/******************************************************************************/
SOAP_FMAC1
void
SOAP_FMAC2
soap_revert(struct soap *soap)
{ soap->peeked = 1;
  if (soap->body)
    soap->level--;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_ignore_element(struct soap *soap)
{ if (!soap_peek_element(soap) || soap->error == SOAP_TAG_MISMATCH)
  { soap->peeked = 0;
    DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Unknown element '%s' (level=%u, %d)\n", soap->tag, soap->level, soap->body));
    if (soap->mustUnderstand && !soap->other)
      return soap->error = SOAP_MUSTUNDERSTAND;
    soap->error = soap->fignore(soap, soap->tag);
    DBGLOG(TEST, if (!soap->error) SOAP_MESSAGE(fdebug, "IGNORING element '%s'\n", soap->tag));
    if (!soap->error && soap->body)
    { soap->level++;
      while (!soap_ignore_element(soap))
        ;
      if (soap->error == SOAP_NO_TAG)
        soap_element_end_in(soap, NULL);
    }
  }
  return soap->error;
}

/******************************************************************************/
static int
fignore(struct soap *soap, const char *tag)
{ if (soap->mode & SOAP_XML_STRICT)
    return SOAP_TAG_MISMATCH;
  return SOAP_OK;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_string_out(struct soap *soap, const char *s)
{ const char *t;
  char tmp[16];
  register short c;
  register short mask = 0;
  if (soap->mode & SOAP_C_UTFSTRING)
    mask = (short)0x8000;
  while ((c = *s++))
  { switch (c)
    { 
    case '&':
      t = "&amp;";
      break;
    case '<':
      t = "&lt;";
      break;
    case '>':
      t = "&gt;";
      break;
    case '"':
      t = "&quot;";
      break;
    case '\'':
      t = "&apos;";
      break;
    default:
      if (c >= 32 || (c & mask))
      { tmp[0] = (char)c;
        if (soap_send_raw(soap, tmp, 1))
          return soap->error;
      }
      else
      { sprintf(tmp, "&#%u;", (unsigned char)c);
        if (soap_send(soap, tmp))
          return soap->error;
      }
      continue;
    }
    if (soap_send(soap, t))
      return soap->error;
  }
  return SOAP_OK;
}

/******************************************************************************/
SOAP_FMAC1
char *
SOAP_FMAC2
soap_string_in(struct soap *soap, int flag)
{ char *t;
  register char *s;
  register int i;
  register int n;
  register wchar c;
  n = 0;
  if (soap_new_block(soap))
    return NULL;
  for (;;)
  { if (!(s = (char*)soap_push_block(soap, SOAP_BLKLEN)))
      return NULL;
    if (flag)
    { for (i = 0; i < SOAP_BLKLEN; i++)
      { if (soap->mode & SOAP_C_UTFSTRING)
          c = soap_get(soap);
        else
          c = soap_getutf8(soap);
        switch (c)
        {
        case EOF:
          goto end;
        case TT:
          if (n == 0)
            goto end;
          n--;
          *s++ = '<';
          soap_unget(soap, '/');
          break;
        case LT:
          n++;
          *s++ = '<';
          break;
        case GT:
          *s++ = '>';
          break;
        case QT:
          *s++ = '"';
          break;
        case AP:
          *s++ = '\'';
          break;
        case '/':
          if (n > 0)
          { c = soap_get(soap);
            if (c == GT)
              n--;
            soap_unget(soap, c);
          }
          *s++ = '/';
          break;
        default:
          *s++ = (char)c;
        }
      }
    }
    else
    { for (i = 0; i < SOAP_BLKLEN; i++)
      { c = soap_getchar(soap);
        switch (c)
        {
        case EOF:
          goto end;
        case '<':
          c = soap_getchar(soap);
          if (c == '/')
          { if (n == 0)
            { c = TT;
              goto end;
            }
            else
              n--;
          }
          else
            n++;
          *s++ = '<';
          soap_unget(soap, c);
          break;
        case '/':
          if (n > 0)
          { c = soap_getchar(soap);
            if (c == '>')
              n--;
            soap_unget(soap, c);
          }
          *s++ = '/';
          break;
        default:
          *s++ = (char)c;
        }
      }
    }
  }
end:
  soap_unget(soap, c);
  *s = '\0';
  soap_size_block(soap, i+1);
  t = soap_save_block(soap, NULL);
  if (flag == 2)
    if (soap_s2QName(soap, t, &t))
      return NULL;
  return t;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_wstring_out(struct soap *soap, const wchar_t *s)
{ const char *t;
  wchar c;
  while ((c = *s++))
  { switch (c)
    { 
    case '&':
      t = "&amp;";
      break;
    case '<':
      t = "&lt;";
      break;
    case '>':
      t = "&gt;";
      break;
    case '"':
      t = "&quot;";
      break;
    case '\'':
      t = "&apos;";
      break;
    default:
      if (soap_pututf8(soap, c))
        return soap->error;
      continue;
    }
    if (soap_send(soap, t))
      return soap->error;
  }
  return SOAP_OK;
}

/******************************************************************************/
SOAP_FMAC1
wchar_t *
SOAP_FMAC2
soap_wstring_in(struct soap *soap, int flag)
{ wchar_t *s;
  int i, n;
  wchar c;
  n = 0;
  if (soap_new_block(soap))
    return NULL;
  for (;;)
  { if (!(s = (wchar_t*)soap_push_block(soap, sizeof(wchar_t)*SOAP_BLKLEN)))
      return NULL;
    for (i = 0; i < SOAP_BLKLEN; i++)
    { c = soap_getutf8(soap);
      switch (c)
      {
      case EOF:
        goto end;
      case TT:
        if (n == 0)
          goto end;
        n--;
        *s++ = '<';
        soap_unget(soap, '/');
        break;
      case LT:
        n++;
        *s++ = '<';
        break;
      case GT:
        *s++ = '>';
        break;
      case QT:
        *s++ = '"';
        break;
      case AP:
        *s++ = '\'';
        break;
      case '/':
        if (n > 0)
        { c = soap_getutf8(soap);
          if (c == GT)
            n--;
          soap_unget(soap, c);
        }
        *s++ = '/';
        break;
      default:
        *s++ = (wchar_t)c & 0x7FFFFFFF;
      }
    }
  }
end:
  soap_unget(soap, c);
  *s = '\0';
  soap_size_block(soap, sizeof(wchar_t)*(i+1));
  return (wchar_t*)soap_save_block(soap, NULL);
}

/******************************************************************************/
SOAP_FMAC1
const char*
SOAP_FMAC2
soap_int2s(struct soap *soap, int n)
{ sprintf(soap->tmpbuf, "%d", n);
  return soap->tmpbuf;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_outint(struct soap *soap, const char *tag, int id, const int *p, const char *type, int n)
{ if (soap_element_begin_out(soap, tag, soap_embedded_id(soap, id, p, n), type) || soap_send(soap, soap_int2s(soap, *p)))
    return soap->error;
  return soap_element_end_out(soap, tag);
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_s2int(struct soap *soap, const char *s, int *p)
{ if (s)
  { char *r;
    *p = (int)strtol(s, &r, 10);
    if (*r)
      return soap->error = SOAP_TYPE_MISMATCH;
  }
  return SOAP_OK;
}

/******************************************************************************/
SOAP_FMAC1
int *
SOAP_FMAC2
soap_inint(struct soap *soap, const char *tag, int *p, const char *type, int t)
{ if (soap_element_begin_in(soap, tag))
    return NULL;
  if (soap->null)
  { if (soap->mode & SOAP_XML_NIL)
    { soap->error = SOAP_NULL;
      return NULL;
    }
    return p;
  }
  if (*soap->type != '\0'
   && soap_match_tag(soap, soap->type, type)
   && soap_match_tag(soap, soap->type, "int")
   && soap_match_tag(soap, soap->type, "short")
   && soap_match_tag(soap, soap->type, "byte")
   && soap_match_tag(soap, soap->type, "unsignedInt")
   && soap_match_tag(soap, soap->type, "unsignedShort")
   && soap_match_tag(soap, soap->type, "unsignedByte"))
  { soap->error = SOAP_TYPE_MISMATCH;
    soap_revert(soap);
    return NULL;
  }
  if (soap->body && !*soap->href)
  { p = (int*)soap_id_enter(soap, soap->id, p, t, sizeof(int), 0);
    if (!p || soap_s2int(soap, soap_value(soap), p))
      return NULL;
  }
  else
    p = (int*)soap_id_forward(soap, soap->href, p, t, sizeof(int));
  if (soap->body && soap_element_end_in(soap, tag))
    return NULL;
  return p;
}

/******************************************************************************/
SOAP_FMAC1
const char*
SOAP_FMAC2
soap_long2s(struct soap *soap, long n)
{ sprintf(soap->tmpbuf, "%ld", n);
  return soap->tmpbuf;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_outlong(struct soap *soap, const char *tag, int id, const long *p, const char *type, int n)
{ if (soap_element_begin_out(soap, tag, soap_embedded_id(soap, id, p, n), type) || soap_send(soap, soap_long2s(soap, *p)))
    return soap->error;
  return soap_element_end_out(soap, tag);
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_s2long(struct soap *soap, const char *s, long *p)
{ if (s)
  { char *r;
    *p = strtol(s, &r, 10);
    if (*r)
      return soap->error = SOAP_TYPE_MISMATCH;
  }
  return SOAP_OK;
}

/******************************************************************************/
SOAP_FMAC1
long *
SOAP_FMAC2
soap_inlong(struct soap *soap, const char *tag, long *p, const char *type, int t)
{ if (soap_element_begin_in(soap, tag))
    return NULL;
  if (soap->null)
  { if (soap->mode & SOAP_XML_NIL)
    { soap->error = SOAP_NULL;
      return NULL;
    }
    return p;
  }
  if (*soap->type != '\0'
   && soap_match_tag(soap, soap->type, type)
   && soap_match_tag(soap, soap->type, "long")
   && soap_match_tag(soap, soap->type, "int")
   && soap_match_tag(soap, soap->type, "short")
   && soap_match_tag(soap, soap->type, "byte")
   && soap_match_tag(soap, soap->type, "unsignedLong")
   && soap_match_tag(soap, soap->type, "unsignedInt")
   && soap_match_tag(soap, soap->type, "unsignedShort")
   && soap_match_tag(soap, soap->type, "unsignedByte"))
  { soap->error = SOAP_TYPE_MISMATCH;
    soap_revert(soap);
    return NULL;
  }
  if (soap->body && !*soap->href)
  { p = (long*)soap_id_enter(soap, soap->id, p, t, sizeof(long), 0);
    if (!p || soap_s2long(soap, soap_value(soap), p))
      return NULL;
  }
  else
    p = (long*)soap_id_forward(soap, soap->href, p, t, sizeof(long));
  if (soap->body && soap_element_end_in(soap, tag))
    return NULL;
  return p;
}

/******************************************************************************/
SOAP_FMAC1
const char*
SOAP_FMAC2
soap_LONG642s(struct soap *soap, LONG64 n)
{ sprintf(soap->tmpbuf, SOAP_LONG_FORMAT, n);
  return soap->tmpbuf;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_outLONG64(struct soap *soap, const char *tag, int id, const LONG64 *p, const char *type, int n)
{ if (soap_element_begin_out(soap, tag, soap_embedded_id(soap, id, p, n), type) || soap_send(soap, soap_LONG642s(soap, *p)))
    return soap->error;
  return soap_element_end_out(soap, tag);
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_s2LONG64(struct soap *soap, const char *s, LONG64 *p)
{ if (s && sscanf(s, SOAP_LONG_FORMAT, p) != 1)
    return soap->error = SOAP_TYPE_MISMATCH;
  return SOAP_OK;
}

/******************************************************************************/
SOAP_FMAC1
LONG64 *
SOAP_FMAC2
soap_inLONG64(struct soap *soap, const char *tag, LONG64 *p, const char *type, int t)
{ if (soap_element_begin_in(soap, tag))
    return NULL;
  if (soap->null)
  { if (soap->mode & SOAP_XML_NIL)
    { soap->error = SOAP_NULL;
      return NULL;
    }
    return p;
  }
  if (*soap->type != '\0'
   && soap_match_tag(soap, soap->type, type)
   && soap_match_tag(soap, soap->type, "integer")
   && soap_match_tag(soap, soap->type, "positiveInteger")
   && soap_match_tag(soap, soap->type, "negativeInteger")
   && soap_match_tag(soap, soap->type, "nonPositiveInteger")
   && soap_match_tag(soap, soap->type, "nonNegativeInteger")
   && soap_match_tag(soap, soap->type, "long")
   && soap_match_tag(soap, soap->type, "int")
   && soap_match_tag(soap, soap->type, "short")
   && soap_match_tag(soap, soap->type, "byte")
   && soap_match_tag(soap, soap->type, "unsignedLong")
   && soap_match_tag(soap, soap->type, "unsignedInt")
   && soap_match_tag(soap, soap->type, "unsignedShort")
   && soap_match_tag(soap, soap->type, "unsignedByte"))
  { soap->error = SOAP_TYPE_MISMATCH;
    soap_revert(soap);
    return NULL;
  }
  if (soap->body && !*soap->href)
  { p = (LONG64*)soap_id_enter(soap, soap->id, p, t, sizeof(LONG64), 0);
    if (!p || soap_s2LONG64(soap, soap_value(soap), p))
      return NULL;
  }
  else
    p = (LONG64*)soap_id_forward(soap, soap->href, p, t, sizeof(LONG64));
  if (soap->body && soap_element_end_in(soap, tag))
    return NULL;
  return p;
}

/******************************************************************************/
SOAP_FMAC1
const char*
SOAP_FMAC2
soap_byte2s(struct soap *soap, char n)
{ return soap_int2s(soap, (int)n);
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_outbyte(struct soap *soap, const char *tag, int id, const char *p, const char *type, int n)
{ if (soap_element_begin_out(soap, tag, soap_embedded_id(soap, id, p, n), type) || soap_send(soap, soap_byte2s(soap, *p)))
    return soap->error;
  return soap_element_end_out(soap, tag);
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_s2byte(struct soap *soap, const char *s, char *p)
{ if (s)
  { char *r;
    *p = (char)strtol(s, &r, 10);
    if (*r)
      return soap->error = SOAP_TYPE_MISMATCH;
  }
  return SOAP_OK;
}

/******************************************************************************/
SOAP_FMAC1
char *
SOAP_FMAC2
soap_inbyte(struct soap *soap, const char *tag, char *p, const char *type, int t)
{ if (soap_element_begin_in(soap, tag))
    return NULL;
  if (soap->null)
  { if (soap->mode & SOAP_XML_NIL)
    { soap->error = SOAP_NULL;
      return NULL;
    }
    return p;
  }
  if (*soap->type != '\0'
   && soap_match_tag(soap, soap->type, type)
   && soap_match_tag(soap, soap->type, "byte")
   && soap_match_tag(soap, soap->type, "unsignedByte"))
  { soap->error = SOAP_TYPE_MISMATCH;
    soap_revert(soap);
    return NULL;
  }
  if (soap->body && !*soap->href)
  { p = (char*)soap_id_enter(soap, soap->id, p, t, sizeof(char), 0);
    if (!p || soap_s2byte(soap, soap_value(soap), p))
      return NULL;
  }
  else
    p = (char*)soap_id_forward(soap, soap->href, p, t, sizeof(char));
  if (soap->body && soap_element_end_in(soap, tag))
    return NULL;
  return p;
}

/******************************************************************************/
SOAP_FMAC1
const char*
SOAP_FMAC2
soap_short2s(struct soap *soap, short n)
{ return soap_int2s(soap, (int)n);
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_outshort(struct soap *soap, const char *tag, int id, const short *p, const char *type, int n)
{ if (soap_element_begin_out(soap, tag, soap_embedded_id(soap, id, p, n), type) || soap_send(soap, soap_short2s(soap, *p)))
    return soap->error;
  return soap_element_end_out(soap, tag);
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_s2short(struct soap *soap, const char *s, short *p)
{ if (s && sscanf(s, "%hd", p) != 1)
    return soap->error = SOAP_TYPE_MISMATCH;
  return SOAP_OK;
}

/******************************************************************************/
SOAP_FMAC1
short *
SOAP_FMAC2
soap_inshort(struct soap *soap, const char *tag, short *p, const char *type, int t)
{ if (soap_element_begin_in(soap, tag))
    return NULL;
  if (soap->null)
  { if (soap->mode & SOAP_XML_NIL)
    { soap->error = SOAP_NULL;
      return NULL;
    }
    return p;
  }
  if (*soap->type != '\0'
   && soap_match_tag(soap, soap->type, type)
   && soap_match_tag(soap, soap->type, "short")
   && soap_match_tag(soap, soap->type, "byte")
   && soap_match_tag(soap, soap->type, "unsignedShort")
   && soap_match_tag(soap, soap->type, "unsignedByte"))
  { soap->error = SOAP_TYPE_MISMATCH;
    soap_revert(soap);
    return NULL;
  }
  if (soap->body && !*soap->href)
  { p = (short*)soap_id_enter(soap, soap->id, p, t, sizeof(short), 0);
    if (!p || soap_s2short(soap, soap_value(soap), p))
      return NULL;
  }
  else
    p = (short*)soap_id_forward(soap, soap->href, p, t, sizeof(short));
  if (soap->body && soap_element_end_in(soap, tag))
    return NULL;
  return p;
}

/******************************************************************************/
SOAP_FMAC1
const char*
SOAP_FMAC2
soap_float2s(struct soap *soap, float n)
{ const char *s;
  if (isnan(n))
    s = "NaN";
  else if (n >= FLT_PINFTY)
    s = "INF";
  else if (n <= FLT_NINFTY)
    s = "-INF";
  else
  { sprintf(soap->tmpbuf, soap->float_format, n);
    s = soap->tmpbuf;
  }
  return s;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_outfloat(struct soap *soap, const char *tag, int id, const float *p, const char *type, int n)
{ if (soap_element_begin_out(soap, tag, soap_embedded_id(soap, id, p, n), type) || soap_send(soap, soap_float2s(soap, *p)))
    return soap->error;
  return soap_element_end_out(soap, tag);
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_s2float(struct soap *soap, const char *s, float *p)
{ if (s)
  { if (!soap_tag_cmp(s, "INF"))
      *p = FLT_PINFTY;
    else if (!soap_tag_cmp(s, "+INF"))
      *p = FLT_PINFTY;
    else if (!soap_tag_cmp(s, "-INF"))
      *p = FLT_NINFTY;
    else if (!soap_tag_cmp(s, "NaN"))
      *p = FLT_NAN;
    else
    { char *r;
      *p = (float)strtod(s, &r);
      if (*r && sscanf(s, soap->float_format, p) != 1)
        return soap->error = SOAP_TYPE_MISMATCH;
    }
  }
  return SOAP_OK;
}

/******************************************************************************/
SOAP_FMAC1
float *
SOAP_FMAC2
soap_infloat(struct soap *soap, const char *tag, float *p, const char *type, int t)
{ if (soap_element_begin_in(soap, tag))
    return NULL;
  if (soap->null)
  { if (soap->mode & SOAP_XML_NIL)
    { soap->error = SOAP_NULL;
      return NULL;
    }
    return p;
  }
  if (*soap->type != '\0'
   && soap_match_tag(soap, soap->type, type)
   && soap_match_tag(soap, soap->type, "float")
   && soap_match_tag(soap, soap->type, "decimal")
   && soap_match_tag(soap, soap->type, "integer")
   && soap_match_tag(soap, soap->type, "positiveInteger")
   && soap_match_tag(soap, soap->type, "negativeInteger")
   && soap_match_tag(soap, soap->type, "nonPositiveInteger")
   && soap_match_tag(soap, soap->type, "nonNegativeInteger")
   && soap_match_tag(soap, soap->type, "long")
   && soap_match_tag(soap, soap->type, "int")
   && soap_match_tag(soap, soap->type, "short")
   && soap_match_tag(soap, soap->type, "byte")
   && soap_match_tag(soap, soap->type, "unsignedLong")
   && soap_match_tag(soap, soap->type, "unsignedInt")
   && soap_match_tag(soap, soap->type, "unsignedShort")
   && soap_match_tag(soap, soap->type, "unsignedByte"))
  { soap->error = SOAP_TYPE_MISMATCH;
    soap_revert(soap);
    return NULL;
  }
  if (soap->body && !*soap->href)
  { p = (float*)soap_id_enter(soap, soap->id, p, t, sizeof(float), 0);
    if (!p || soap_s2float(soap, soap_value(soap), p))
      return NULL;
  }
  else
    p = (float*)soap_id_forward(soap, soap->href, p, t, sizeof(float));
  if (soap->body && soap_element_end_in(soap, tag))
    return NULL;
  return p;
}

/******************************************************************************/
SOAP_FMAC1
const char*
SOAP_FMAC2
soap_double2s(struct soap *soap, double n)
{ const char *s;
  if (isnan(n))
    s = "NaN";
  else if (n >= DBL_PINFTY)
    s = "INF";
  else if (n <= DBL_NINFTY)
    s = "-INF";
  else
  { sprintf(soap->tmpbuf, soap->double_format, n);
    s = soap->tmpbuf;
  }
  return s;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_outdouble(struct soap *soap, const char *tag, int id, const double *p, const char *type, int n)
{ if (soap_element_begin_out(soap, tag, soap_embedded_id(soap, id, p, n), type) || soap_send(soap, soap_double2s(soap, *p)))
    return soap->error;
  return soap_element_end_out(soap, tag);
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_s2double(struct soap *soap, const char *s, double *p)
{ if (s)
  { if (!soap_tag_cmp(s, "INF"))
      *p = DBL_PINFTY;
    else if (!soap_tag_cmp(s, "+INF"))
      *p = DBL_PINFTY;
    else if (!soap_tag_cmp(s, "-INF"))
      *p = DBL_NINFTY;
    else if (!soap_tag_cmp(s, "NaN"))
      *p = DBL_NAN;
    else
    { char *r;
      *p = (float)strtod(s, &r);
      if (*r && sscanf(s, soap->double_format, p) != 1)
        return soap->error = SOAP_TYPE_MISMATCH;
    }
  }
  return SOAP_OK;
}

/******************************************************************************/
SOAP_FMAC1
double *
SOAP_FMAC2
soap_indouble(struct soap *soap, const char *tag, double *p, const char *type, int t)
{ if (soap_element_begin_in(soap, tag))
    return NULL;
  if (soap->null)
  { if (soap->mode & SOAP_XML_NIL)
    { soap->error = SOAP_NULL;
      return NULL;
    }
    return p;
  }
  if (*soap->type != '\0'
   && soap_match_tag(soap, soap->type, type)
   && soap_match_tag(soap, soap->type, "float")
   && soap_match_tag(soap, soap->type, "double")
   && soap_match_tag(soap, soap->type, "decimal")
   && soap_match_tag(soap, soap->type, "integer")
   && soap_match_tag(soap, soap->type, "positiveInteger")
   && soap_match_tag(soap, soap->type, "negativeInteger")
   && soap_match_tag(soap, soap->type, "nonPositiveInteger")
   && soap_match_tag(soap, soap->type, "nonNegativeInteger")
   && soap_match_tag(soap, soap->type, "long")
   && soap_match_tag(soap, soap->type, "int")
   && soap_match_tag(soap, soap->type, "short")
   && soap_match_tag(soap, soap->type, "byte")
   && soap_match_tag(soap, soap->type, "unsignedLong")
   && soap_match_tag(soap, soap->type, "unsignedInt")
   && soap_match_tag(soap, soap->type, "unsignedShort")
   && soap_match_tag(soap, soap->type, "unsignedByte"))
  { soap->error = SOAP_TYPE_MISMATCH;
    soap_revert(soap);
    return NULL;
  }
  if (soap->body && !*soap->href)
  { p = (double*)soap_id_enter(soap, soap->id, p, t, sizeof(double), 0);
    if (!p || soap_s2double(soap, soap_value(soap), p))
      return NULL;
  }
  else
    p = (double*)soap_id_forward(soap, soap->href, p, t, sizeof(double));
  if (soap->body && soap_element_end_in(soap, tag))
    return NULL;
  return p;
}

/******************************************************************************/
SOAP_FMAC1
const char*
SOAP_FMAC2
soap_unsignedByte2s(struct soap *soap, unsigned char n)
{ return soap_unsignedInt2s(soap, (unsigned int)n);
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_outunsignedByte(struct soap *soap, const char *tag, int id, const unsigned char *p, const char *type, int n)
{ if (soap_element_begin_out(soap, tag, soap_embedded_id(soap, id, p, n), type) || soap_send(soap, soap_unsignedByte2s(soap, *p)))
    return soap->error;
  return soap_element_end_out(soap, tag);
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_s2unsignedByte(struct soap *soap, const char *s, unsigned char *p)
{ if (s)
  { char *r;
    *p = (unsigned char)strtol(s, &r, 10);
    if (*r)
      return soap->error = SOAP_TYPE_MISMATCH;
  }
  return SOAP_OK;
}

/******************************************************************************/
SOAP_FMAC1
unsigned char *
SOAP_FMAC2
soap_inunsignedByte(struct soap *soap, const char *tag, unsigned char *p, const char *type, int t)
{ if (soap_element_begin_in(soap, tag))
    return NULL;
  if (soap->null)
  { if (soap->mode & SOAP_XML_NIL)
    { soap->error = SOAP_NULL;
      return NULL;
    }
    return p;
  }
  if (*soap->type != '\0'
   && soap_match_tag(soap, soap->type, type)
   && soap_match_tag(soap, soap->type, "unsignedByte"))
  { soap->error = SOAP_TYPE_MISMATCH;
    soap_revert(soap);
    return NULL;
  }
  if (soap->body && !*soap->href)
  { p = (unsigned char*)soap_id_enter(soap, soap->id, p, t, sizeof(unsigned char), 0);
    if (!p || soap_s2unsignedByte(soap, soap_value(soap), p))
      return NULL;
  }
  else
    p = (unsigned char*)soap_id_forward(soap, soap->href, p, t, sizeof(unsigned char));
  if (soap->body && soap_element_end_in(soap, tag))
    return NULL;
  return p;
}

/******************************************************************************/
SOAP_FMAC1
const char*
SOAP_FMAC2
soap_unsignedShort2s(struct soap *soap, unsigned short n)
{ return soap_unsignedInt2s(soap, (unsigned int)n);
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_outunsignedShort(struct soap *soap, const char *tag, int id, const unsigned short *p, const char *type, int n)
{ if (soap_element_begin_out(soap, tag, soap_embedded_id(soap, id, p, n), type) || soap_send(soap, soap_unsignedShort2s(soap, *p)))
    return soap->error;
  return soap_element_end_out(soap, tag);
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_s2unsignedShort(struct soap *soap, const char *s, unsigned short *p)
{ if (s && sscanf(s, "%hu", p) != 1)
    return soap->error = SOAP_TYPE_MISMATCH;
  return SOAP_OK;
}

/******************************************************************************/
SOAP_FMAC1
unsigned short *
SOAP_FMAC2
soap_inunsignedShort(struct soap *soap, const char *tag, unsigned short *p, const char *type, int t)
{ if (soap_element_begin_in(soap, tag))
    return NULL;
  if (soap->null)
  { if (soap->mode & SOAP_XML_NIL)
    { soap->error = SOAP_NULL;
      return NULL;
    }
    return p;
  }
  if (*soap->type != '\0'
   && soap_match_tag(soap, soap->type, type)
   && soap_match_tag(soap, soap->type, "unsignedShort")
   && soap_match_tag(soap, soap->type, "unsignedByte"))
  { soap->error = SOAP_TYPE_MISMATCH;
    soap_revert(soap);
    return NULL;
  }
  if (soap->body && !*soap->href)
  { p = (unsigned short*)soap_id_enter(soap, soap->id, p, t, sizeof(unsigned short), 0);
    if (!p || soap_s2unsignedShort(soap, soap_value(soap), p))
      return NULL;
  }
  else
    p = (unsigned short*)soap_id_forward(soap, soap->href, p, t, sizeof(unsigned short));
  if (soap->body && soap_element_end_in(soap, tag))
    return NULL;
  return p;
}

/******************************************************************************/
SOAP_FMAC1
const char*
SOAP_FMAC2
soap_unsignedInt2s(struct soap *soap, unsigned int n)
{ sprintf(soap->tmpbuf, "%u", n);
  return soap->tmpbuf;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_outunsignedInt(struct soap *soap, const char *tag, int id, const unsigned int *p, const char *type, int n)
{ if (soap_element_begin_out(soap, tag, soap_embedded_id(soap, id, p, n), type) || soap_send(soap, soap_unsignedInt2s(soap, *p)))
    return soap->error;
  return soap_element_end_out(soap, tag);
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_s2unsignedInt(struct soap *soap, const char *s, unsigned int *p)
{ if (s)
  { char *r;
    *p = strtoul(s, &r, 10);
    if (*r)
      return soap->error = SOAP_TYPE_MISMATCH;
  }
  return SOAP_OK;
}

/******************************************************************************/
SOAP_FMAC1
unsigned int *
SOAP_FMAC2
soap_inunsignedInt(struct soap *soap, const char *tag, unsigned int *p, const char *type, int t)
{ if (soap_element_begin_in(soap, tag))
    return NULL;
  if (soap->null)
  { if (soap->mode & SOAP_XML_NIL)
    { soap->error = SOAP_NULL;
      return NULL;
    }
    return p;
  }
  if (*soap->type != '\0'
   && soap_match_tag(soap, soap->type, type)
   && soap_match_tag(soap, soap->type, "unsignedInt")
   && soap_match_tag(soap, soap->type, "unsignedShort")
   && soap_match_tag(soap, soap->type, "unsignedByte"))
  { soap->error = SOAP_TYPE_MISMATCH;
    soap_revert(soap);
    return NULL;
  }
  if (soap->body && !*soap->href)
  { p = (unsigned int*)soap_id_enter(soap, soap->id, p, t, sizeof(unsigned int), 0);
    if (!p || soap_s2unsignedInt(soap, soap_value(soap), p))
      return NULL;
  }
  else
    p = (unsigned int*)soap_id_forward(soap, soap->href, p, t, sizeof(unsigned int));
  if (soap->body && soap_element_end_in(soap, tag))
    return NULL;
  return p;
}

/******************************************************************************/
SOAP_FMAC1
const char*
SOAP_FMAC2
soap_unsignedLong2s(struct soap *soap, unsigned long n)
{ sprintf(soap->tmpbuf, "%lu", n);
  return soap->tmpbuf;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_outunsignedLong(struct soap *soap, const char *tag, int id, const unsigned long *p, const char *type, int n)
{ if (soap_element_begin_out(soap, tag, soap_embedded_id(soap, id, p, n), type) || soap_send(soap, soap_unsignedLong2s(soap, *p)))
    return soap->error;
  return soap_element_end_out(soap, tag);
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_s2unsignedLong(struct soap *soap, const char *s, unsigned long *p)
{ if (s)
  { char *r;
    *p = strtoul(s, &r, 10);
    if (*r)
      return soap->error = SOAP_TYPE_MISMATCH;
  }
  return SOAP_OK;
}

/******************************************************************************/
SOAP_FMAC1
unsigned long *
SOAP_FMAC2
soap_inunsignedLong(struct soap *soap, const char *tag, unsigned long *p, const char *type, int t)
{ if (soap_element_begin_in(soap, tag))
    return NULL;
  if (soap->null)
  { if (soap->mode & SOAP_XML_NIL)
    { soap->error = SOAP_NULL;
      return NULL;
    }
    return p;
  }
  if (*soap->type != '\0'
   && soap_match_tag(soap, soap->type, type)
   && soap_match_tag(soap, soap->type, "unsignedLong")
   && soap_match_tag(soap, soap->type, "unsignedInt")
   && soap_match_tag(soap, soap->type, "unsignedShort")
   && soap_match_tag(soap, soap->type, "unsignedByte"))
  { soap->error = SOAP_TYPE_MISMATCH;
    soap_revert(soap);
    return NULL;
  }
  if (soap->body && !*soap->href)
  { p = (unsigned long*)soap_id_enter(soap, soap->id, p, t, sizeof(unsigned long), 0);
    if (!p || soap_s2unsignedLong(soap, soap_value(soap), p))
      return NULL;
  }
  else
    p = (unsigned long*)soap_id_forward(soap, soap->href, p, t, sizeof(unsigned long));
  if (soap->body && soap_element_end_in(soap, tag))
    return NULL;
  return p;
}

/******************************************************************************/
SOAP_FMAC1
const char*
SOAP_FMAC2
soap_ULONG642s(struct soap *soap, ULONG64 n)
{ sprintf(soap->tmpbuf, SOAP_ULONG_FORMAT, n);
  return soap->tmpbuf;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_outULONG64(struct soap *soap, const char *tag, int id, const ULONG64 *p, const char *type, int n)
{ if (soap_element_begin_out(soap, tag, soap_embedded_id(soap, id, p, n), type) || soap_send(soap, soap_ULONG642s(soap, *p)))
    return soap->error;
  return soap_element_end_out(soap, tag);
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_s2ULONG64(struct soap *soap, const char *s, ULONG64 *p)
{ if (s && sscanf(s, SOAP_ULONG_FORMAT, p) != 1)
    return soap->error = SOAP_TYPE_MISMATCH;
  return SOAP_OK;
}

/******************************************************************************/
SOAP_FMAC1
ULONG64 *
SOAP_FMAC2
soap_inULONG64(struct soap *soap, const char *tag, ULONG64 *p, const char *type, int t)
{ if (soap_element_begin_in(soap, tag))
    return NULL;
  if (soap->null)
  { if (soap->mode & SOAP_XML_NIL)
    { soap->error = SOAP_NULL;
      return NULL;
    }
    return p;
  }
  if (*soap->type != '\0'
   && soap_match_tag(soap, soap->type, type)
   && soap_match_tag(soap, soap->type, "positiveInteger")
   && soap_match_tag(soap, soap->type, "nonNegativeInteger")
   && soap_match_tag(soap, soap->type, "unsignedLong")
   && soap_match_tag(soap, soap->type, "unsignedInt")
   && soap_match_tag(soap, soap->type, "unsignedShort")
   && soap_match_tag(soap, soap->type, "unsignedByte"))
  { soap->error = SOAP_TYPE_MISMATCH;
    soap_revert(soap);
    return NULL;
  }
  if (soap->body && !*soap->href)
  { p = (ULONG64*)soap_id_enter(soap, soap->id, p, t, sizeof(ULONG64), 0);
    if (!p || soap_s2ULONG64(soap, soap_value(soap), p))
      return NULL;
  }
  else
    p = (ULONG64*)soap_id_forward(soap, soap->href, p, t, sizeof(ULONG64));
  if (soap->body && soap_element_end_in(soap, tag))
    return NULL;
  return p;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_s2string(struct soap *soap, const char *s, char **t)
{ if (s)
  { *t = (char*)soap_malloc(soap, strlen(s) + 1);
    if (!*t)
      return soap->error = SOAP_EOM;
    strcpy(*t, s);
  }
  return SOAP_OK;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_s2QName(struct soap *soap, const char *s, char **t)
{ if (s)
  { const char *p = strchr(s, ':');
    if (p)
    { struct soap_nlist *np = soap->nlist;
      int n = p - s;
      while (np && (np->id[n] || strncmp(np->id, s, n)))
        np = np->next;
      if (np)
      { if (np->index >= 0 && soap->local_namespaces)
        { const char *q = soap->local_namespaces[np->index].id;
          if (q)
          { *t = (char*)soap_malloc(soap, strlen(p) + strlen(q) + 1);
            strcpy(*t, q);
            strcat(*t, p);
            return SOAP_OK;
          }
        }
        if (np->ns)
        { *t = (char*)soap_malloc(soap, strlen(p) + strlen(np->ns) + 3);
	  sprintf(*t, "\"%s\"%s", np->ns, p);
          return SOAP_OK;
        }
      }
      DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Namespace prefix of '%s' not defined\n", s));
      return soap->error = SOAP_SYNTAX_ERROR; 
    }
    *t = (char*)soap_malloc(soap, strlen(s) + 1);
    if (!*t)
      return soap->error = SOAP_EOM;
    strcpy(*t, s);
  }
  return SOAP_OK;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_outstring(struct soap *soap, const char *tag, int id, char *const*p, const char *type, int n) 
{ if (!*p)
  { if (soap_element_null(soap, tag, id, type))
      return soap->error;
  }
  else
  { struct soap_plist *pp;
    int i = soap_pointer_lookup(soap, *p, n, &pp);
    if (id > 0)
    { if (i)
      { if (soap_element_begin_out(soap, tag, id, type) || soap_string_out(soap, *p) || soap_element_end_out(soap, tag))
          return soap->error;
        soap_set_embedded(soap, pp);
      }
      else
      { i = soap_pointer_enter(soap, *p, n, &pp);
        if (soap_element_begin_out(soap, tag, id, type) || soap_string_out(soap, *p) || soap_element_end_out(soap, tag))
          return soap->error;
        if (soap->mode & SOAP_IO_LENGTH)
          pp->mark1 = 0;
        else
          pp->mark2 = 0;
      }
    }
    else if (i)
    { if (soap_is_embedded(soap, pp))
      { if (soap_element_ref(soap, tag, 0, i))
          return soap->error;
      }
      else if (soap_is_single(soap, pp))
      { if (soap_element_begin_out(soap, tag, 0, type) || soap_string_out(soap, *p) || soap_element_end_out(soap, tag))
          return soap->error;
      }
      else
      { if (soap_element_begin_out(soap, tag, i, type) || soap_string_out(soap, *p) || soap_element_end_out(soap, tag))
          return soap->error;
        soap_set_embedded(soap, pp);
      }
    }
    else
    { soap_pointer_enter(soap, *p, n, &pp);
      if (soap_element_begin_out(soap, tag, id, type) || soap_string_out(soap, *p) || soap_element_end_out(soap, tag))
        return soap->error;
      if (soap->mode & SOAP_IO_LENGTH)
        pp->mark1 = 0;
      else
        pp->mark2 = 0;
    }
  }
  return SOAP_OK;
}

/******************************************************************************/
SOAP_FMAC1
char **
SOAP_FMAC2
soap_instring(struct soap *soap, const char *tag, char **p, const char *type, int t, int flag)
{ if (soap_element_begin_in(soap, tag))
    return NULL;
  if (soap->null)
  { p = (char**)soap_id_enter(soap, soap->id, p, t, sizeof(char**), 0);
    if (p)
      *p = NULL;
  }
  else if (soap->body && !*soap->href)
  { if (soap_match_tag(soap, soap->type, "PointerTostring") == 0)
    { p = (char**)soap_id_enter(soap, soap->id, p, t, sizeof(char**), 0);
      p = (char**)soap_instring(soap, "string", p, type, t, flag);
    }
    else
    { if (!p)
        if ((p = (char**)soap_id_enter(soap, "", p, t, sizeof(char**), 0)) == NULL)
          return NULL;
      *p = (char*)soap_id_enter(soap, soap->id, soap_string_in(soap, flag), t, 0, 0);
    }
  }
  else
    p = (char**)soap_id_lookup(soap, soap->href, (void**)p, t, sizeof(char*), 0);
  if (soap->body && soap_element_end_in(soap, tag))
    return NULL;
  return p;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_outwstring(struct soap *soap, const char *tag, int id, wchar_t *const*p, const char *type, int n) 
{ if (!*p)
  { if (soap_element_null(soap, tag, id, type))
      return soap->error;
  }
  else
  { struct soap_plist *pp;
    int i = soap_pointer_lookup(soap, *p, n, &pp);
    if (id > 0)
    { if (i)
      { if (soap_element_begin_out(soap, tag, id, type) || soap_wstring_out(soap, *p) || soap_element_end_out(soap, tag))
          return soap->error;
        soap_set_embedded(soap, pp);
      }
      else
      { i = soap_pointer_enter(soap, *p, n, &pp);
        if (soap_element_begin_out(soap, tag, id, type) || soap_wstring_out(soap, *p) || soap_element_end_out(soap, tag))
          return soap->error;
        if (soap->mode & SOAP_IO_LENGTH)
          pp->mark1 = 0;
        else
          pp->mark2 = 0;
      }
    }
    else if (i)
    { if (soap_is_embedded(soap, pp))
      { if (soap_element_ref(soap, tag, 0, i))
          return soap->error;
      }
      else if (soap_is_single(soap, pp))
      { if (soap_element_begin_out(soap, tag, 0, type) || soap_wstring_out(soap, *p) || soap_element_end_out(soap, tag))
          return soap->error;
      }
      else
      { if (soap_element_begin_out(soap, tag, i, type) || soap_wstring_out(soap, *p) || soap_element_end_out(soap, tag))
          return soap->error;
        soap_set_embedded(soap, pp);
      }
    }
    else
    { if (soap_element_begin_out(soap, tag, id, type) || soap_wstring_out(soap, *p) || soap_element_end_out(soap, tag))
        return soap->error;
      if (soap->mode & SOAP_IO_LENGTH)
        pp->mark1 = 0;
      else
        pp->mark2 = 0;
    }
  }
  return SOAP_OK;
}

/******************************************************************************/
SOAP_FMAC1
wchar_t **
SOAP_FMAC2
soap_inwstring(struct soap *soap, const char *tag, wchar_t **p, const char *type, int t)
{ if (soap_element_begin_in(soap, tag))
    return NULL;
  if (soap->null)
  { p = (wchar_t**)soap_id_enter(soap, soap->id, p, t, sizeof(wchar_t**), 0);
    if (p)
      *p = NULL;
  }
  else if (soap->body && !*soap->href)
  { if (soap_match_tag(soap, soap->type, "PointerTostring") == 0)
      p = (wchar_t**)soap_inwstring(soap, "string", (wchar_t**)soap_id_enter(soap, soap->id, p, t, sizeof(wchar_t**), 0), type, t);
    else
    { if (!p)
        if (!(p = (wchar_t**)soap_id_enter(soap, "", p, t, sizeof(wchar_t**), 0)))
          return NULL;
      *p = (wchar_t*)soap_id_enter(soap, soap->id, soap_wstring_in(soap, 1), t, 0, 0);
    }
  }
  else
    p = (wchar_t**)soap_id_lookup(soap, soap->href, (void**)p, t, sizeof(wchar_t*), 0);
  if (soap->body && soap_element_end_in(soap, tag))
    return NULL;
  return p;
}
/******************************************************************************/

#ifndef MAC_CARBON
#ifndef UNDER_CE
/******************************************************************************/
static time_t
soap_timegm(struct tm *T)
{
#if defined(HAVE_TIMEGM)
  return timegm(T);
#elif defined(HAVE_FTIME)
  struct timeb t;
  ftime(&t);
  T->tm_min -= t.timezone;
  T->tm_isdst = -1;
  return mktime(T);
#else
#warning "time_t (de)serialization is not MT safe on this platform"
  time_t t;
  char *tz = getenv("TZ");
  putenv("TZ=UTC");
  tzset();
  t = mktime(T);
  if (tz)
  { char tmp[16];
    strcpy(tmp, "TZ=");
    strncat(tmp, tz, 12);
    tmp[15] = '\0';
    putenv(tmp);
  }
  else
    putenv("TZ=");
  tzset();
  return t;
#endif
}

/******************************************************************************/
SOAP_FMAC1
const char*
SOAP_FMAC2
soap_dateTime2s(struct soap *soap, time_t n)
{
#if defined(HAVE_GMTIME_R)
  struct tm T;
  strftime(soap->tmpbuf, sizeof(soap->tmpbuf), "%Y-%m-%dT%H:%M:%SZ", gmtime_r(&n, &T));
#elif defined(HAVE_GMTIME)
  strftime(soap->tmpbuf, sizeof(soap->tmpbuf), "%Y-%m-%dT%H:%M:%SZ", gmtime(&n));
#elif defined(HAVE_FTIME)
  struct timeb t;
#if defined(HAVE_LOCALTIME_R)
  struct tm T;
  ftime(&t);
  strftime(soap->tmpbuf, sizeof(soap->tmpbuf), "%Y-%m-%dT%H:%M:%S", localtime_r(&n, &T));
  sprintf(soap->tmpbuf + strlen(soap->tmpbuf), "%+03d:%02d", -t.timezone/60, abs(t.timezone)%60);
#else
  ftime(&t);
  strftime(soap->tmpbuf, sizeof(soap->tmpbuf), "%Y-%m-%dT%H:%M:%S", localtime(&n));
  sprintf(soap->tmpbuf + strlen(soap->tmpbuf), "%+03d:%02d", -t.timezone/60, abs(t.timezone)%60);
#endif
#elif defined(HAVE_LOCALTIME_R)
  struct tm T;
  strftime(soap->tmpbuf, sizeof(soap->tmpbuf), "%Y-%m-%dT%H:%M:%S", localtime_r(&n, &T));
#else
  strftime(soap->tmpbuf, sizeof(soap->tmpbuf), "%Y-%m-%dT%H:%M:%S", localtime(&n));
#endif
  return soap->tmpbuf;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_outdateTime(struct soap *soap, const char *tag, int id, const time_t *p, const char *type, int n)
{ if (soap_element_begin_out(soap, tag, soap_embedded_id(soap, id, p, n), type) || soap_send(soap, soap_dateTime2s(soap, *p)))
    return soap->error;
  return soap_element_end_out(soap, tag);
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_s2dateTime(struct soap *soap, const char *s, time_t *p)
{ if (s)
  { struct tm T;
    char zone[16];
    memset(&T, 0, sizeof(struct tm));
    zone[sizeof(zone)-1] = '\0';
    sscanf(s, "%d-%d-%dT%d:%d:%d%15s", &T.tm_year, &T.tm_mon, &T.tm_mday, &T.tm_hour, &T.tm_min, &T.tm_sec, zone);
    if (T.tm_year <= 1901 && T.tm_year != 1)
      *p = -1;
    else if (T.tm_year >= 2038)
      *p = -1;
    else
    { if (T.tm_year == 1)
        T.tm_year = 70;
      else
        T.tm_year -= 1900;
      T.tm_mon--;
      if (*zone)
      { if (*zone == '.')
        { for (s = zone+1; *s; s++)
            if (*s < '0' || *s > '9')
              break;
        }
        else
          s = zone;
        if (*s != 'Z')
        { int h = 0, m = 0;
          sscanf(s, "%d:%d", &h, &m);
          T.tm_hour -= h;
          if (h >= 0)
            T.tm_min -= m;
          else
            T.tm_min += m;
        }
        *p = soap_timegm(&T);
      }
      else
        *p = mktime(&T); /* no time zone: suppose it is localtime? */
    }
  }
  return SOAP_OK;
}

/******************************************************************************/
SOAP_FMAC1
time_t *
SOAP_FMAC2
soap_indateTime(struct soap *soap, const char *tag, time_t *p, const char * type, int t)
{ if (soap_element_begin_in(soap, tag))
    return NULL;
  if (soap->null)
  { if (soap->mode & SOAP_XML_NIL)
    { soap->error = SOAP_NULL;
      return NULL;
    }
    return p;
  }
  if (*soap->type != '\0'
   && soap_match_tag(soap, soap->type, type)
   && soap_match_tag(soap, soap->type, "dateTime"))
  { soap->error = SOAP_TYPE_MISMATCH;
    soap_revert(soap);
    return NULL;
  }
  if (soap->body && !*soap->href)
  { p = (time_t*)soap_id_enter(soap, soap->id, p, t, sizeof(time_t), 0);
    if (!p || soap_s2dateTime(soap, soap_value(soap), p))
      return NULL;
  }
  else
    p = (time_t*)soap_id_forward(soap, soap->href, p, t, sizeof(time_t));
  if (soap->body && soap_element_end_in(soap, tag))
    return NULL;
  return p;
}
/******************************************************************************/
#endif
#endif

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_outliteral(struct soap *soap, const char *tag, char *const*p)
{ int i;
  const char *t;
  if ((t = strchr(tag, ':')))
  { strncpy(soap->tmpbuf, tag, t-tag);
    soap->tmpbuf[t-tag] = '\0';
    for (i = 0; soap->local_namespaces[i].id; i++)
      if (!strcmp(soap->tmpbuf, soap->local_namespaces[i].id))
        break;
    sprintf(soap->tmpbuf, "<%s xmlns=\"%s\">", t+1, soap->local_namespaces[i].ns ? soap->local_namespaces[i].ns : "");
  }
  else
    sprintf(soap->tmpbuf, "<%s>", tag);
  if (soap_send(soap, soap->tmpbuf))
    return soap->error;
  if (p && *p)
  { if (soap_send(soap, *p))
      return soap->error;
  }
  if (t)
    sprintf(soap->tmpbuf, "</%s>", t+1);
  else
    sprintf(soap->tmpbuf, "</%s>", tag);
  return soap_send(soap, soap->tmpbuf);
}

/******************************************************************************/
SOAP_FMAC1
char **
SOAP_FMAC2
soap_inliteral(struct soap *soap, const char *tag, char **p)
{ if (soap_element_begin_in(soap, tag))
    return NULL;
  if (!p)
    if (!(p = (char**)soap_malloc(soap, sizeof(char*))))
      return NULL;
  if (soap->null)
    *p = NULL;
  else if (soap->body)
    *p = soap_string_in(soap, 0);
  else
    *p = NULL;
  if (soap->body && soap_element_end_in(soap, tag))
    return NULL;
  return p;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_outwliteral(struct soap *soap, const char *tag, wchar_t *const*p)
{ int i;
  const char *t;
  wchar_t c;
  const wchar_t *s;
  if ((t = strchr(tag, ':')))
  { strncpy(soap->tmpbuf, tag, t-tag);
    soap->tmpbuf[t-tag] = '\0';
    for (i = 0; soap->local_namespaces[i].id; i++)
      if (!strcmp(soap->tmpbuf, soap->local_namespaces[i].id))
        break;
    sprintf(soap->tmpbuf, "<%s xmlns=\"%s\">", t+1, soap->local_namespaces[i].ns ? soap->local_namespaces[i].ns : "");
  }
  else
    sprintf(soap->tmpbuf, "<%s>", tag);
  if (soap_send(soap, soap->tmpbuf))
    return soap->error;
  if (p)
  { s = *p;
    while ((c = *s++))
      if (soap_pututf8(soap, c))
        return soap->error;
  }
  if (t)
    sprintf(soap->tmpbuf,"</%s>", t+1);
  else
    sprintf(soap->tmpbuf,"</%s>", tag);
  return soap_send(soap, soap->tmpbuf);
}

/******************************************************************************/
SOAP_FMAC1
wchar_t **
SOAP_FMAC2
soap_inwliteral(struct soap *soap, const char *tag, wchar_t **p)
{ if (soap_element_begin_in(soap, tag))
    return NULL;
  if (!p)
    if (!(p = (wchar_t**)soap_malloc(soap, sizeof(wchar_t*))))
      return NULL;
  if (soap->null)
    *p = NULL;
  else if (soap->body)
    *p = soap_wstring_in(soap, 0);
  else
    *p = NULL;
  if (soap->body && soap_element_end_in(soap, tag))
    return NULL;
  return p;
}

/******************************************************************************/
SOAP_FMAC1
char *
SOAP_FMAC2
soap_value(struct soap *soap)
{ size_t i;
  wchar c = 0;
  char *s;
  s = soap->tmpbuf;
  for (i = 0; i < sizeof(soap->tmpbuf)-1; i++)
  { c = soap_get(soap);
    if (c == TT || c == EOF || blank(c))
      break;
    *s++ = (char)c;
  }
  if (c == EOF || c == TT)
    soap_unget(soap, c);
  *s = '\0';
  return soap->tmpbuf;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_getline(struct soap *soap, char *s, int len)
{ int i = len;
  wchar c = 0;
  for (;;)
  { while (--i > 0)
    { c = soap_getchar(soap);
      if (c == '\r' || c == '\n' || c == EOF)
        break;
      *s++ = (char)c;
    }
    *s = '\0';
    while (c != '\n' && c != EOF)
      c = soap_getchar(soap);
    if (c == EOF)
      return SOAP_EOF;
    if (i+1 == len) /* empty line: end of HTTP header */
      break;
    c = soap_unget(soap, soap_getchar(soap));
    if (c != ' ' && c != '\t') /* HTTP line continuation? */
      break;
  }
  return SOAP_OK;
}

/******************************************************************************/
static size_t
soap_begin_dime(struct soap *soap)
{ if (soap->mode & SOAP_ENC_DIME)
  { size_t count;
    sprintf(soap->id, soap->dime_id_format, 0);
    soap->dime_id = soap->id;
    if (soap->local_namespaces)
    { if (soap->local_namespaces[0].out)
        soap->dime_type = (char*)soap->local_namespaces[0].out;
      else
        soap->dime_type = (char*)soap->local_namespaces[0].ns;
    }
    soap->dime_options = NULL;
    count = soap->dime_size + 12 + ((soap->count+3)&-4) + ((strlen(soap->dime_id)+3)&-4) + ((strlen(soap->dime_type)+3)&-4);
    soap->dime_size = soap->count;
    if (soap->dime_count)
      soap->dime_flags = SOAP_DIME_MB | SOAP_DIME_ABSURI;
    else
      soap->dime_flags = SOAP_DIME_MB | SOAP_DIME_ME | SOAP_DIME_ABSURI;
    return count;
  }
  return soap->count;
}

/******************************************************************************/
static int
soap_putdimefield(struct soap *soap, const char *s, size_t n)
{ if (soap_send_raw(soap, s, n))
    return soap->error;
  return soap_send_raw(soap, "\0\0\0", -(int)n&3);
}

/******************************************************************************/
SOAP_FMAC1
char *
SOAP_FMAC2
soap_dime_option(struct soap *soap, unsigned short type, const char *option)
{ size_t n;
  char *s = NULL;
  if (option)
  { n = strlen(option);
    s = (char*)soap_malloc(soap, n + 5);
    if (s)
    { s[0] = type >> 8;
      s[1] = type & 0xFF;
      s[2] = n >> 8;
      s[3] = n & 0xFF;
      strcpy(s + 4, option);
    }
  }
  return s;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_putdimehdr(struct soap *soap)
{ unsigned char tmp[12];
  size_t optlen = 0, idlen = 0, typelen = 0;
  DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Put DIME header id='%s'\n", soap->dime_id?soap->dime_id:""));
  if (soap->dime_options)
    optlen = ((unsigned char)soap->dime_options[2] << 8 | (unsigned char)soap->dime_options[3]) + 4;
  if (soap->dime_id)
    idlen = strlen(soap->dime_id);
  if (soap->dime_type)
    typelen = strlen(soap->dime_type);
  tmp[0] = SOAP_DIME_VERSION | (soap->dime_flags & 0x7);
  tmp[1] = soap->dime_flags & 0xF0;
  tmp[2] = optlen >> 8;
  tmp[3] = optlen & 0xFF;
  tmp[4] = idlen >> 8;
  tmp[5] = idlen & 0xFF;
  tmp[6] = typelen >> 8;
  tmp[7] = typelen & 0xFF;
  tmp[8] = soap->dime_size >> 24;
  tmp[9] = soap->dime_size >> 16 & 0xFF;
  tmp[10] = soap->dime_size >> 8 & 0xFF;
  tmp[11] = soap->dime_size & 0xFF;
  if (soap_send_raw(soap, (char*)tmp, 12)
   || soap_putdimefield(soap, soap->dime_options, optlen)
   || soap_putdimefield(soap, soap->dime_id, idlen)
   || soap_putdimefield(soap, soap->dime_type, typelen))
    return soap->error;
  return SOAP_OK;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_putdime(struct soap *soap, int i, char *id, char *type, char *options, void *ptr, size_t size)
{ void *h;
  if (id)
    soap->dime_id = id;
  else
  { sprintf(soap->tmpbuf, soap->dime_id_format, i);
    soap->dime_id = soap->tmpbuf;
  }
  soap->dime_type = type;
  soap->dime_options = options;
  soap->dime_size = size;
  soap->dime_flags = SOAP_DIME_VERSION | SOAP_DIME_MEDIA;
  if (--soap->dime_count == 0)
    soap->dime_flags |= SOAP_DIME_ME;
  if (soap_putdimehdr(soap))
    return soap->error;
  if (soap->fdimereadopen && ((h = soap->fdimereadopen(soap, (void*)ptr, soap->dime_id, type, options)) || soap->error))
  { size_t n;
    if (!h)
      return soap->error;
    while (size)
    { if (size < sizeof(soap->tmpbuf))
        n = size;
      else
        n = sizeof(soap->tmpbuf);
      if (!(n = soap->fdimeread(soap, h, soap->tmpbuf, n)))
      { soap->error = SOAP_EOF;
	break;
      }
      if (soap_send_raw(soap, soap->tmpbuf, n))
        break;
      size -= n;
    }
    soap->fdimereadclose(soap, h);
    soap_send_raw(soap, "\0\0\0", -(int)soap->dime_size&3);
    return soap->error;
  }
  return soap_putdimefield(soap, (char*)ptr, size);
}

/******************************************************************************/
static char *
soap_getdimefield(struct soap *soap, int n)
{ register wchar c;
  register int i;
  register char *s;
  char *p = NULL;
  if (n)
  { p = (char*)soap_malloc(soap, n + 1);
    if (p)
    { s = p;
      for (i = n; i > 0; i--)
      { if ((c = soap_get1(soap)) == EOF)
        { soap->error = SOAP_EOF;
          return NULL;
        }
        *s++ = (char)c;
      }
      *s = '\0';
      if ((soap->error = soap_move(soap, -n&3)))
        return NULL;
    }
    else
      soap->error = SOAP_EOM;
  }
  return p;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_getdimehdr(struct soap *soap)
{ register wchar c;
  register char *s;
  register int i;
  unsigned char tmp[12];
  int optlen, idlen, typelen;
  if (!(soap->mode & SOAP_ENC_DIME))
    return soap->error = SOAP_EOD;
  DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Get DIME header\n"));
  if (soap->dime_buflen || soap->dime_chunksize)
  { if (soap_move(soap, soap->dime_size-soap_tell(soap)))
      return soap->error = SOAP_EOF;
    soap_unget(soap, soap_getchar(soap)); /* skip padding and get hdr */
    DBGLOG(TEST, SOAP_MESSAGE(fdebug, "... From chunked\n"));
    return SOAP_OK;
  }
  s = (char*)tmp;
  for (i = 12; i > 0; i--)
  { if ((c = soap_getchar(soap)) == EOF)
      return soap->error = SOAP_EOF;
    *s++ = (char)c;
  }
  if ((tmp[0]&0xF8) != SOAP_DIME_VERSION)
    return soap->error = SOAP_DIME_MISMATCH;
  soap->dime_flags = (tmp[0]&0x7) | (tmp[1]&0xF0);
  optlen = tmp[2] << 8 | tmp[3];
  idlen = tmp[4] << 8 | tmp[5];
  typelen = tmp[6] << 8 | tmp[7];
  soap->dime_size = tmp[8] << 24 | tmp[9] << 16 | tmp[10] << 8 | tmp[11];
  DBGLOG(TEST, SOAP_MESSAGE(fdebug, "DIME size=%u flags=0x%X\n", (unsigned int)soap->dime_size, soap->dime_flags));
  if (!(soap->dime_options = soap_getdimefield(soap, optlen)) && soap->error)
    return soap->error;
  if (!(soap->dime_id = soap_getdimefield(soap, idlen)) && soap->error)
    return soap->error;
  if (!(soap->dime_type = soap_getdimefield(soap, typelen)) && soap->error)
    return soap->error;
  DBGLOG(TEST, SOAP_MESSAGE(fdebug, "DIME id=%s, type=%s, options=%s\n", soap->dime_id?soap->dime_id:"", soap->dime_type?soap->dime_type:"", soap->dime_options?soap->dime_options+4:""));
  if (soap->dime_flags & SOAP_DIME_ME)
    soap->mode &= ~SOAP_ENC_DIME;
  return SOAP_OK;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_getdime(struct soap *soap)
{ if (soap_getdimehdr(soap))
    return soap->error;
  if (soap->fdimewriteopen && ((soap->dime_ptr = (char*)soap->fdimewriteopen(soap, soap->dime_id, soap->dime_type, soap->dime_options)) || soap->error))
  { char *id, *type, *options;
    size_t size, n;
    if (!soap->dime_ptr)
      return soap->error;
    id = soap->dime_id;
    type = soap->dime_type;
    options = soap->dime_options;
    for (;;)
    { size = soap->dime_size;
      for (;;)
      { n = soap->buflen - soap->bufidx;
        if (size < n)
          n = size;
        if ((soap->error = soap->fdimewrite(soap, (void*)soap->dime_ptr, soap->buf + soap->bufidx, n)))
          break;
	size -= n;
	if (!size)
	{ soap->bufidx += n;
	  break;
	}
	if (soap_recv(soap))
        { soap->error = SOAP_EOF;
	  goto end;
        }
      }
      if (soap_move(soap, -(int)soap->dime_size&3))
      { soap->error = SOAP_EOF;
	break;
      }
      if (!(soap->dime_flags & SOAP_DIME_CF))
        break;
      if (soap_getdimehdr(soap))
        break;
    }
end:
    if (soap->fdimewriteclose)
      soap->fdimewriteclose(soap, (void*)soap->dime_ptr);
    soap->dime_size = 0;
    soap->dime_id = id;
    soap->dime_type = type;
    soap->dime_options = options;
  }
  else if (soap->dime_flags & SOAP_DIME_CF)
  { char *id, *type, *options;
    register wchar c;
    register char *s;
    register int i;
    id = soap->dime_id;
    type = soap->dime_type;
    options = soap->dime_options;
    if (soap_new_block(soap))
      return SOAP_EOM;
    for (;;)
    { s = (char*)soap_push_block(soap, soap->dime_size);
      if (!s)
        return soap->error = SOAP_EOM;
      for (i = soap->dime_size; i > 0; i--)
      { if ((c = soap_get1(soap)) == EOF)
          return soap->error = SOAP_EOF;
        *s++ = (char)c;
      }
      if (soap_move(soap, -(int)soap->dime_size&3))
        return soap->error = SOAP_EOF;
      if (!(soap->dime_flags & SOAP_DIME_CF))
        break;
      if (soap_getdimehdr(soap))
        return soap->error;
    }
    soap->dime_size = soap->blist->size++; /* allocate one more for '\0' */
    if (!(soap->dime_ptr = soap_save_block(soap, NULL)))
      return soap->error;
    soap->dime_ptr[soap->dime_size] = '\0'; /* make 0-terminated to enable string-based attachments */
    soap->dime_id = id;
    soap->dime_type = type;
    soap->dime_options = options;
  }
  else
    soap->dime_ptr = soap_getdimefield(soap, soap->dime_size);
  return soap->error;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_encode_string(const char *s, char *t, int len)
{ register int c;
  int n = len;
  while ((c = *s++) && --n > 0)
  { if (c == ' ') 
      *t++ = '+';
    else if (c == '!'
          || c == '$'
          || (c >= '(' && c <= '.')
          || (c >= '0' && c <= '9')
          || (c >= 'A' && c <= 'Z')
          || c == '_'
          || (c >= 'a' && c <= 'z'))
      *t++ = c;
    else if (n > 2)
    { *t++ = '%';
      *t++ = (c>>4) + (c > 159 ? '7' : '0');
      c &= 0xF;
      *t++ = c + (c > 9 ? '7' : '0');
      n -= 2;
    }
    else
      break;
  }
  *t = '\0';
  return len - n;
}

/******************************************************************************/
SOAP_FMAC1
const char*
SOAP_FMAC2
soap_decode_string(char *buf, int len, const char *val)
{ const char *s;
  char *t;
  for (s = val; *s; s++)
    if (*s != ' ' && *s != '=')
      break;
  if (*s == '"')
  { t = buf;
    s++;
    while (*s && *s != '"' && --len)
      *t++ = *s++;
    *t = '\0';
    do s++;
    while (*s && *s != '&' && *s != ';' && *s != '=');
  }
  else
  { t = buf;
    while (*s && *s != '&' && *s != ';' && *s != '=' && --len)
      switch (*s)
      { case '+':
          *t++ = ' ';
        case ' ':
          s++;
          break;
        case '%':
          *t++ = ((s[1] >= 'A' ? (s[1]&0x7) + 9 : s[1] - '0') << 4) + (s[2] >= 'A' ? (s[2]&0x7) + 9 : s[2] - '0');
          s += 3;
          break;
        default:
          *t++ = *s++;
      }
    *t = '\0';
  }
  return s;
}

/******************************************************************************/

#ifdef WITH_COOKIES
/******************************************************************************/
SOAP_FMAC1
struct soap_cookie*
SOAP_FMAC2
soap_cookie(struct soap *soap, const char *name, const char *domain, const char *path)
{ struct soap_cookie *p;
  size_t n;
  if (!domain)
    domain = soap->cookie_domain;
  if (!path)
    path = soap->cookie_path;
  if (*path == '/')
    path++;
  n = strlen(path);
  for (p = soap->cookies; p; p = p->next)
    if (!strcmp(p->name, name) && !strcmp(p->domain, domain) && !strncmp(p->path, path, n))
      break;
  return p;
}

/******************************************************************************/
SOAP_FMAC1
struct soap_cookie*
SOAP_FMAC2
soap_set_cookie(struct soap *soap, const char *name, const char *value, const char *domain, const char *path)
{ struct soap_cookie **p, *q;
  int n;
  DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Set cookie: %s=%s domain=%s path=%s\n", name, value?value:"", domain?domain:"", path?path:""));
  if (!domain)
    domain = soap->cookie_domain;
  if (!domain)
  { soap_set_receiver_error(soap, "Cookie domain not set", NULL, SOAP_HTTP_ERROR);
    return NULL;
  }
  if (!path)
    path = soap->cookie_path;
  if (!path)
  { soap_set_receiver_error(soap, "Cookie path not set", NULL, SOAP_HTTP_ERROR);
    return NULL;
  }
  if (*path == '/')
    path++;
  q = soap_cookie(soap, name, domain, path);
  if (!q)
  { if ((q = (struct soap_cookie*)malloc(sizeof(struct soap_cookie))))
    { if ((q->name = (char*)malloc(strlen(name)+1)))
        strcpy(q->name, name);
      q->value = NULL;
      q->domain = NULL;
      q->path = NULL;
      q->expire = -1;
      q->version = 0;
      q->secure = 0;
      q->env = 0;
      q->modified = 0;
      for (p = &soap->cookies, n = soap->cookie_max; *p && n; p = &(*p)->next, n--)
        if (!strcmp((*p)->name, name) && (*p)->path && strcmp((*p)->path, path) < 0)
          break;
      if (n)
      { q->next = *p;
        *p = q;
      }
      else
      { free(q->name);
        free(q);
        q = NULL;
      }
    }
  }
  else
    q->modified = 1;
  if (q)
  { if (q->value)
    { free(q->value);
      q->value = NULL;
    }
    if (q->domain)
    { free(q->domain);
      q->domain = NULL;
    }
    if (q->path)
    { free(q->path);
      q->path = NULL;
    }
    if (value && *value && (q->value = (char*)malloc(strlen(value)+1)))
      strcpy(q->value, value);
    if ((q->domain = (char*)malloc(strlen(domain)+1)))
      strcpy(q->domain, domain);
    if ((q->path = (char*)malloc(strlen(path)+1)))
      strcpy(q->path, path);
    q->session = 1;
  }
  return q;
}

/******************************************************************************/
SOAP_FMAC1
void
SOAP_FMAC2
soap_clr_cookie(struct soap *soap, const char *name, const char *domain, const char *path)
{ struct soap_cookie **p, *q;
  if (!domain)
    domain = soap->cookie_domain;
  if (!domain)
  { soap_set_receiver_error(soap, "Cookie domain not set", "", SOAP_HTTP_ERROR);
    return;
  }
  if (!path)
    path = soap->cookie_path;
  if (!path)
  { soap_set_receiver_error(soap, "Cookie path not set", "", SOAP_HTTP_ERROR);
    return;
  }
  if (*path == '/')
   path++;
  for (p = &soap->cookies, q = *p; q; p = &q->next, q = *p)
    if (!strcmp(q->name, name) && !strcmp(q->domain, domain) && !strncmp(q->path, path, strlen(q->path)))
    { if (q->value)
        free(q->value);
      if (q->domain)
        free(q->domain);
      if (q->path)
        free(q->path);
      *p = q->next;
      free(q);
    }
}

/******************************************************************************/
SOAP_FMAC1
char *
SOAP_FMAC2
soap_cookie_value(struct soap *soap, const char *name, const char *domain, const char *path)
{ struct soap_cookie *p;
  if ((p = soap_cookie(soap, name, domain, path)))
    return p->value;
  return NULL;
}

/******************************************************************************/
SOAP_FMAC1
long
SOAP_FMAC2
soap_cookie_expire(struct soap *soap, const char *name, const char *domain, const char *path)
{ struct soap_cookie *p;
  if ((p = soap_cookie(soap, name, domain, path)))
    return p->expire;
  return -1;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_set_cookie_expire(struct soap *soap, const char *name, long expire, const char *domain, const char *path)
{ struct soap_cookie *p;
  if ((p = soap_cookie(soap, name, domain, path)))
  { p->expire = expire;
    p->modified = 1;
    return SOAP_OK;
  }
  return SOAP_EOF;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_set_cookie_session(struct soap *soap, const char *name, const char *domain, const char *path)
{ struct soap_cookie *p;
  if ((p = soap_cookie(soap, name, domain, path)))
  { p->session = 1;
    p->modified = 1;
    return SOAP_OK;
  }
  return SOAP_EOF;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_clr_cookie_session(struct soap *soap, const char *name, const char *domain, const char *path)
{ struct soap_cookie *p;
  if ((p = soap_cookie(soap, name, domain, path)))
  { p->session = 0;
    p->modified = 1;
    return SOAP_OK;
  }
  return SOAP_EOF;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_putsetcookies(struct soap *soap)
{ struct soap_cookie *p;
  char *s, tmp[4096];
  for (p = soap->cookies; p; p = p->next)
    if (p->modified || !p->env)
    { s = tmp;
      if (p->name)
        s += soap_encode_string(p->name, s, tmp-s+4064);
      if (p->value)
      { *s++ = '=';
        s += soap_encode_string(p->value, s, tmp-s+4064);
      }
      if (p->domain && (int)strlen(p->domain) < tmp-s+4064)
        sprintf(s, ";Domain=%s", p->domain);
      else if (soap->cookie_domain && (int)strlen(soap->cookie_domain) < tmp-s+4064)
        sprintf(s, ";Domain=%s", soap->cookie_domain);
      s += strlen(s);
      if (p->path && (int)strlen(p->path) < tmp-s+4064)
        sprintf(s, ";Path=/%s", p->path);
      else if (soap->cookie_path && (int)strlen(soap->cookie_path) < tmp-s+4064)
        sprintf(s, ";Path=/%s", soap->cookie_path);
      else 
        strcpy(s, ";Path=/");
      s += strlen(s);
      if (p->version > 0)
      { sprintf(s, ";Version=%u", p->version);
        s += strlen(s);
      }
      if (p->expire >= 0)
      { sprintf(s, ";Max-Age=%ld", p->expire);
        s += strlen(s);
      }
      if (p->secure)
        strcpy(s, ";Secure");
      DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Set-Cookie: %s\n", tmp));
      if (soap->fposthdr(soap, "Set-Cookie", tmp))
        return soap->error;
    }
  return SOAP_OK;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_putcookies(struct soap *soap, const char *domain, const char *path, int secure)
{ struct soap_cookie **p, *q;
  unsigned int version = 0;
  time_t now = time(NULL);
  char *s, tmp[4096];
  p = &soap->cookies;
  while ((q = *p))
  { if (q->expire && now > q->expire)
    { DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Cookie %s expired\n", q->name));
      free(q->name);
      if (q->value)
        free(q->value);
      if (q->domain)
        free(q->domain);
      if (q->path)
        free(q->path);
      *p = q->next;
      free(q);
    }
    else if ((!q->domain || !strcmp(q->domain, domain))
          && (!q->path || !strncmp(q->path, path, strlen(q->path)))
          && (!q->secure || secure))
    { s = tmp;
      if (q->version != version)
      { sprintf(s, "$Version=%u;", q->version);
        version = q->version;
      }
      if (q->name)
        s += soap_encode_string(q->name, s, tmp-s+4080);
      if (q->value)
      { *s++ = '=';
        s += soap_encode_string(q->value, s, tmp-s+4080);
      }
      if (q->path && (int)strlen(q->path) < tmp-s+4080)
      { sprintf(s, ";$Path=/%s", q->path);
        s += strlen(s);
      }
      if (q->domain && (int)strlen(q->domain) < tmp-s+4080)
        sprintf(s, ";$Domain=%s", q->domain);
      DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Cookie: %s\n", tmp));
      if (soap->fposthdr(soap, "Cookie", tmp))
        return soap->error;
      p = &q->next;
    }
    else
      p = &q->next;
  }
  return SOAP_OK;
}

/******************************************************************************/
SOAP_FMAC1
void
SOAP_FMAC2
soap_getcookies(struct soap *soap, const char *val)
{ struct soap_cookie *p = NULL, *q;
  const char *s;
  char *t, tmp[4096]; /* cookie size is up to 4096 bytes [RFC2109] */
  char *domain = NULL;
  char *path = NULL;
  unsigned int version = 0;
  time_t now = time(NULL);
  if (!val)
    return;
  s = val;
  while (*s)
  { s = soap_decode_string(tmp, sizeof(tmp), s);
    if (!soap_tag_cmp(tmp, "$Version"))
    { if ((s = soap_decode_string(tmp, sizeof(tmp), s)))
      { if (p)
          p->version = (int)atol(tmp);
        else
          version = (int)atol(tmp);
      }
    }
    else if (!soap_tag_cmp(tmp, "$Path"))
    { s = soap_decode_string(tmp, sizeof(tmp), s);
      if (*tmp)
      { t = (char*)malloc(strlen(tmp)+1);
        strcpy(t, tmp);
      }
      else
        t = NULL;
      if (p)
        p->path = t;
      else
      { if (path)
          free(path);
        path = t;
      }
    }
    else if (!soap_tag_cmp(tmp, "$Domain"))
    { s = soap_decode_string(tmp, sizeof(tmp), s);
      if (*tmp)
      { t = (char*)malloc(strlen(tmp)+1);
        strcpy(t, tmp);
      }
      else
        t = NULL;
      if (p)
        p->domain = t;
      else
      { if (domain)
          free(domain);
        domain = t;
      }
    }
    else if (p && !soap_tag_cmp(tmp, "Path"))
    { s = soap_decode_string(tmp, sizeof(tmp), s);
      if (*tmp)
      { p->path = (char*)malloc(strlen(tmp)+1);
        strcpy(p->path, tmp);
      }
      else
        p->path = NULL;
    }
    else if (p && !soap_tag_cmp(tmp, "Domain"))
    { s = soap_decode_string(tmp, sizeof(tmp), s);
      if (*tmp)
      { p->domain = (char*)malloc(strlen(tmp)+1);
        strcpy(p->domain, tmp);
      }
      else
        p->domain = NULL;
    }
    else if (p && !soap_tag_cmp(tmp, "Version"))
    { s = soap_decode_string(tmp, sizeof(tmp), s);
      p->version = (unsigned int)atol(tmp);
    }
    else if (p && !soap_tag_cmp(tmp, "Max-Age"))
    { s = soap_decode_string(tmp, sizeof(tmp), s);
      p->expire = now + atol(tmp);
    }
    else if (p && !soap_tag_cmp(tmp, "Expires"))
    { struct tm T;
      char a[3]; 
      static const char mns[] = "anebarprayunulugepctovec";
      s = soap_decode_string(tmp, sizeof(tmp), s);
      memset(&T, 0, sizeof(struct tm));
      a[0] = tmp[4];
      a[1] = tmp[5];
      a[2] = '\0';
      T.tm_mday = (int)atol(a);
      a[0] = tmp[8];
      a[1] = tmp[9];
      T.tm_mon = (strstr(mns, a) - mns) / 2;
      a[0] = tmp[11];
      a[1] = tmp[12];
      T.tm_year = 100 + (int)atol(a);
      a[0] = tmp[13];
      a[1] = tmp[14];
      T.tm_hour = (int)atol(a);
      a[0] = tmp[16];
      a[1] = tmp[17];
      T.tm_min = (int)atol(a);
      a[0] = tmp[19];
      a[1] = tmp[20];
      T.tm_sec = (int)atol(a);
      p->expire = soap_timegm(&T);
    }
    else if (p && !soap_tag_cmp(tmp, "Secure"))
      p->secure = 1;
    else
    { if (p)
      { DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Got environment cookie %s=%s domain=%s path=%s expire=%ld secure=%d\n", p->name, p->value?p->value:"", p->domain?p->domain:"", p->path?p->path:"", p->expire, p->secure));
        if ((q = soap_set_cookie(soap, p->name, p->value, p->domain, p->path)))
        { q->version = p->version;
          q->expire = p->expire;
          q->secure = p->secure;
          q->env = 1;
        }
        if (p->name)
          free(p->name);
        if (p->value)
          free(p->value);
        if (p->domain)
          free(p->domain);
        if (p->path)
          free(p->path);
        free(p);
      }
      if ((p = (struct soap_cookie*)malloc(sizeof(struct soap_cookie))));
      { p->name = (char*)malloc(strlen(tmp)+1);
        strcpy(p->name, tmp);
        s = soap_decode_string(tmp, sizeof(tmp), s);
        if (*tmp)
        { p->value = (char*)malloc(strlen(tmp)+1);
          strcpy(p->value, tmp);
        }
        else
          p->value = NULL;
        p->domain = domain;
        p->path = path;
        p->expire = 0;
        p->secure = 0;
        p->version = version;
      }
    }
    if (*s == ';')
      s++;
  }
  if (p)
  { DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Got cookie %s=%s domain=%s path=%s expire=%ld secure=%d\n", p->name, p->value?p->value:"", p->domain?p->domain:"", p->path?p->path:"", p->expire, p->secure));
    if ((q = soap_set_cookie(soap, p->name, p->value, p->domain, p->path)))
    { q->version = p->version;
      q->expire = p->expire;
      q->secure = p->secure;
    }
    if (p->name)
      free(p->name);
    if (p->value)
      free(p->value);
    if (p->domain)
      free(p->domain);
    if (p->path)
      free(p->path);
    free(p);
  }
  if (domain)
    free(domain);
  if (path)
    free(path);
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_getenv_cookies(struct soap *soap)
{ struct soap_cookie *p;
  const char *s;
  char key[4096], val[4096]; /* cookie size is up to 4096 bytes [RFC2109] */
  if (!(s = getenv("HTTP_COOKIE")))
    return SOAP_EOF;
  do
  { s = soap_decode_string(key, sizeof(key), s);
    s = soap_decode_string(val, sizeof(val), s);
    p = soap_set_cookie(soap, key, val, NULL, NULL);
    if (p)
      p->env = 1;
    if (*s == ';')
      s++;
  } while (*s);
  return SOAP_OK;
}

/******************************************************************************/
SOAP_FMAC1
struct soap_cookie*
SOAP_FMAC2
soap_copy_cookies(struct soap *soap)
{ struct soap_cookie *p, **q, *r;
  q = &r;
  for (p = soap->cookies; p; p = p->next)
  { *q = (struct soap_cookie*)malloc(sizeof(struct soap_cookie));
    **q = *p;
    if (p->name)
    { (*q)->name = (char*)malloc(strlen(p->name)+1);
      strcpy((*q)->name, p->name);
    }
    if (p->value)
    { (*q)->value = (char*)malloc(strlen(p->value)+1);
      strcpy((*q)->value, p->value);
    }
    if (p->domain)
    { (*q)->domain = (char*)malloc(strlen(p->domain)+1);
      strcpy((*q)->domain, p->domain);
    }
    if (p->path)
    { (*q)->path = (char*)malloc(strlen(p->path)+1);
      strcpy((*q)->path, p->path);
    }
    q = &(*q)->next;
  }
  *q = NULL;
  return r;
}

/******************************************************************************/
SOAP_FMAC1
void
SOAP_FMAC2
soap_free_cookies(struct soap *soap)
{ struct soap_cookie *p;
  for (p = soap->cookies; p; p = soap->cookies)
  { soap->cookies = p->next;
    free(p->name);
    if (p->value)
      free(p->value);
    if (p->domain)
      free(p->domain);
    if (p->path)
      free(p->path);
    free(p);
  }
}

/******************************************************************************/
#endif

#ifdef WITH_GZIP
/******************************************************************************/
static int
soap_getgzipheader(struct soap *soap)
{ int i;
  wchar c, f = 0;
  DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Get gzip header\n"));
  for (i = 0; i < 9; i++)
  { if ((c = soap_get1(soap) == EOF))
      return soap->error = SOAP_EOF;
    if (i == 2)
      f = c;
  }
  if (f & 0x04) /* FEXTRA */
  { for (i = soap_get1(soap) + soap_get1(soap) << 8; i; i--)
      if (soap_get1(soap) == EOF)
        return soap->error = SOAP_EOF;
  }
  if (f & 0x08) /* FNAME */
    do
      c = soap_get1(soap);
    while (c && c != EOF);
  if (c != EOF && (f & 0x10)) /* FCOMMENT */
    do
      c = soap_get1(soap);
    while (c && f != EOF);
  if (c != EOF && (f & 0x01)) /* FHCRC */
  { if ((c = soap_get1(soap)) != EOF)
      c = soap_get1(soap);
  }
  if (c == EOF)
    return soap->error = SOAP_EOF;
  return SOAP_OK;
}

/******************************************************************************/
#endif

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_begin_recv(struct soap *soap)
{ wchar c;
  soap_set_local_namespaces(soap);
  soap->mode = soap->imode;
  if (!soap->keep_alive)
  { soap->buflen = 0;
    soap->bufidx = 0;
  }
  if (!(soap->mode & SOAP_IO_KEEPALIVE))
    soap->keep_alive = 0;
  soap->ahead = 0;
  soap->peeked = 0;
  soap->level = 0;
  soap->is_in_header = 0;
  soap->alloced = 0;
  soap->count = 0;
  soap->length = 0;
  soap->cdata = 0;
  *soap->endpoint = '\0';
  soap->dime_chunksize = 0;
  soap->dime_buflen = 0;
#ifdef WIN32
#ifndef UNDER_CE
  if (soap->socket < 0)
#ifndef WITH_FASTCGI
#ifdef __BORLANDC__
    setmode(soap->recvfd, O_BINARY);
#else
    _setmode(soap->recvfd, _O_BINARY);
#endif
#endif
#endif
#endif
#ifdef WITH_ZLIB
  soap->zlib_in = SOAP_ZLIB_NONE;
  soap->zlib_out = SOAP_ZLIB_NONE;
  soap->d_stream.next_in = Z_NULL;
  soap->d_stream.avail_in = 0;
  soap->d_stream.next_out = (Byte*)soap->buf;
  soap->d_stream.avail_out = SOAP_BUFLEN;
  soap->z_ratio_in = 1.0;
#endif
  c = soap_getchar(soap);
  if ((c & 0xFFFC) == (SOAP_DIME_VERSION | SOAP_DIME_MB) && (soap_get0(soap) & 0xFFF0) == 0x20)
    soap->mode |= SOAP_ENC_DIME;
#ifdef WITH_GZIP
  else if (c == 0x1F)
  { if (soap_getgzipheader(soap))
      return soap->error;
    if (inflateInit2(&soap->d_stream, -MAX_WBITS) != Z_OK)
      return soap->error = SOAP_ZLIB_ERROR;
    soap->mode |= SOAP_ENC_ZLIB;
    soap->zlib_in = SOAP_ZLIB_GZIP;
    soap->z_crc = crc32(0L, NULL, 0);
    DBGLOG(TEST, SOAP_MESSAGE(fdebug, "gzip initialized\n"));
    memcpy(soap->z_buf, soap->buf, SOAP_BUFLEN);
    /* should not chunk over plain transport, so why bother to check? */
    /* if ((soap->mode & SOAP_IO) == SOAP_IO_CHUNK) */
    /*   soap->z_buflen = soap->bufidx; */
    /* else */
    soap->d_stream.next_in = (Byte*)(soap->z_buf + soap->bufidx);
    soap->d_stream.avail_in = soap->buflen - soap->bufidx;
    soap->z_buflen = soap->buflen;
    soap->buflen = soap->bufidx;
  }  
#endif
  else
    while (blank(c))
      c = soap_getchar(soap);
  if (c == EOF)
    return soap->error = SOAP_EOF;
  soap_unget(soap, c);
  if (c != '<' && !(soap->mode & (SOAP_ENC_DIME | SOAP_ENC_ZLIB)))
  { soap->mode &= ~SOAP_IO;
    if ((soap->error = soap->fparse(soap)))
      return soap->error;
    if ((soap->mode & SOAP_IO) == SOAP_IO_CHUNK)
    { soap->chunkbuflen = soap->buflen;
      soap->buflen = soap->bufidx;
      soap->chunksize = 0;
    }
#ifdef WITH_ZLIB
    if (soap->zlib_in)
    { /* fparse should not use soap_unget */
#ifdef WITH_GZIP
      c = soap_get1(soap);
      if (c == 0x1F)
      { if (soap_getgzipheader(soap))
          return soap->error;
        if (inflateInit2(&soap->d_stream, -MAX_WBITS) != Z_OK)
          return soap->error = SOAP_ZLIB_ERROR;
        soap->z_crc = crc32(0L, NULL, 0);
        DBGLOG(TEST, SOAP_MESSAGE(fdebug, "gzip initialized\n"));
      }
      else
      { soap_revget1(soap);
#else
      {
#endif
        if (inflateInit(&soap->d_stream) != Z_OK)
          return soap->error = SOAP_ZLIB_ERROR;
        DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Inflate initialized\n"));
      }
      soap->mode |= SOAP_ENC_ZLIB;
      memcpy(soap->z_buf, soap->buf, SOAP_BUFLEN);
      soap->d_stream.next_in = (Byte*)(soap->z_buf + soap->bufidx);
      soap->d_stream.avail_in = soap->buflen - soap->bufidx;
      soap->z_buflen = soap->buflen;
      soap->buflen = soap->bufidx;
    }
#endif
  }
  if (soap->mode & SOAP_ENC_DIME)
  { if (soap_getdimehdr(soap))
      return soap->error;
    if (soap->dime_flags & SOAP_DIME_CF)
    { DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Chunked DIME SOAP message\n"));
      soap->dime_chunksize = soap->dime_size;
      if (soap->buflen - soap->bufidx >= soap->dime_chunksize)
      { soap->dime_buflen = soap->buflen;
        soap->buflen = soap->bufidx + soap->dime_chunksize;
      }
      else
        soap->dime_chunksize -= soap->buflen - soap->bufidx;
    }
    soap->count = soap->buflen - soap->bufidx;
  }
  return SOAP_OK;
}

/******************************************************************************/
static int
http_parse(struct soap *soap)
{ char header[SOAP_HDRLEN], *s;
  unsigned short g = 0, k;
  DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Waiting for response...\n"));
  *soap->endpoint = '\0';
  *soap->path = '\0';
  soap->length = 0;
  do
  { if (soap_getline(soap, soap->msgbuf, sizeof(soap->msgbuf)))
      return SOAP_EOF;
    DBGLOG(TEST,SOAP_MESSAGE(fdebug, "HTTP status: %s\n", soap->msgbuf));
    for (;;)
    { if (soap_getline(soap, header, SOAP_HDRLEN))
        return SOAP_EOF;
      if (!*header)
        break;
      DBGLOG(TEST,SOAP_MESSAGE(fdebug, "HTTP header: %s\n", header));
      s = strchr(header, ':');
      if (s)
      { *s = '\0';
        do s++;
        while (*s && *s <= 32);
      }
      if ((soap->error = soap->fparsehdr(soap, header, s)))
        return soap->error;
    }
    if ((s = strchr(soap->msgbuf, ' ')))
      k = (unsigned short)strtoul(s, NULL, 10);
    else
      k = 0;
  } while (k == 100);
  DBGLOG(TEST,SOAP_MESSAGE(fdebug, "Finished HTTP header parsing\n"));
  s = strstr(soap->msgbuf, "HTTP/");
  if (soap->keep_alive > 0 && s && s[7] != '1')
    soap->keep_alive = 0;
  else if (soap->keep_alive < 0)
    soap->keep_alive = 1;
  DBGLOG(TEST,SOAP_MESSAGE(fdebug, "Keep alive connection = %d\n", soap->keep_alive));
  if (s && (((g = !strncmp(soap->msgbuf, "GET ", 4))) || !strncmp(soap->msgbuf, "POST ", 5)))
  { size_t m = strlen(soap->endpoint);
    size_t n = m + (s - soap->msgbuf) - 5 - (!g);
    if (n >= (int)sizeof(soap->endpoint))
      n = sizeof(soap->endpoint) - 1;
    strncpy(soap->path, soap->msgbuf + 4 + (!g), n - m);
    soap->path[n - m] = '\0';
    strcat(soap->endpoint, soap->path);
    DBGLOG(TEST,SOAP_MESSAGE(fdebug, "Target endpoint='%s'\n", soap->endpoint));
    if (g)
      return soap->error = SOAP_GET_METHOD;
    return SOAP_OK;
  }
  if (k == 0 || k == 200 || k == 400 || k == 500)
    return SOAP_OK;
  return soap_set_receiver_error(soap, "HTTP error", soap->msgbuf, SOAP_HTTP_ERROR);
}

/******************************************************************************/
static int
http_parse_header(struct soap *soap, const char *key, const char *val)
{ if (!soap_tag_cmp(key, "Host"))
  { 
#ifdef WITH_OPENSSL
    if (soap->imode & SOAP_ENC_SSL)
      strcpy(soap->endpoint, "https://");
    else
#endif
    strcpy(soap->endpoint, "http://");
    strncat(soap->endpoint, val, sizeof(soap->endpoint) - 8);
    soap->endpoint[sizeof(soap->endpoint) - 1] = '\0';
  }
  else if (!soap_tag_cmp(key, "Content-Type"))
  { if (!soap_tag_cmp(val, "*application/dime*"))
      soap->mode |= SOAP_ENC_DIME;
  }
  else if (!soap_tag_cmp(key, "Content-Length"))
    soap->length = strtoul(val, NULL, 10);
  else if (!soap_tag_cmp(key, "Content-Encoding"))
  { if (!soap_tag_cmp(val, "deflate*"))
#ifdef WITH_ZLIB
      soap->zlib_in = SOAP_ZLIB_DEFLATE;
#else
      return SOAP_ZLIB_ERROR;
#endif
    else if (!soap_tag_cmp(val, "gzip*"))
#ifdef WITH_GZIP
      soap->zlib_in = SOAP_ZLIB_GZIP;
#else
      return SOAP_ZLIB_ERROR;
#endif
  }
#ifdef WITH_ZLIB
  else if (!soap_tag_cmp(key, "Accept-Encoding"))
  {
#ifdef WITH_GZIP
    if (!soap_tag_cmp(val, "*gzip*"))
      soap->zlib_out = SOAP_ZLIB_GZIP;
    else
#endif
    if (!soap_tag_cmp(val, "*deflate*"))
      soap->zlib_out = SOAP_ZLIB_DEFLATE;
    else
      soap->zlib_out = SOAP_ZLIB_NONE;
  }
#endif
  else if (!soap_tag_cmp(key, "Transfer-Encoding"))
  { soap->mode &= ~SOAP_IO;
    if (!soap_tag_cmp(val, "chunked*"))
      soap->mode |= SOAP_IO_CHUNK;
  }
  else if (!soap_tag_cmp(key, "Connection"))
  { if (!soap_tag_cmp(val, "Keep-Alive*"))
      soap->keep_alive = -soap->keep_alive;
    else if (!soap_tag_cmp(val, "close*"))
      soap->keep_alive = 0;
  }
  else if (!soap_tag_cmp(key, "SOAPAction"))
  { if (*val && (soap->action = (char*)malloc(strlen(val))))
    { strcpy(soap->action, val + 1);
      soap->action[strlen(soap->action) - 1] = '\0';
    }
  }
/* [ Deal with .NET bug (invalid XML) */
  else if (!soap_tag_cmp(key, "Server"))
  { if (!soap_tag_cmp(val, "Microsoft-IIS*"))
      soap->dot_net_bug = 1;
  }
  else if (!soap_tag_cmp(key, "User-Agent"))
  { if (!soap_tag_cmp(val, "*.NET CLR*") || !soap_tag_cmp(val, "*MS Web Services Client Protocol*"))
      soap->dot_net_bug = 1;
  }
/* ] */
#ifdef WITH_COOKIES
  else if (!soap_tag_cmp(key, "Cookie") || !soap_tag_cmp(key, "Set-Cookie"))
    soap_getcookies(soap, val);
#endif
  return SOAP_OK;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_envelope_begin_out(struct soap *soap)
{ return soap_element_begin_out(soap, "SOAP-ENV:Envelope", 0, NULL);
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_envelope_end_out(struct soap *soap)
{ return soap_send(soap, "</SOAP-ENV:Envelope>"); 
} 

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_envelope_begin_in(struct soap *soap)
{ if (soap_element_begin_in(soap, "SOAP-ENV:Envelope"))
    return soap->error = SOAP_VERSIONMISMATCH;
  return SOAP_OK;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_envelope_end_in(struct soap *soap)
{ return soap_element_end_in(soap, "SOAP-ENV:Envelope");
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_body_begin_out(struct soap *soap)
{ if (soap->encodingStyle && !*soap->encodingStyle && soap->local_namespaces)
  { if (soap->local_namespaces[1].out)
      soap->encodingStyle = soap->local_namespaces[1].out;
    else
      soap->encodingStyle = soap->local_namespaces[1].ns;
  }
  if (soap->encodingStyle)
  { if (soap_send(soap, "<SOAP-ENV:Body SOAP-ENV:encodingStyle=\"")
     || soap_send(soap, soap->encodingStyle)
     || soap_send(soap, "\">"))
      return soap->error;
    return SOAP_OK;
  }
  return soap_send(soap, "<SOAP-ENV:Body>");
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_body_end_out(struct soap *soap)
{ return soap_send(soap, "</SOAP-ENV:Body>");
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_body_begin_in(struct soap *soap)
{ return soap_element_begin_in(soap, "SOAP-ENV:Body");
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_body_end_in(struct soap *soap)
{ return soap_element_end_in(soap, "SOAP-ENV:Body");
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_recv_header(struct soap *soap)
{ if (soap_getheader(soap) && soap->error == SOAP_TAG_MISMATCH)
    soap->error = SOAP_OK;
  return soap->error;
}

/******************************************************************************/
SOAP_FMAC1
void
SOAP_FMAC2
soap_set_endpoint(struct soap *soap, const char *endpoint)
{ const char *s;
  size_t i, j, n;
  *soap->endpoint = '\0';
  *soap->host = '\0';
  *soap->path = '\0';
  soap->port = 80;
  if (!endpoint || !*endpoint)
    return;
#ifdef WITH_OPENSSL
  if (!strncmp(endpoint, "https:", 6))
    soap->port = 443;
#endif
  strncpy(soap->endpoint, endpoint, SOAP_TAGLEN);
  s = strchr(endpoint, ':');
  if (s && s[1] == '/' && s[2] == '/')
    s += 3;
  else
    s = endpoint;
  n = strlen(s);
  if (n >= SOAP_TAGLEN)
    n = SOAP_TAGLEN;
  for (i = 0; i < n; i++)
  { soap->host[i] = s[i];
    if (s[i] == '/' || s[i] == ':')
      break; 
  }
  soap->host[i] = '\0';
  if (s[i] == ':')
  { soap->port = (int)atol(s + i + 1);
    for (i++; i < n; i++)
      if (s[i] == '/')
        break;
  }
  for (j = i++; j < n; j++)
    soap->path[j - i] = s[j];
  soap->path[j - i]='\0';
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_connect(struct soap *soap, const char *endpoint, const char *action)
{ char host[SOAP_TAGLEN];
  int port;
  size_t count;
  strcpy(host, soap->host); /* save to compare */
  port = soap->port; /* save to compare */
  soap_set_endpoint(soap, endpoint);
  if (action)
  { soap->action = (char*)malloc(strlen(action)+1);
    strcpy(soap->action, action);
  }
  if (*soap->host)
  { if (soap->socket < 0 || strcmp(soap->host, host) || soap->port != port)
    { soap->keep_alive = 0; /* force close */
      soap_closesock(soap);
      DBGLOG(TEST,SOAP_MESSAGE(fdebug, "Connect to host='%s' path='%s' port=%d\n", soap->host, soap->path, soap->port));
      soap->socket = soap->fopen(soap, endpoint, soap->host, soap->port);
      if (soap->socket < 0 && soap->error)
        return soap->error;
      soap->keep_alive = ((soap->omode & SOAP_IO_KEEPALIVE) != 0);
    }
    else
    {
#ifndef MAC_CARBON
      if (soap_poll(soap) != SOAP_OK)
      { soap->keep_alive = 0; /* force close */
        soap_closesock(soap);
        DBGLOG(TEST,SOAP_MESSAGE(fdebug, "Reconnect to host='%s' path='%s' port=%d\n", soap->host, soap->path, soap->port));
        soap->socket = soap->fopen(soap, endpoint, soap->host, soap->port);
        if (soap->socket < 0 && soap->error)
          return soap->error;
      }
#endif
    }
  }
  soap->status = SOAP_POST;
  count = soap_begin_dime(soap);
  if (soap_begin_send(soap))
    return soap->error;
  if ((soap->mode & SOAP_IO) != SOAP_IO_STORE && !(soap->mode & SOAP_ENC_XML) && endpoint)
  { int n = soap->mode;
    soap->mode &= ~(SOAP_IO | SOAP_ENC_ZLIB);
    if ((n & SOAP_IO) != SOAP_IO_FLUSH)
      soap->mode |= SOAP_IO_BUFFER;
    if ((soap->error = soap->fpost(soap, endpoint, soap->host, soap->port, soap->path, action, count)))
      return soap->error;
    if ((n & SOAP_IO) == SOAP_IO_CHUNK)
    { if (soap_flush(soap))
        return soap->error;
    }
    soap->mode = n;
  }
  if (soap->mode & SOAP_ENC_DIME)
    return soap_putdimehdr(soap);
  return SOAP_OK;
}

/******************************************************************************/
static char *
soap_base64_string(const char *s, char *t, size_t n)
{ register size_t i;
  register unsigned long m;
  char d[5];
  if (!s || !t)
    return NULL;
  *t = '\0';
  d[4] = '\0';
  for (; n > 2; n -= 3, s += 3)
  { m = s[0] << 16 | s[1] << 8 | s[2];
    for (i = 4; i > 0; m >>= 6)
      d[--i] = base64[m&0x3F];
    strcat(t, d);
  }
  if (n > 0)
  { m = 0;
    for (i = 0; i < n; i++)
      m = m << 8 | *s++;
    for (; i < 3; i++)
      m <<= 8;
    for (i++; i > 0; m >>= 6)
      d[--i] = base64[m&0x3F];
    for (i = 3; i > n; i--)
      d[i] = '=';
    strcat(t, d);
  }
  return t;
}

/******************************************************************************/
static int
soap_puthttphdr(struct soap *soap, int status, size_t count)
{ const char *s;
  if (status == SOAP_HTML)
    s = "text/html; charset=utf-8";
  else if (soap->mode & SOAP_ENC_DIME)
    s = "application/dime; charset=us-ascii";
  else if (soap->version == 2)
    s = "application/soap+xml; charset=utf-8";
  else
    s = "text/xml; charset=utf-8";
  soap->error = soap->fposthdr(soap, "Content-Type", s);
  if (soap->error)
    return soap->error;
#ifdef WITH_ZLIB
  if (soap->omode & SOAP_ENC_ZLIB)
#ifdef WITH_GZIP
    soap->error = soap->fposthdr(soap, "Content-Encoding", "gzip");
#else
    soap->error = soap->fposthdr(soap, "Content-Encoding", "deflate");
#endif
#endif
  if ((soap->omode & SOAP_IO) == SOAP_IO_CHUNK)
    soap->error = soap->fposthdr(soap, "Transfer-Encoding", "chunked");
  else if (count > 0)
  { sprintf(soap->tmpbuf, "%lu", (unsigned long)count);
    soap->error = soap->fposthdr(soap, "Content-Length", soap->tmpbuf);
  }
  if (soap->error)
    return soap->error;
  return soap->error = soap->fposthdr(soap, "Connection", soap->keep_alive ? "Keep-Alive" : "close");
}

/******************************************************************************/
static int
http_post(struct soap *soap, const char *endpoint, const char *host, int port, const char *path, const char *action, size_t count)
{ const char *s;
  if (soap->status == SOAP_GET)
    s = "GET";
  else
    s = "POST";
  if (!endpoint || (strncmp(endpoint, "http:", 5) && strncmp(endpoint, "https:", 6) && strncmp(endpoint, "httpg:", 6)))
    return SOAP_OK;
  if (soap->proxy_host)
    sprintf(soap->tmpbuf, "%s %s HTTP/%s", s, endpoint, soap->http_version);
  else
    sprintf(soap->tmpbuf, "%s /%s HTTP/%s", s, path, soap->http_version);
  if ((soap->error = soap->fposthdr(soap, soap->tmpbuf, NULL)))
    return soap->error;
  if (port != 80)
    sprintf(soap->tmpbuf, "%s:%d", host, port);
  else
    strcpy(soap->tmpbuf, host); 
  if ((soap->error = soap->fposthdr(soap, "Host", soap->tmpbuf))
   || (soap->error = soap->fposthdr(soap, "User-Agent", "gSOAP/2.2"))
   || (soap->error = soap_puthttphdr(soap, SOAP_OK, count)))
    return soap->error;
#ifdef WITH_ZLIB
#ifdef WITH_GZIP
  if ((soap->error = soap->fposthdr(soap, "Accept-Encoding", "gzip, deflate")))
#else
  if ((soap->error = soap->fposthdr(soap, "Accept-Encoding", "deflate")))
#endif
    return soap->error;
#endif
  if (soap->userid && soap->passwd && strlen(soap->userid) + strlen(soap->passwd) < 761)
  { sprintf(soap->tmpbuf + 262, "%s:%s", soap->userid, soap->passwd);
    strcpy(soap->tmpbuf, "Basic ");
    soap_base64_string(soap->tmpbuf + 262, soap->tmpbuf + 6, strlen(soap->tmpbuf + 262));
    if ((soap->error = soap->fposthdr(soap, "Authorization", soap->tmpbuf)))
      return soap->error;
  }
  if (soap->proxy_userid && soap->proxy_passwd && strlen(soap->proxy_userid) + strlen(soap->proxy_passwd) < 761)
  { sprintf(soap->tmpbuf+262, "%s:%s", soap->proxy_userid, soap->proxy_passwd);
    strcpy(soap->tmpbuf, "Basic ");
    soap_base64_string(soap->tmpbuf+262, soap->tmpbuf+6, strlen(soap->tmpbuf+262));
    if ((soap->error = soap->fposthdr(soap, "Proxy-Authorization", soap->tmpbuf)))
      return soap->error;
  }
#ifdef WITH_COOKIES
#ifdef WITH_OPENSSL
  if (soap_putcookies(soap, host, path, soap->ssl != NULL))
    return soap->error;
#else
  if (soap_putcookies(soap, host, path, 0))
    return soap->error;
#endif
#endif
  if (action)
  { sprintf(soap->tmpbuf, "\"%s\"", action);
    if ((soap->error = soap->fposthdr(soap, "SOAPAction", soap->tmpbuf)))
      return soap->error;
  }
  return soap->error = soap->fposthdr(soap, NULL, NULL);
}

/******************************************************************************/
static int
http_post_header(struct soap *soap, const char *key, const char *val)
{ if (key)
  { if (soap_send(soap, key))
      return soap->error;
    if (val && (soap_send(soap, ": ") || soap_send(soap, val)))
      return soap->error;
  }
  return soap_send(soap, "\r\n");
}

/******************************************************************************/
static int
http_response(struct soap *soap, int status, size_t count)
{ if (!status || status == SOAP_HTML)
  { DBGLOG(TEST, SOAP_MESSAGE(fdebug, "OK 200\n"));
    if (soap->master >= 0 || soap->socket >= 0) /* standalone application */
    { sprintf(soap->tmpbuf, "HTTP/%s 200 OK", soap->http_version);
      if ((soap->error = soap->fposthdr(soap, soap->tmpbuf, NULL)))
        return soap->error;
    }
    else if ((soap->error = soap->fposthdr(soap, "Status", "200 OK")))
      return soap->error;
  }
  else
  { DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Error 500\n"));
    if (soap->master >= 0 || soap->socket >= 0) /* standalone application */
    { if (soap->version == 2 && strcmp(*soap_faultcode(soap), "SOAP-ENV:Sender"))
        sprintf(soap->tmpbuf, "HTTP/%s 400 Bad Request", soap->http_version);
      else
        sprintf(soap->tmpbuf, "HTTP/%s 500 Internal Server Error", soap->http_version);
      if ((soap->error = soap->fposthdr(soap, soap->tmpbuf, NULL)))
        return soap->error;
    }
    else if ((soap->error = soap->fposthdr(soap, "Status", "500 Internal Server Error")))
      return soap->error;
  }
  if ((soap->error = soap->fposthdr(soap, "Server", "gSOAP/2.2"))
   || (soap->error = soap_puthttphdr(soap, status, count)))
    return soap->error;
#ifdef WITH_COOKIES
  if (soap_putsetcookies(soap))
    return soap->error;
#endif
  return soap->error = soap->fposthdr(soap, NULL, NULL);
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_response(struct soap *soap, int status)
{ size_t count;
  soap->status = status;
  count = soap_begin_dime(soap);
  if (soap_begin_send(soap))
    return soap->error;
  if ((soap->mode & SOAP_IO) != SOAP_IO_STORE && !(soap->mode & SOAP_ENC_XML))
  { int n = soap->mode;
    soap->mode &= ~(SOAP_IO | SOAP_ENC_ZLIB);
    if ((n & SOAP_IO) != SOAP_IO_FLUSH)
      soap->mode |= SOAP_IO_BUFFER;
    if ((soap->error = soap->fresponse(soap, status, count)))
      return soap->error;
    if ((n & SOAP_IO) == SOAP_IO_CHUNK)
    { if (soap_flush(soap))
        return soap->error;
    }
    soap->mode = n;
  }
  if (soap->mode & SOAP_ENC_DIME)
    return soap_putdimehdr(soap);
  return SOAP_OK;
}

/******************************************************************************/
SOAP_FMAC1
void
SOAP_FMAC2
soap_set_fault(struct soap *soap)
{ const char **c = soap_faultcode(soap);
  const char **s = soap_faultstring(soap);
  const char **d = soap_faultdetail(soap);
  if (!*c)
  { if (soap->version == 2)
      *c = "SOAP-ENV:Sender";
    else
      *c = "SOAP-ENV:Client";
  }
  if (*s)
    return;
  switch (soap->error)
  { case SOAP_CLI_FAULT:
      *s = "Client fault";
      break;
    case SOAP_SVR_FAULT:
      *s = "Server fault";
      break;
    case SOAP_TAG_MISMATCH:
      sprintf(soap->msgbuf, "Tag mismatch: element '%s' does not correspond to expected element", soap->tag);
      *s = soap->msgbuf;
      break;
    case SOAP_TYPE_MISMATCH:
      sprintf(soap->msgbuf, "Data type '%s' mismatch in element '%s'", soap->type, soap->tag);
      *s = soap->msgbuf;
      break;
    case SOAP_SYNTAX_ERROR:
      *s = "XML syntax error";
      break;
    case SOAP_NO_TAG:
      *s = "No XML element tag found";
      break;
    case SOAP_MUSTUNDERSTAND:
      *c = "SOAP-ENV:MustUnderstand";
      sprintf(soap->msgbuf, "The data in element '%s' must be understood but cannot be handled", soap->tag);
      *s = soap->msgbuf;
      break;
    case SOAP_VERSIONMISMATCH:
      *c = "SOAP-ENV:VersionMismatch";
      *s = "SOAP version mismatch or invalid SOAP message";
      break;
    case SOAP_DIME_MISMATCH:
      *s = "DIME version mismatch";
      break;
    case SOAP_NAMESPACE:
      sprintf(soap->msgbuf, "Namespace URI mismatch in element '%s'", soap->tag);
      *s = soap->msgbuf;
      if (d)
        *d = "The namespace URI in the message was not found in the namespace table";
      break;
    case SOAP_OBJ_MISMATCH:
      *s = "Object mismatch";
      break;
    case SOAP_FATAL_ERROR:
      *s = "Fatal error";
      break;
    case SOAP_NO_METHOD:
      sprintf(soap->msgbuf, "Method '%s' not implemented", soap->tag);
      *s = soap->msgbuf;
      break;
    case SOAP_GET_METHOD:
      *s = "HTTP GET method not implemented";
      break;
    case SOAP_EOM:
      *s = "Out of memory";
      break;
    case SOAP_IOB:
      *s = "Array index out of bounds";
      break;
    case SOAP_NULL:
      sprintf(soap->msgbuf, "Cannot create nilable object for type '%s' in element '%s'", soap->type, soap->tag);
      *s = soap->msgbuf;
      if (d)
        *d = "The object is not nilable because the XML schema type for this element is not nilable";
      break;
    case SOAP_MULTI_ID:
      *s = "Non-unique id attribute";
      break;
    case SOAP_MISSING_ID:
      *s = "Missing id: referenced data is missing or had to be ignored";
      break;
    case SOAP_HREF:
      *s = "Invalid XML: object reference with href attribute is incompatible with actual object referred to";
      break;
    case SOAP_FAULT:
      break;
    case SOAP_TCP_ERROR:
      *s = tcp_error(soap);
      break;
    case SOAP_HTTP_ERROR:
      *s = "HTTP error";
      break;
    case SOAP_SSL_ERROR:
      *s = "SSL error";
      break;
    case SOAP_PLUGIN_ERROR:
      *s = "Plugin registry error";
      break;
    case SOAP_DIME_ERROR:
      *s = "DIME error";
      break;
    case SOAP_ZLIB_ERROR:
#ifdef WITH_ZLIB
      *s = "Zlib/gzip error";
      if (d)
        *d = soap->d_stream.msg;
#else
      *s = "Zlib not installed for required message (de)compression";
#endif
      break;
    case SOAP_EOD:
      *s = "End of DIME error";
      break;
    case SOAP_EOF:
      *s = "End of file or no input";
      if (d)
        *d = soap_strerror(soap, soap->errnum ? soap->errnum : soap_errno);
      break;
    default:
      *s = "Unknown error code";
    }
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_send_fault(struct soap *soap)
{ int status = soap->error;
  if (status == SOAP_STOP)
    return status;
  DBGLOG(TEST,SOAP_MESSAGE(fdebug, "Sending back fault struct for error code %d\n", soap->error));
  soap->keep_alive = 0; /* to terminate connection */
  soap_set_fault(soap);
  if (status != SOAP_EOF || soap_poll(soap) == SOAP_OK)
  { soap->error = SOAP_OK;
    soap_serializeheader(soap);
    soap_serializefault(soap);
    soap_begin_count(soap);
    if (soap->mode & SOAP_IO_LENGTH)
    { soap_envelope_begin_out(soap);
      soap_putheader(soap);
      soap_body_begin_out(soap);
      soap_putfault(soap);
      soap_body_end_out(soap);
      soap_envelope_end_out(soap);
    }
    if (soap_response(soap, status)
     || soap_envelope_begin_out(soap)
     || soap_putheader(soap)
     || soap_body_begin_out(soap)
     || soap_putfault(soap)
     || soap_body_end_out(soap)
     || soap_envelope_end_out(soap))
    { soap_closesock(soap);
      return soap->error;
    }
    if (soap->mode & SOAP_ENC_DIME)
      soap_send_raw(soap, "\0\0\0", -(int)soap->count&3);
    soap_end_send(soap);
  }
  soap_closesock(soap);
  return soap->error = status;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_recv_fault(struct soap *soap)
{ int status;
  DBGLOG(TEST,SOAP_MESSAGE(fdebug, "Receiving SOAP Fault\n"));
  status = soap->error;
  if (soap_getfault(soap))
  { DBGLOG(TEST,SOAP_MESSAGE(fdebug, "Error: soap_get_soapfault() failed. Is this a SOAP message at all?\n"));
    soap->error = status;
    if (soap->version == 2)
      *soap_faultcode(soap) = "SOAP-ENV:Sender";
    else
      *soap_faultcode(soap) = "SOAP-ENV:Client";
    soap_set_fault(soap);
    return soap->error;
  }
  /* Use the soap fault received to set soap->error to the appropriate value */
  if (!soap_match_tag(soap, *soap_faultcode(soap), "SOAP-ENV:Server") || !soap_match_tag(soap, *soap_faultcode(soap), "SOAP-ENV:Receiver"))
    status = SOAP_SVR_FAULT; 
  else if (!soap_match_tag(soap, *soap_faultcode(soap), "SOAP-ENV:Client") || !soap_match_tag(soap, *soap_faultcode(soap), "SOAP-ENV:Sender"))
    status = SOAP_CLI_FAULT;
  else if (!soap_match_tag(soap, *soap_faultcode(soap), "SOAP-ENV:MustUnderstand"))
    status = SOAP_MUSTUNDERSTAND;
  else if (!soap_match_tag(soap, *soap_faultcode(soap), "SOAP-ENV:VersionMismatch"))
    status = SOAP_VERSIONMISMATCH;
  else
  { DBGLOG(TEST,SOAP_MESSAGE(fdebug, "Fault code %s\n", *soap_faultcode(soap) ));
    status = SOAP_FAULT;
  }
  if (soap_body_end_in(soap) || soap_envelope_end_in(soap) || soap_end_recv(soap))
    return soap->error;
  soap_closesock(soap); 
  return soap->error = status;
}

/******************************************************************************/
SOAP_FMAC1
void
SOAP_FMAC2
soap_print_fault(struct soap *soap, FILE *fd)
{ if (soap->error)
  { const char **s = soap_faultdetail(soap);
    if (!*soap_faultcode(soap))
      soap_set_fault(soap);
    if (!*soap_faultstring(soap))
      *soap_faultstring(soap) = "";
    fprintf(fd, "SOAP FAULT: %s\n\"%s\"\n", *soap_faultcode(soap), *soap_faultstring(soap));
    if (s && *s)
      fprintf(fd, "Detail: %s\n", *s);
  }
}
 
/******************************************************************************/
SOAP_FMAC1
void
SOAP_FMAC2
soap_print_fault_location(struct soap *soap, FILE *fd)
{ int c;
  if (soap->error && soap->buflen > 0)
  { if (soap->bufidx == 0)
      soap->bufidx = 1;
    c = soap->buf[soap->bufidx-1];
    soap->buf[soap->bufidx-1] = '\0';
    soap->buf[soap->buflen-1] = '\0';
    if (soap->bufidx < soap->buflen)
      fprintf(fd, "%s%c\n** HERE **\n%s\n", soap->buf, c, soap->buf+soap->bufidx);
    else
      fprintf(fd, "%s%c\n** HERE **\n", soap->buf, c);
  }
}
 
/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_size(const int *size, int dim)
{ int i, n = size[0];
  for (i = 1; i < dim; i++)
    n *= size[i];
  return n;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_getoffsets(const char *attr, const int *size, int *offset, int dim)
{ int i, j = 0;
  if (offset)
    for (i = 0; i < dim && attr && *attr; i++)
    { attr++;
      j *= size[i];
      j += offset[i] = (int)atol(attr);
      attr = strchr(attr, ',');
    }
  else
    for (i = 0; i < dim && attr && *attr; i++)
    { attr++;
      j *= size[i];
      j += (int)atol(attr);
      attr = strchr(attr, ',');
    }
  return j;
}

/******************************************************************************/
SOAP_FMAC1
char *
SOAP_FMAC2
soap_putsize(struct soap *soap, const char *type, int size)
{ if (!type)
    return NULL;
  if (soap->version == 2)
    sprintf(soap->type, "%s[%d", type, size);
  else
    sprintf(soap->type, "%s[%d]", type, size);
  return soap->type;
}

/******************************************************************************/
SOAP_FMAC1
char *
SOAP_FMAC2
soap_putsizesoffsets(struct soap *soap, const char *type, const int *size, const int *offset, int dim)
{ int i;
  if (!type)
    return NULL;
  if (soap->version == 2)
  { sprintf(soap->type, "%s[%d", type, size[0]);
    for (i = 1; i < dim; i++)
      sprintf(soap->type + strlen(soap->type), " %d", size[i]);
  }
  else
  { sprintf(soap->type, "%s[%d", type, size[0]+offset[0]);
    for (i = 1; i < dim; i++)
      sprintf(soap->type + strlen(soap->type), ",%d", size[i] + offset[i]);
    strcat(soap->type, "]");
  }
  return soap->type;
}

/******************************************************************************/
SOAP_FMAC1
char *
SOAP_FMAC2
soap_putsizes(struct soap *soap, const char *type, const int *size, int dim)
{ int i;
  if (!type)
    return NULL;
  sprintf(soap->type, "%s[%d", type, size[0]);
  if (soap->version == 2)
  { for (i = 1; i < dim; i++)
      sprintf(soap->type + strlen(soap->type), " %d", size[i]);
  }
  else
  { for (i = 1; i < dim; i++)
      sprintf(soap->type + strlen(soap->type), ",%d", size[i]);
    strcat(soap->type, "]");
  }
  return soap->type;
}

/******************************************************************************/
SOAP_FMAC1
char *
SOAP_FMAC2
soap_putoffset(struct soap *soap, int offset)
{ sprintf(soap->arrayOffset, "[%d]", offset);
  return soap->arrayOffset;
}

/******************************************************************************/
SOAP_FMAC1
char *
SOAP_FMAC2
soap_putoffsets(struct soap *soap, const int *offset, int dim)
{ int i;
  sprintf(soap->arrayOffset, "[%d", offset[0]);
  for (i = 1; i < dim; i++)
    sprintf(soap->arrayOffset + strlen(soap->arrayOffset), ",%d", offset[i]);
  strcat(soap->arrayOffset, "]");
  return soap->arrayOffset;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_getsize(const char *attr1, const char *attr2, int *j)
{ int n, k;
  char *s;
  *j = 0;
  if (!*attr1)
    return -1;
  n = 1;
  do
  { attr1++;
    k = (int)strtol(attr1, &s, 10);
    if (s == attr1)
      return -1;
    n *= k;
    attr1 = strchr(s, ',');
    if (!attr1)
      attr1 = strchr(s, ' ');
    if (attr2 && *attr2)
    { attr2++;
      *j *= k;
      *j += (int)strtol(attr2, &s, 10);
      attr2 = s;
    }
  } while (attr1 && *attr1 != ']');
  return n - *j;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_getsizes(const char *attr, int *size, int dim)
{ int i, n;
  if (!*attr)
    return -1;
  i = strlen(attr);
  n = 1;
  do
  { for (i = i-1; i >= 0; i--)
      if (attr[i] == '[' || attr[i] == ',' || attr[i] == ' ')
        break;
    n *= size[--dim] = (int)atol(attr + i + 1);
  } while (i >= 0 && attr[i] != '[');
  return n;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_getposition(const char *attr, int *pos)
{ int i, n;
  if (!*attr)
    return -1;
  n = 0;
  i = 1;
  do
  { pos[n++] = (int)atol(attr + i);
    while (attr[i] && attr[i] != ',' && attr[i] != ']')
      i++;
    if (attr[i] == ',')
      i++;
  } while (n < 32 && attr[i] && attr[i] != ']');
  return n;
}

/******************************************************************************/
SOAP_FMAC1
int
SOAP_FMAC2
soap_register_plugin_arg(struct soap *soap, int (*fcreate)(struct soap*, struct soap_plugin*, void*), void *arg)
{ struct soap_plugin *p;
  int r;
  p = (struct soap_plugin*)malloc(sizeof(struct soap_plugin));
  p->id = NULL;
  p->data = NULL;
  p->fcopy = NULL;
  p->fdelete = NULL;
  if (!p)
    return soap->error = SOAP_EOM;
  r = fcreate(soap, p, arg);
  if (!r && p->fcopy && p->fdelete)
  { p->next = soap->plugins;
    soap->plugins = p;
    DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Registered '%s' plugin\n", p->id));
    return SOAP_OK;
  }
  free(p);
  DBGLOG(TEST, SOAP_MESSAGE(fdebug, "Could not register plugin '%s': plugin returned error %d (or fcopy and fdelete callbacks were not set)\n", p->id?p->id:"?", r));
  return r;
}

/******************************************************************************/
static void*
fplugin(struct soap *soap, const char *id)
{ struct soap_plugin *p;
  for (p = soap->plugins; p; p = p->next)
    if (p->id == id || !strcmp(p->id, id))
      return soap->plugins->data;
  return NULL;
}

/******************************************************************************/
SOAP_FMAC1
void *
SOAP_FMAC2
soap_lookup_plugin(struct soap *soap, const char *id)
{ return soap->fplugin(soap, id);
}

/******************************************************************************/
#ifdef __cplusplus
}
#endif

