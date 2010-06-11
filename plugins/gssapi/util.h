/*
 * $Id: util.h,v 1.1 2002-10-14 10:31:36 cvs Exp $
 */

#ifndef DCAP_GSS_UTIL_H
#define DCAP_GSS_UTIL_H

#include <gssapi.h>
#ifdef MIT_KRB5
#include <gssapi_krb5.h>
#include <gssapi_generic.h>
#endif /* MIT_KRB5 */

extern int asprintf (char **ret, const char *format, ...);
extern void gss_print_errors(int);
extern void sockaddr_to_gss_address (const struct sockaddr *, OM_uint32 *, gss_buffer_desc *);

#endif
