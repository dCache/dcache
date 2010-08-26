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
 * $Id: dcap_url.h,v 1.5 2004-11-01 19:33:29 tigran Exp $
 */
#ifndef DCAP_URL_H
#define DCAP_URL_H

#include "dcap_types.h"

extern dcap_url *dc_getURL(const char *);
extern char *url2config( dcap_url *, char *);
extern int isUrl(const char *);

#endif /* DCAP_URL_H */
