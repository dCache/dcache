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
 * $Id: string2stat.c,v 1.10 2005-08-15 10:05:03 tigran Exp $
 */


#ifdef WIN32
void string2stat64( const char **arg,  struct _stati64 *s );
#else
void string2stat64( const char **arg,  struct stat64 *s );
#endif
