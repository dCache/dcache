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
 * $Id: io.h,v 1.5 2004-11-01 19:33:30 tigran Exp $
 */
#ifndef IO_H
#define IO_H

#include "ioTunnel.h"

extern int             readln(int, char *, int, ioTunnel *);
extern int             readn(int, char *, int, ioTunnel *);
extern int             writen(int, const char *, int, ioTunnel *);
extern int             writeln(int, const char *, int, ioTunnel *);

#endif
