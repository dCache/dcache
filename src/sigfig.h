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
 * $Id: sigfig.h.in,v 1.57 2007-07-09 19:35:09 tigran Exp $
 */

#include <sys/types.h>

void dc_print_with_sigfig( char *buffer, size_t buffer_size, int sigfigs,
			   double value);
