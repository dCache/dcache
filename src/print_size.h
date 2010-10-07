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
 * $Id: print_size.h.in,v 1.57 2007-07-09 19:35:09 tigran Exp $
 */

void dc_bytes_as_size( char *buffer, off64_t size);
void dc_bytes_as_size_with_max_fraction( char *buffer, off64_t size,
					 double max_fraction);
