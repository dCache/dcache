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
 * $Id: input_parser.h,v 1.4 2004-11-01 19:33:30 tigran Exp $
 */
#ifndef INPUT_PARSER_H
#define INPUT_PARSER_H

#include "ioTunnel.h"
extern char **inputParser(int, ioTunnel *);

#endif
