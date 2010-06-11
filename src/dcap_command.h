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
 * $Id: dcap_command.h,v 1.11 2006-07-17 15:13:36 tigran Exp $
 */

#ifndef DCAP_COMMAND_H
#define DCAP_COMMAND_H

#include "dcap_types.h"

extern int do_command_fail   (char **, asciiMessage *);
extern int do_command_dummy  (char **, asciiMessage *);
extern int do_command_reject (char **, asciiMessage *);
extern int do_command_byebye (char **, asciiMessage *);
extern int do_command_welcome(char **, asciiMessage *);
extern int do_command_ok     (char **, asciiMessage *);
extern int do_command_retry  (char **, asciiMessage *);
extern int do_command_pong   (char **, asciiMessage *);
extern int do_command_stat   (char **, asciiMessage *);
extern int do_command_shutdown   (char **, asciiMessage *);
extern int do_command_connect   (char **, asciiMessage *);

#endif
