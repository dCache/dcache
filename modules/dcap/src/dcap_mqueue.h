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
 * $Id: dcap_mqueue.h,v 1.8 2004-11-01 19:33:29 tigran Exp $
 */
#ifndef DCAP_MQUEUE_H
#define DCAP_MQUEUE_H

#include "dcap_types.h"

extern messageQueue *newQueue(unsigned int);
extern int queueAddMessage(unsigned int, asciiMessage *);
extern int queueGetMessage(unsigned int, asciiMessage **);
extern void deleteQueue(unsigned int);

#endif
