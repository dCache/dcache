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
 * $Id: linux_trace.c,v 1.2 2004-11-01 19:33:30 tigran Exp $
 */


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <setjmp.h>

#if __GNUC__ < 2
#error you need gcc >= 2 to use this abomination
#endif


#undef ulong
#define ulong unsigned long

#ifndef  MAX_STACK
#define MAX_STACK 40
#endif


#define retaddr(n)                                       \
	if((__builtin_frame_address((n)) != 0L) && (n < MAX_STACK) ) { \
        results[n] = (ulong)__builtin_return_address(n);  \
	   ++(*max); \
	   }else{ \
             goto done; \
	   }


void getStackTrace(unsigned long *results, int *max)
{

    retaddr(0);
    retaddr(1);
    retaddr(2);
    retaddr(3);
    retaddr(4);
    retaddr(5);
    retaddr(6);
    retaddr(7);
    retaddr(8);
    retaddr(9);
    retaddr(10);
    retaddr(11);
    retaddr(12);
    retaddr(13);
    retaddr(14);
    retaddr(15);
    retaddr(16);
    retaddr(17);
    retaddr(18);
    retaddr(19);
    retaddr(20);
    retaddr(21);
    retaddr(22);
    retaddr(23);
    retaddr(24);
    retaddr(25);
    retaddr(26);
    retaddr(27);
    retaddr(28);
    retaddr(29);
    retaddr(30);
    retaddr(31);
    retaddr(32);
    retaddr(33);
    retaddr(34);
    retaddr(35);
    retaddr(36);
    retaddr(37);
    retaddr(38);
    retaddr(39);

  done:

    return;

}

