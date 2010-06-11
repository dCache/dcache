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
 * $Id: currentThread.c,v 1.4 2004-11-01 19:33:28 tigran Exp $
 */


#include <stdio.h>
#include <pthread.h>


void currentThread()
{
	static pthread_t last;
	static int count ;
	static int init;


	if( init == 0 ) {
		last = pthread_self();
		init ++;
		count++;
	}else{
		if( last == pthread_self() ) {
			count ++;
		}else{
			fprintf(stderr, "Thread [%d] hits %d times", last, count);
			last = pthread_self();
			count = 1;
		}
	}
}
