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
 * $Id: sysdep.h,v 1.11 2004-11-01 19:33:30 tigran Exp $
 */
#ifndef SYS_DEP_H
#define SYS_DEP_H

#ifndef NOT_THREAD_SAFE

#include <pthread.h>

#define MUTEX(x) pthread_mutex_t x = PTHREAD_MUTEX_INITIALIZER
#define COND(x)  pthread_cond_t  x = PTHREAD_COND_INITIALIZER
#define RDLOCK(x) pthread_rwlock_t x = PTHREAD_RWLOCK_INITIALIZER
#define TKEY pthread_key_t

#define m_init(x) pthread_mutex_init(x,  NULL)
#define m_lock(x) pthread_mutex_lock(x)
#define m_unlock(x) pthread_mutex_unlock(x)
#define m_trylock(x) pthread_mutex_trylock(x)

#define c_wait(a,b) pthread_cond_wait(a,b)
#define c_broadcast(x) pthread_cond_broadcast(x)

#define t_keycreate(a,b) pthread_key_create(a,b)
#define t_setspecific(a,b) pthread_setspecific(a,b)
#define t_getspecific(a,b)  *b = pthread_getspecific(a)


#define rw_wrlock(x) pthread_rwlock_wrlock(x)
#define rw_rdlock(x) pthread_rwlock_rdlock(x)
#define rw_unlock(x) pthread_rwlock_unlock(x)

#else /* NOT_THREAD_SAFE */

#define MUTEX(x) int x
#define COND(x)  int x
#define RDLOCK(x) int x

#define m_init(x) {}
#define m_lock(x) {}
#define m_unlock(x) {}
#define m_trylock(x) 0

#define c_wait(a,b) {}
#define c_broadcast(x) {}


#define rw_wrlock(x) {}
#define rw_rdlock(x) {}
#define rw_unlock(x) {}

#endif /* NOT_THREAD_SAFE */
#endif
