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
 * $Id: dispatcher.c,v 1.6 2004-11-01 19:33:29 tigran Exp $
 */
#ifndef _REENTRANT
#define _REENTRANT
#endif				/* _REENTRANT */
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <signal.h>
#include <string.h>


#if defined(sun) && !defined(USE_PTHREAD)
#include <thread.h>
#include <synch.h>
#else
#include <pthread.h>
#endif				/* #if defined(sun) && !defined(USE_PTHREAD) */


#define RAND(x)  (random()%(x) +1)

#define QLEN 2
#define CNUM 5

#if defined(sun) && !defined(USE_PTHREAD)

static mutex_t  gLock = DEFAULTMUTEX;
static mutex_t  cLock = DEFAULTMUTEX;
static cond_t   gCond = DEFAULTCV;

#define m_init(x) mutex_init(x, USYNC_THREAD, NULL);
#define m_lock(x) mutex_lock(x)
#define m_unlock(x) mutex_unlock(x)
#define m_trylock(x) mutex_trylock(x)

#define c_wait(a,b) cond_wait(a,b)
#define c_broadcast(x) cond_broadcast(x)

#else

static pthread_mutex_t gLock = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t cLock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t gCond = PTHREAD_COND_INITIALIZER;

#define m_init(x) pthread_mutex_init(x,  NULL);
#define m_lock(x) pthread_mutex_lock(x)
#define m_unlock(x) pthread_mutex_unlock(x)
#define m_trylock(x) pthread_mutex_trylock(x)

#define c_wait(a,b) pthread_cond_wait(a,b)
#define c_broadcast(x) pthread_cond_broadcast(x)

#endif				/* #if defined(sun) && !defined(USE_PTHREAD) */


typedef struct {
	short           destination;
	char           *body;
	int             len;
}               message;

typedef struct {
	message       **mQueue;
	int             qLen;	/* queue length */
	int             mnum;	/* message number */
	int             ID;
#if defined(sun) && !defined(USE_PTHREAD)
	mutex_t         lock;
#else
	pthread_mutex_t lock;
#endif				/* #if defined(sun) && !defined(USE_PTHREAD) */
}               client;


typedef struct {
	int             myID;
	int             fd;
}               chain;


client          clients[CNUM];

static int      mTotal = 0;


static int      getMessage(int, int, client *, message *);


void           *
thread_task(void *arg)
{

	chain          *myChain;
	message        *out;
	int             i;

	myChain = (chain *) arg;



	while (1) {
		i = getMessage(myChain->fd, myChain->myID, clients, out);
		if (i < 0) {
			m_lock(&cLock);

			if (mTotal == 0) {
				break;
			} else {
				m_unlock(&cLock);
				continue;
			}
		}
		mTotal--;
		m_unlock(&cLock);

		free(out);
	}

	free(myChain);
	return NULL;
}


int 
main()
{


	int             Pipe[2];
	int             pid;
	message         msg;
	int             i;
	int             k;
#if defined(sun) && !defined(USE_PTHREAD)
	thread_t        wthread[CNUM];
#else
	pthread_t       wthread[CNUM];
#endif
	chain          *c;


	/* Patrick did not like Pipes, but it's easy to use them... */
	if (pipe(Pipe) < 0) {
		perror("pipe");
		exit(2);
	}
	/*
	 * write to Pipe[1] read from Pipe[0]
	 */

	pid = fork();

	if (pid < 0) {
		perror("fork");
		exit(1);
	}
	/* take it easy... */
	signal(SIGPIPE, SIG_IGN);

	if (pid) {		/* parent */
		/* this process will write to the pipe */

		/* close unneeded pipe descriptor */
		close(Pipe[0]);
		srandom(pid);

		for (i = 0; i < 50; i++) {
			msg.destination = RAND(CNUM) - 1;
			write(Pipe[1], &msg, sizeof(message));
			sleep(1);
		}
		close(Pipe[1]);
		exit(0);

	} else {		/* child */
		/* this process will read from the pipe */

		/* close unneeded pipe descriptor */
		close(Pipe[1]);
		srandom(getppid());


		for (i = 0; i < CNUM; i++) {
			clients[i].mQueue =
				(message **) malloc(sizeof(message *) * QLEN);
			if (clients[i].mQueue == NULL) {
				perror("malloc");
				exit(4);
			}
			clients[i].qLen = QLEN;
			clients[i].mnum = 0;
			clients[i].ID = i + 1;
			m_init(&clients[i].lock);

			/* Just do it... with threads */

			c = (chain *) malloc(sizeof(chain));
			if (c == NULL) {
				perror("malloc");
				exit(4);
			}
			c->fd = Pipe[0];
			c->myID = i;
#if defined(sun) && !defined(USE_PTHREAD)
			thr_create(NULL, 0, thread_task, (void *) c, 0, &wthread[i]);
#else
			pthread_create(&wthread[i], NULL, thread_task, (void *) c);
#endif
		}

		/* lets wait untill all threads will finish */

#if defined(sun) && !defined(USE_PTHREAD)
		while (thr_join(0, NULL, NULL) == 0);
#else
		for (i = 0; i < CNUM; i++)
			pthread_join(wthread[i], NULL);
#endif




		for (i = 0; i < CNUM; i++) {
			printf("Client %d have %d messages\n", i, clients[i].mnum);
		}
		fflush(stdout);

		/* Final cleanup */

		for (i = 0; i < CNUM; i++) {
			for (k = 0; k < clients[i].mnum; k++) {
				free(clients[i].mQueue[k]);
			}
			free(clients[i].mQueue);
		}


		exit(0);
	}

}


static int 
getMessage(int fd, int myID, client * clnt, message * out)
{

	int             n;
	message        *msg;
	message       **tmp;
	int             destination;


	while (1) {
		m_lock(&clnt[myID].lock);
		if (clnt[myID].mnum) {
			
			/* printf("Hey! I([%d]) have message in the Queue. Skeep scanning...\n", myID); */
			 

			out = clnt[myID].mQueue[0];
			if (clnt[myID].mnum > 1) {
				memmove(&clnt[myID].mQueue[0], &clnt[myID].mQueue[1],
					sizeof(&clnt[myID].mQueue[0]) * (clnt[myID].mnum -
									 1));
			}
			clnt[myID].mnum -= 1;
			m_unlock(&clnt[myID].lock);
			return 0;
		}
		m_unlock(&clnt[myID].lock);

		if (m_trylock(&gLock) == 0) {	/* we got the lock, and doing
						 * wat ever we want ... */
			printf("\tMessages for clinets:");

			while (1) {
				msg = (message *) malloc(sizeof(message));
				if (msg == NULL) {
					perror("malloc");
					m_unlock(&gLock);
					c_broadcast(&gCond);
					return -1;
				}
				n = read(fd, msg, sizeof(message));
				if (n <= 0) {
					printf("\n");
					fflush(stdout);
					m_unlock(&gLock);
					c_broadcast(&gCond);
					return -1;
				}
				m_lock(&cLock);
				mTotal++;
				m_unlock(&cLock);

				destination = msg->destination;

				m_lock(&clnt[destination].lock);
				if (clnt[destination].mnum == clnt[destination].qLen) {
					tmp =
						(message **) realloc(clnt[destination].mQueue, sizeof(message *) *
					      (clnt[destination].qLen + 1));
					if (tmp == NULL) {
						perror("malloc");
						free(msg);
						m_unlock(&gLock);
						c_broadcast(&gCond);
						m_unlock(&clnt[destination].lock);
						return -1;
					}
					
					clnt[destination].mQueue = tmp;
					clnt[destination].qLen += 1;

				}
				clnt[destination].mQueue[clnt[destination].mnum] = msg;
				clnt[destination].mnum += 1;

				if (destination == myID) {
					printf(" [%d](my self).\n", myID);
					m_unlock(&gLock);
					c_broadcast(&gCond);


					out = clnt[myID].mQueue[0];
					if (clnt[myID].mnum > 1) {
						memmove(&clnt[myID].mQueue[0],
						      &clnt[myID].mQueue[1],
							sizeof(message *) *
						     (clnt[myID].mnum - 1));
					}
					clnt[myID].mnum -= 1;
					m_unlock(&clnt[myID].lock);
					return 0;
				} else {
					printf(" [%d]", destination);
				}

				fflush(stdout);
				m_unlock(&clnt[destination].lock);
				c_broadcast(&gCond);
			}

		} else {

			/*
			 * mutex aready locked. we have to wait untill
			 * somebody will done out tasks.
			 */


/*			printf("Everything is locked...I([%d]) will wait.\n", myID); */

			c_wait(&gCond, &clnt[myID].lock);
			m_unlock(&clnt[myID].lock);
		}
	}
}
