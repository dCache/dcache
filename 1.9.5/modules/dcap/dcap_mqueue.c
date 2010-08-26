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
 * $Id: dcap_mqueue.c,v 1.22 2004-11-01 19:33:29 tigran Exp $
 */
#include <stdlib.h>
#include <string.h>
#include "dcap_types.h"
#include "dcap_debug.h"
#include "sysdep.h"


#define QLEN 2 /* default/initial size of queue */


static messageQueue *queueList = NULL;

static unsigned int qListLen = 0; /* number of elements in the memory*/
static unsigned int qMemLen = 0;  /* number of allocated rooms for elements */


static MUTEX(gLock);

messageQueue *newQueue(unsigned int id)
{
	messageQueue *tmpQueueList;
	
	
	m_lock(&gLock);
	
	dc_debug(DC_INFO, "Allocated message queues %d, used %d\n", qMemLen,qListLen );
	if(qMemLen == qListLen) {
		tmpQueueList = (messageQueue *)realloc(queueList, sizeof(messageQueue) * (qMemLen + 1));
		if (tmpQueueList == NULL) {
			m_lock(&gLock);
			return NULL;
		}
		queueList = tmpQueueList;
		qMemLen++;
	}
    /* allocate memory for message queue */
	queueList[qListLen].mQueue = (asciiMessage **)malloc(sizeof(asciiMessage *) * QLEN);
	if (queueList[qListLen].mQueue == NULL) {				
		m_lock(&gLock);
		return NULL;
	}
	
	queueList[qListLen].qLen = QLEN;		/* set default queue size */
	queueList[qListLen].mnum = 0;		/* no messages at beginning */
	queueList[qListLen].id = id;			/* lets know who we are */
	m_init(&queueList[qListLen].lock);	/* initialize queue lock */
	
	qListLen++;
	
	/* reuse of tmpQueueList*/
	
	tmpQueueList = &queueList[qListLen -1];
	m_unlock(&gLock);
	
	return tmpQueueList;
  
}


int queueAddMessage(unsigned int destination, asciiMessage *msg)
{
	register unsigned int i;
	asciiMessage **tmpQueue;

	/* no action for NULL message */
	if( msg == NULL ) return -1;
	
	m_lock(&gLock);
	for(i = 0; i < qListLen; i++) {
		if(queueList[i].id == destination) {
			m_lock(&queueList[i].lock);
			
			/* reallocate message queue, if no room available */
			if(queueList[i].mnum == queueList[i].qLen) {
				tmpQueue = (asciiMessage **)realloc(queueList[i].mQueue,
						sizeof(asciiMessage *) * (queueList[i].qLen +1));
				if(tmpQueue == NULL){
					m_unlock(&queueList[i].lock);
					m_unlock(&gLock);
					return -1;
				}
				
				queueList[i].mQueue = tmpQueue;
				queueList[i].qLen++;
			}
			
			queueList[i].mQueue[queueList[i].mnum] = msg;
			queueList[i].mnum++;
			
			m_unlock(&queueList[i].lock);
			m_unlock(&gLock);
			return 0;
		}
	}

	/* if we are here, then the message for unexisting destination */

	m_unlock(&gLock);
	/* remove message, nobody needs it*/
	if( msg->msg != NULL ) {
		free(msg->msg);
	}
	free(msg);
	
	return -1;
}


int queueGetMessage(unsigned int destination, asciiMessage **msg)
{

	register unsigned int i;
	
	m_lock(&gLock);
	for( i=0; i < qListLen; i++) {
		if(queueList[i].id == destination) {
			m_lock(&queueList[i].lock);
            if(!queueList[i].mnum) {
			   m_unlock(&queueList[i].lock);
				m_unlock(&gLock);
			   return -1; 
			}			
			*msg = queueList[i].mQueue[0];
			
			/* if there are more messages in the queue, shift them */
			if(queueList[i].mnum >1) {
				memmove(&queueList[i].mQueue[0], &queueList[i].mQueue[1],
							sizeof(asciiMessage *) * (queueList[i].mnum - 1) );
			}
			queueList[i].mnum--;
			m_unlock(&queueList[i].lock);
			m_unlock(&gLock);
			return 0;
		}
	}
	m_unlock(&gLock);	
	return -1;
}


void deleteQueue(unsigned int id)
{

	register unsigned int i;
		
	m_lock(&gLock);
	
	/* if queue is empty - return */
	if (!qListLen) {	
		m_unlock(&gLock);
		return;
	}
	
	for(i = 0; i < qListLen; i++) {
		if(queueList[i].id == id) {
		
			dc_debug(DC_INFO, "Removing unneeded queue [%d]", id);
			
			m_lock(&queueList[i].lock);
			free(queueList[i].mQueue);
			m_unlock(&queueList[i].lock);
			
			/* if i is not last element, then shift the array */
			if( i != qListLen -1) {
				memmove(&queueList[i], &queueList[i+1], sizeof(messageQueue)*(qListLen - i - 1));
			}
			
			qListLen--;
			m_unlock(&gLock);
			return;
		}
	}

	dc_debug(DC_ERROR, "Trying to delete unexisting queue");
	m_unlock(&gLock);
	return;
  
}
