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
 * $Id: dcap_accept.c,v 1.10 2004-11-01 19:33:28 tigran Exp $
 */

#include <stdlib.h>
#include <string.h>
#include "dcap_types.h"
#include "dcap_debug.h"
#include "sysdep.h"

/*
	this code is not thread-safe. The callers have to take care
	about synchronization.
*/


static acceptSocket *accepted = NULL;

static unsigned int qLen = 0; /* number of elements in the memory*/

int queueAddAccepted(int id, int sock)
{
	acceptSocket * tmp;

	tmp = realloc(accepted, sizeof(acceptSocket)*(qLen +1));
	if(tmp == NULL) {
		return -1;
	}

	accepted = tmp;
	accepted[qLen].sock = sock;
	accepted[qLen].id = id;

	++qLen;

	return 0;
}


int queueGetAccepted(int id)
{

	register unsigned int i;
	acceptSocket * tmp;
	int s;

	for(i = 0; i < qLen; i++) {
		if(accepted[i].id == id) {
			s = accepted[i].sock;

			if(qLen == 1) { /* last element */
				free(accepted);
				accepted = NULL;
				qLen = 0;
			}else{

				tmp = malloc(sizeof(acceptSocket)*(qLen - 1));

				if(tmp == NULL) {
					dc_debug(DC_ERROR, "Failed to allocate memory.");
					return s;
				}

				memcpy(tmp, accepted, sizeof(acceptSocket)*i);
				memcpy(&tmp[i], &accepted[i+1], sizeof(acceptSocket)*(qLen -i -1));
				free(accepted);
				accepted = tmp;
				--qLen;
			}
			return s;
		}
	}

	return -1;
}
