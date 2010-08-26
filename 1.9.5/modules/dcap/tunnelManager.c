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
 * $Id: tunnelManager.c,v 1.11 2004-11-01 19:33:30 tigran Exp $
 */

#ifndef WIN32
#   include <dlfcn.h>
#   include <unistd.h>
#else
#    include "win32_dlfcn.h"
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "sysdep.h"
#include "ioTunnel.h"
#include "dcap_debug.h"

typedef struct {
	int sock;
	ioTunnel *tunnel;
} tunnelPair;

static MUTEX(gLock);

static tunnelPair *tunnelMap;
static unsigned int qLen = 0; /* number of elements in the memory*/


int setTunnelPair(int sock, ioTunnel *tunnel)
{
	tunnelPair *tmp;
	m_lock(&gLock);
	
	tmp = realloc(tunnelMap, sizeof(tunnelPair)*(qLen +1));
	if(tmp == NULL) {
		m_unlock(&gLock);
		return -1;
	}
	
	tunnelMap = tmp;
	tunnelMap[qLen].sock = sock;
	tunnelMap[qLen].tunnel = tunnel;
	
	++qLen;
	
	m_unlock(&gLock);
	return 0;
}

ioTunnel * getTunnelPair(int sock)
{

	unsigned int i;
	ioTunnel *en;
	
	m_lock(&gLock);
	for(i = 0; i < qLen; i++) {
		if(tunnelMap[i].sock == sock) {		
			en =  tunnelMap[i].tunnel;
			m_unlock(&gLock);
			return en;
		}
	}
	m_unlock(&gLock);
	return NULL;
}


ioTunnel *addIoPlugin(const char *libname)
{
	void *handle;
	ioTunnel *tunnel;

	if(libname == NULL) {
		dc_debug(DC_ERROR, "Bad tunnel name");
		return NULL;
	}

	/* magick library name, do nothing */
	if( strcmp(libname, "null") == 0 ) {
		return NULL;
	}

	handle = dlopen( libname, RTLD_NOW);
		
	if(handle == NULL) {		
		goto fail;
	}

	tunnel = (ioTunnel *)malloc(sizeof(ioTunnel));
	if(tunnel == NULL) {
		dc_debug(DC_ERROR, "Failed to allocate memory for tunnel");
		dlclose(handle);
		return NULL;
	}

	
	tunnel->eRead = (ssize_t (*)(int , void *, size_t ))dlsym(handle, "eRead");
	if(tunnel->eRead == NULL) goto fail;
	
	tunnel->eWrite = (ssize_t (*)(int ,const void *, size_t ))dlsym(handle, "eWrite");
	if(tunnel->eWrite == NULL) goto fail;
	
	tunnel->eInit = (int(*)(int))dlsym(handle, "eInit");	
	if(tunnel->eInit == NULL) goto fail;
	
	tunnel->eDestroy = (int(*)(int))dlsym(handle, "eDestroy");
	if(tunnel->eDestroy == NULL) goto fail;
		

	dc_debug(DC_INFO, "Activating IO tunnel. Provider: [%s].", libname);
		
	return tunnel;
	
fail:
	dc_debug(DC_ERROR, "Failed to add IO tunnel (%s). Provider: [%s].", dlerror(), libname);

	if(handle != NULL ) {
		dlclose(handle);
	}
	return NULL;	
	
}
