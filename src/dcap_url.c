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
 * $Id: dcap_url.c,v 1.17 2005-08-15 10:05:03 tigran Exp $
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <netdb.h>

#include "dcap.h"
#include "dcap_types.h"
#include "dcap_errno.h"
#include "dcap_error.h"
#include "dcap_url.h"
#include "debug_level.h"
#include "xutil.h"
#include "dcap_str_util.h"

#define DCAP_PREFIX "dcap://"
#define PNFS_PREFIX "pnfs://"
#define DEFAULT_DOOR "dcache"


int isUrl( const char *path)
{

	return (strstr(path, DCAP_PREFIX) != NULL ) || (strstr(path, PNFS_PREFIX) != NULL ) ;

}


dcap_url* dc_getURL( const char *path )
{

	dcap_url *url;
	char *s;
	char *w;
	char *host;
	int host_len;
	int type = URL_NONE;
	int def_door_len;
	char *domain;
	struct servent *se;
	short port;


	if(path == NULL) {
		dc_errno = DEURL;
		return NULL;
	}


	s = strstr(path, DCAP_PREFIX);
	if( s != NULL ) {
		type = URL_DCAP;
	}else{

		s = strstr(path, PNFS_PREFIX);
		if( s != NULL ) {
			type = URL_PNFS;
		}

	}


	if( (type != URL_DCAP) && (type != URL_PNFS) ) {
		dc_errno = DEURL;
		return NULL;
	}

	url = (dcap_url *)malloc(sizeof(dcap_url));
	if(url == NULL) {
		dc_debug(DC_ERROR, "Failed to allocate dcap_url for %s", path);
		return NULL;
	}


	url->host = NULL;
	url->file = NULL;
	url->prefix = NULL;
	url->type= type;


	if( s != path ) {
		url->prefix = (char *)xstrndup(path, s - path );
	}else{
		s = (char *)path;
	}

	/* now s is a pointer to url without prefix */

	s = (char *)(s + strlen(DCAP_PREFIX));


	/* w points to a first / in the path */
	w = strchr(s, '/');
	if(w == NULL) {
		free(url);
		return NULL;
	}

	url->file = strdup(w + 1);

	host_len = w-s;

    if( host_len != 0 ) {

		host = xstrndup(s, host_len );

		if(host == NULL) {
			dc_debug(DC_ERROR, "Failed to duplicate host in url %s", path);
			free(url);
			return NULL;
		}


		/* if port not specified, take it from /etc/services or fall back to default */
		w = strchr(host, ':');
		if( w == NULL ) {
			w = strchr(path, ':');
			w = xstrndup(path, w - path);
			se = getservbyname(w, "tcp");
			free(w);
			port = se ? ntohs(se->s_port) : DEFAULT_DOOR_PORT;
			url->host = malloc(host_len + 1 + 8);
			url->host[0] = '\0';
			sprintf(url->host, "%s:%d", host, port );
			free(host);
		}else{
			url->host = host;
		}


	}else{

		if( url->type == URL_PNFS ) {
			free(url);
			return NULL;
		}

		domain = strchr(w + 1, '/');
		domain++;
		w = strchr(domain , '/');
		if( w == NULL ) {
			w = (char *) domain + strlen(domain);
		}

		host_len = w - domain ;
		def_door_len = strlen(DEFAULT_DOOR);


		url->host = (char *)malloc( def_door_len + host_len + host_len >  0 ? 2 : 1);
		if(url->host == NULL) {
			dc_debug(DC_ERROR, "Failed to allocate hostname for %s", path);
			free(url);
			return NULL;
		}

		memcpy(url->host, DEFAULT_DOOR , def_door_len);
		if(host_len) {
			memcpy(url->host + def_door_len, ".", 1);
		}
		memcpy(url->host + def_door_len +1, domain, host_len);
		url->host[host_len + def_door_len +1] = '\0';

	}

	return url;
}

char * url2config(char *configLine, size_t configLineSize, dcap_url *url)
{

	snprintf(configLine, configLineSize, "%s", url->host);
	configLine[configLineSize-1] = '\0';

	if ( url->prefix != NULL ) {
		dc_snaprintf(configLine, configLineSize, ":lib%sTunnel.so", url->prefix );
	}

	return configLine;
}

