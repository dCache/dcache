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
 * $Id: links.c,v 1.14 2004-11-01 19:33:30 tigran Exp $
 */


#ifdef WIN32
#include <string.h>

char *followLink( const char *path)
{
	return _strdup(path);
}

#else

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <string.h>
#include <limits.h>
#include <sys/param.h>


char *followLink( const char *path)
{
	char *real_path;
	char *ret;
	int path_max;

#ifdef PATH_MAX
	path_max = PATH_MAX;
#else
	path_max = pathconf(path, _PC_PATH_MAX);
	if (path_max <= 0)
		path_max = 4096;
#endif

	real_path = malloc(path_max);
	if( real_path == NULL ) {
		return NULL;
	}

	ret = realpath(path, real_path);

	if ( ret == NULL ) {
		free(real_path);
	}

	return ret;
}

#endif /* WIN32 */
