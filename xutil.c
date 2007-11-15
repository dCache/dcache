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
 * $Id: xutil.c,v 1.2 2004-11-01 19:33:30 tigran Exp $
 */
#include <string.h>
#include <stdlib.h>


/*
    set of functions, wich missing on some system,
	have a different behavior or has a bugs in imlementation.

 */



#ifndef PATH_SEPARATOR
#   ifdef WIN32
#       define PATH_SEPARATOR '\\'
#   else
#       define PATH_SEPARATOR '/'
#   endif /* WIN32 */
#endif /*PATH_SEPARATOR */



/* there is no strndup in standart C library */
char *xstrndup( const char *source, size_t size)
{
#if defined(linux) && defined(_GNU_SOURCE)
	return strndup(source, size);
#else /* ! defined(linux) && defined(_GNU_SOURCE) */

	char *new_string = NULL;
	int len;
	
	len = strlen(source);
	
	
	if( size < len ) {
		new_string  = malloc(size +1);
		if( new_string == NULL ) {
			return NULL;
		}
		
		memcpy(new_string, source, size);
		new_string[size] = '\0';
	}else{ /* size >= len */ 
	
		new_string  = malloc(len +1);
		if( new_string == NULL ) {
			return NULL;
		}
		
		memcpy(new_string, source, len);
		new_string[len] = '\0';
	}
	
	return new_string;

#endif /* defined(linux) && defined(_GNU_SOURCE) */
}



char * xbasename( const char *path)
{


	char *fname, *s;

	if( path == NULL ) {
		return NULL ;
	}


	fname = (char *)strrchr(path, PATH_SEPARATOR);
	
	
	/* only file name */
	
	if( fname == NULL ) {
		return strdup(path);
	}

	/*  /path */

	if( fname == path ) {
		
		/* only / */
		if(strlen(path) == 1 ) {
			return strdup(path);
		}
		return strdup(path +1);
	}
	
	/* / at end */

	if( fname == path + strlen(path) -1) {
		s = xstrndup( path , strlen(path) -1 );
		fname = xbasename(s);
		free(s);
		return fname;
	}
	
	return strdup ( fname + 1 );

}


char *xdirname ( const char *path)
{
	char *dir, *s;

	if( path == NULL ) {
		return NULL;
	}


	dir = (char *)strrchr(path, PATH_SEPARATOR);
	
	/* only file name specified */
	if( dir == NULL ) {
		return strdup(".");
	}

	/* /path */

	if( dir == path ) {	
		return strdup ( path );
	}
	

	/* / at end */

	if( dir == path + strlen(path) -1) {
		s = xstrndup( path , strlen(path) -1);
		dir = xdirname(s);
		free(s);
		return dir;
	}		

	return xstrndup( path, dir - path );
}
