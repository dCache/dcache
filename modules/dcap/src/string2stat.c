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
 * $Id: string2stat.c,v 1.10 2005-08-15 10:05:03 tigran Exp $
 */


#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#ifdef WIN32
#	include "dcap_unix2win.h"
#endif /* WIN32 */

#ifdef __CYGWIN__
#define atoll(a) atol(a)
#endif

/* Local function prototypes */
static mode_t string2mode(const char *str_mode);


mode_t string2mode(const char *str_mode)
{

	mode_t mode = 0; /*  d rwx rwx rwx */


	if( strlen( str_mode ) < 9 ) {
		/* invalid mode */
		return 0;
	}



	switch( str_mode[0] ) {
		case '-' :
			mode |= S_IFREG; /* regular file */
			break;
		case 'd' :
			mode |= S_IFDIR; /* directory */
			break;
#ifndef WIN32
		case 'l' :
			mode |= S_IFLNK; /* symbolic link */
			break;
		case 'x' :
			mode |= S_IFCHR; /* charactrer device , just someting */
			break;
		default:
			mode |= S_IFIFO; /* FIFO, something else */
			break;
#endif /* ! WIN32 */
	}


#ifndef WIN32

	/* owner */

	if( str_mode[1] == 'r' )
		mode |= S_IRUSR; /* u+r */
	if( str_mode[2] == 'w' )
		mode |= S_IWUSR; /* u+w */
	if( str_mode[3] == 'x' )
		mode |= S_IXUSR; /* u+x */


	/* group */

	if( str_mode[4] == 'r' )
		mode |= S_IRGRP; /* g+r */
	if( str_mode[5] == 'w' )
		mode |= S_IWGRP; /* g+w */
	if( str_mode[6] == 'x' )
		mode |= S_IXGRP; /* g+x */

	/* others */

	if( str_mode[7] == 'r' )
		mode |= S_IROTH; /* o+r */
	if( str_mode[8] == 'w' )
		mode |= S_IWOTH; /* o+w */
	if( str_mode[9] == 'x' )
		mode |= S_IXOTH; /* o+x */

#else  /* ! WIN32 */

	if( str_mode[1] == 'r' )
		mode |= _S_IREAD;
	if( str_mode[2] == 'w' )
		mode |= _S_IWRITE;
#endif/* ! WIN32 */

	return mode;
}

#ifdef WIN32
void string2stat64( const char **arg,  struct _stati64 *s )
#else
void string2stat64( const char **arg,  struct stat64 *s )
#endif
{
	 int i;
	 char *c;

#ifdef WIN32
	 memset( s, 0, sizeof(struct _stati64) );
#else
	 memset( s, 0, sizeof(struct stat64) );
#endif

	 for( i = 1; arg[i] != NULL; i++) {


		c = strchr( arg[i], '=' );

		if( c == NULL ) continue;

		c++;

		if( strncmp( arg[i], "-st_dev", c - arg[i] -2 ) == 0 ) {
			s->st_dev = atoi(c);
			continue;
		}

		if( strncmp( arg[i], "-st_ino", c - arg[i] -2 ) == 0 ) {
			s->st_ino = atoi(c);
			continue;
		}

		if( strncmp( arg[i], "-st_mode", c - arg[i] -2 ) == 0 ) {
			s->st_mode = string2mode((const char *)c);
			continue;
		}

		if( strncmp( arg[i], "-st_nlink", c - arg[i] -2 ) == 0 ) {
			s->st_nlink = atoi(c);
			continue;
		}

		if( strncmp( arg[i], "-st_uid", c - arg[i] -2 ) == 0 ) {
			s->st_uid = atoi(c);
			continue;
		}

		if( strncmp( arg[i], "-st_gid", c - arg[i] -2 ) == 0 ) {
			s->st_gid = atoi(c);
			continue;
		}

		if( strncmp( arg[i], "-st_rdev", c - arg[i] -2 ) == 0 ) {
			s->st_rdev = atoi(c);
			continue;
		}

		if( strncmp( arg[i], "-st_size", c - arg[i] -2 ) == 0 ) {
			s->st_size = atoll(c);
			continue;
		}

#ifndef WIN32

		if( strncmp( arg[i], "-st_blksize", c - arg[i] -2 ) == 0 ) {
			s->st_blksize = atoi(c);
			continue;
		}

		if( strncmp( arg[i], "-st_blocks", c - arg[i] -2 ) == 0 ) {
			s->st_blocks = atoi(c);
			continue;
		}
#endif /* ! WIN32 */

		if( strncmp( arg[i], "-st_atime", c - arg[i] -2 ) == 0 ) {
			s->st_atime = atoi(c);
			continue;
		}

		if( strncmp( arg[i], "-st_mtime", c - arg[i] -2 ) == 0 ) {
			s->st_mtime = atoi(c);
			continue;
		}

		if( strncmp( arg[i], "-st_ctime", c - arg[i] -2 ) == 0 ) {
			s->st_ctime = atoi(c);
			continue;
		}

	 }
}

