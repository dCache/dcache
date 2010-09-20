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
 * $Id: dcap_access.c,v 1.3 2004-11-01 19:33:29 tigran Exp $
 */

#include "dcap.h"

/**
  * faked access system call, based on stat.
  */


#ifndef WIN32

int
dc_access(const char *path, int mode)
{
	int rc;
	struct stat sbuf;
	uid_t uid;
	gid_t gid;
	int isOwner = 0;
	int isGroup = 0;
	int result = 0;

	rc = dc_stat(path, &sbuf);
	if( rc == 0 ) {

		if( mode == F_OK ) {
			return 0;
		}

		uid = geteuid();
		gid = getegid();


		if( uid == sbuf.st_uid ) {
			isOwner = 1;
		}

		if( gid == sbuf.st_gid ) {
			isGroup = 1;
		}


		rc = 0;
		result = 1;

		if( (mode & R_OK) == R_OK ) {
			if( isOwner ) {
				rc |= sbuf.st_mode & S_IRUSR;
			}

			if( isGroup ) {
				rc |= sbuf.st_mode & S_IRGRP;
			}

			rc |= sbuf.st_mode & S_IROTH;

			result &= rc != 0 ? 1 : 0;
		}

		if( (mode & W_OK) == W_OK ) {
			if( isOwner ) {
				rc |= sbuf.st_mode & S_IWUSR;
			}

			if( isGroup ) {
				rc |= sbuf.st_mode & S_IWGRP;
			}

			rc |= sbuf.st_mode & S_IWOTH;

			result &= rc != 0 ? 1 : 0;
		}

		if( (mode & X_OK) == X_OK ) {
			if( isOwner ) {
				rc |= sbuf.st_mode & S_IXUSR;
			}

			if( isGroup ) {
				rc |= sbuf.st_mode & S_IXGRP;
			}

			rc |= sbuf.st_mode & S_IXOTH;

			result &= rc != 0 ? 1 : 0;

		}

	}


	rc  = result == 0 ? -1 : 0;

	return rc;
}

#else
int
dc_access(const char *path, int mode)
{
	int rc;
	struct stat sbuf;

	rc  = dc_stat(path, &sbuf);

	return rc;
}

#endif
