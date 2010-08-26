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
 * $Id: str2errno.c,v 1.2 2005-04-25 07:56:37 tigran Exp $
 */

#include <stdio.h>
#include <errno.h>
#include <string.h>
 
typedef struct {
	const char *errStr;
	int errNo;
} errnoMap;


static errnoMap eMap[] = {	

	{ "EOK",	0 },	/* Ok */
	{ "EPERM",	1 },	/* Operation not permitted */
	{ "ENOENT",	2 },	/* No such file or directory */
	{ "ESRCH",	3 },	/* No such process */
	{ "EINTR",	4 },	/* Interrupted system call */
	{ "EIO",	5 },	/* I/O error */
	{ "ENXIO",	6 },	/* No such device or address */
	{ "E2BIG",	7 },	/* Argument list too long */
	{ "ENOEXEC",8 },	/* Exec format error */
	{ "EBADF",	9 },	/* Bad file number */
	{ "ECHILD",	10 },	/* No child processes */
	{ "EAGAIN",	11 },	/* Try again */
	{ "ENOMEM",	12 },	/* Out of memory */
	{ "EACCES",	13 },	/* Permission denied */
	{ "EFAULT",	14 },	/* Bad address */
	{ "ENOTBLK",15 },	/* Block device required */
	{ "EBUSY",	16 },	/* Device or resource busy */
	{ "EEXIST",	17 },	/* File exists */
	{ "EXDEV",	18 },	/* Cross-device link */
	{ "ENODEV",	19 },	/* No such device */
	{ "ENOTDIR",20 },	/* Not a directory */
	{ "EISDIR",	21 },	/* Is a directory */
	{ "EINVAL",	22 },	/* Invalid argument */
	{ "ENFILE",	23 },	/* File table overflow */
	{ "EMFILE",	24 },	/* Too many open files */
	{ "ENOTTY",	25 },	/* Not a typewriter */
	{ "ETXTBSY",26 },	/* Text file busy */
	{ "EFBIG",	27 },	/* File too large */
	{ "ENOSPC",	28 },	/* No space left on device */
	{ "ESPIPE",	29 },	/* Illegal seek */
	{ "EROFS",	30 },	/* Read-only file system */
	{ "EMLINK",	31 },	/* Too many links */
	{ "EPIPE",	32 },	/* Broken pipe */
	{ "EDOM",	33 },	/* Math argument out of domain of func */
	{ "ERANGE",	34 },	/* Math result not representable */

	{ NULL, -1 } 
};


int str2errno( const char *errStr)
{
	
	
	int i;
	int err = EIO; /* default error: I/O error */
	
	for( i = 0; eMap[i].errStr != NULL; i++ ) {
		if( strcmp(errStr, eMap[i].errStr) == 0 ) {
			err = eMap[i].errNo;
		}
	}
	
	
	return err;
}






#ifdef _MAIN_

int main( int argc, char *argv[] )
{	
	
	if( argc != 2 ) {
		errno = EAGAIN;
	}else{
		errno = str2errno(argv[1]);
	}
	
	perror("str2errno ");
}

#endif /* _MAIN */
