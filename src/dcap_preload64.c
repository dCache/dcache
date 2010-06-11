/*
 *   DCAP - dCache Access Protocol client interface
 *
 *   Copyright (C) 2000,2004 DESY Hamburg DMG-Division.
 *
 *   AUTHOR: Tigran Mkrtchayn (tigran.mkrtchyan@desy.de)
 *   FIXED BY: Vladimir Podstavkov (podstvkv@fnal.gov)
 *
 *   This program can be distributed under the terms of the GNU LGPL.
 *   See the file COPYING.LIB
 *
 */


/*
 * $Id: dcap_preload64.c,v 1.7 2004-11-04 14:16:39 tigran Exp $
 */


#include <dcap.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <limits.h>
#include <stdlib.h>
#include <errno.h>
#include <stdarg.h>
#include <dirent.h>
#include <dcap_debug.h>


/* Replacing system calls with our if we are PRELOAD library */


/*
 *  work around linux glibc header files mess:
 *  all calls replaced with there 64 bit implementations
 *  if _FILE_OFFSET_BITS=64 is defined.
 */

int open64(const char *path, int flags,...)
{
	mode_t mode = 0;
	va_list    args;

	if (flags & O_CREAT) {
		va_start(args, flags);
		mode = va_arg(args, mode_t);
		va_end(args);
	}

	dc_debug(DC_TRACE, "Running preloaded open64 for %s", path);
	return dc_open(path, flags, mode);
}

int creat64(const char *path, mode_t mode)
{
	dc_debug(DC_TRACE, "Running preloaded creat for %s", path);
	return dc_creat(path, mode);
}

ssize_t  pread64(int fd, void  *buff,  size_t n , off64_t off)
{
	dc_debug(DC_TRACE, "Running preloaded pread64 for [%d]", fd);
 	return  dc_pread64(fd, buff, n, off);
}

ssize_t  pwrite64(int fd, const void  *buff,  size_t n, off64_t off)
{
	dc_debug(DC_TRACE, "Running preloaded pwrite64 for [%d]", fd);
 	return dc_pwrite64(fd, buff, n, off);
}

off64_t lseek64(int fd, off64_t offset, int mode)
{
	dc_debug(DC_TRACE, "Running preloaded lseek64 for [%d][%lld][%d]", fd, (off64_t)offset, mode);
	return dc_lseek64(fd, offset, mode);
}

int __xstat64(int i, const char *p, struct stat64 *s)
{
	dc_debug(DC_TRACE, "Running preloaded __xstat64 for %s", p);
	return dc_stat64(p, s);
}

int __lxstat64(int i, const char *p, struct stat64 *s)
{
	dc_debug(DC_TRACE, "Running preloaded __lxstat64 for %s", p);
	return dc_lstat64(p, s);
}

int __fxstat64( int i, int fd, struct stat64 *s)
{
	dc_debug(DC_TRACE, "Running preloaded fstat64 for [%d]", fd);
	return dc_fstat64(fd, s);
}

int stat64(const char *p, struct stat64 *s)
{
	dc_debug(DC_TRACE, "Running preloaded stat64 for %s", p);
	return dc_stat64(p, s);
}

int lstat64(const char *p, struct stat64 *s)
{
	dc_debug(DC_TRACE, "Running preloaded lstat64 for %s", p);
	return dc_lstat64(p, s);
}

int fstat64( int fd, struct stat64 *s)
{
	dc_debug(DC_TRACE, "Running preloaded fstat64 for [%d]", fd);
	return dc_fstat64(fd, s);
}

#ifdef sgi
dirent64_t *readdir64( DIR *dir)
#else
struct dirent64 *readdir64( DIR *dir)
#endif
{
	dc_debug(DC_TRACE, "Running preloaded readdir64");
#ifdef sgi
	return ( dirent64_t *)dc_readdir64(dir);
#else
	return dc_readdir64(dir);
#endif
}


FILE *fopen64(const char *path, const char *mode)
{
	dc_debug(DC_TRACE, "Running preloaded fopen for [%s, %s]", path, mode);
	return dc_fopen(path, mode);

}

int fseeko64( FILE *stream, off64_t offset, int whence)
{
	dc_debug(DC_TRACE, "Running preloaded fseeko64");
	return dc_fseeko64( stream, offset, whence );
}

off64_t ftello64 (FILE *stream )
{
	dc_debug(DC_TRACE, "Running preloaded ftello64");
	return dc_ftello64( stream );
}
