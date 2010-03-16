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
 * $Id: dcap_preload.c,v 1.39 2006-09-26 07:47:27 tigran Exp $
 */
#include <dcap.h>
#include <sys/types.h>
#include <fcntl.h>
#include <limits.h>
#include <stdlib.h>
#include <errno.h>
#include <stdarg.h>
#include <dirent.h>
#include <dcap_debug.h>
#include <sys/mman.h>


/* 
 * some applications do not close the file, which they have opned
 */
static int cleanupEnabled;
extern void dc_closeAll(); 


/* Replacing system calls with our if we are PRELOAD library */
 
int open(const char *path, int flags,...)
{
	int rc;
	mode_t mode = 0;
	va_list    args;
	
	if (flags & O_CREAT) {
		va_start(args, flags);
		mode = va_arg(args, mode_t);
		va_end(args);
	}	

	rc =  dc_open(path, flags, mode);
	
	dc_debug(DC_TRACE, "Running preloaded open for %s, fd = %d", path, rc);
	if( (rc >=0 ) && ! cleanupEnabled) {
		dc_debug(DC_INFO, "Enabling cleanup atexit");		
		++cleanupEnabled;
		atexit( dc_closeAll );
	}
	return rc;
}

int creat(const char *path, mode_t mode)
{
	int rc;
	
	rc = dc_creat(path, mode);
	dc_debug(DC_TRACE, "Running preloaded creat for %s, fd = %d", path, rc);
	
	return rc;
}

off_t lseek(int fd, off_t offset, int mode)
{
	dc_debug(DC_TRACE, "Running preloaded lseek for [%d][%ld][%d]", fd, offset, mode);
	return dc_lseek(fd, offset, mode);
}

#ifdef HAVE__XSTAT
int _xstat(int i, const char *p, struct stat *s)
{
	dc_debug(DC_TRACE, "Running preloaded _xstat for %s", p);
	return dc_stat(p, s);
}
#endif /* HAVE__XSTAT */

int __xstat(int i, const char *p, struct stat *s)
{
	dc_debug(DC_TRACE, "Running preloaded __xstat for %s", p);
	return dc_stat(p, s);
}

#ifdef HAVE__LXSTAT
int _lxstat(int i, const char *p, struct stat *s)
{
	dc_debug(DC_TRACE, "Running preloaded _lxstat for %s", p);
	return dc_lstat(p, s);
}
#endif /* HAVE__LXSTAT */

int __lxstat(int i, const char *p, struct stat *s)
{
	dc_debug(DC_TRACE, "Running preloaded __lxstat for %s", p);
	return dc_lstat(p, s);
}

#ifdef HAVE__FXSTAT
int _fxstat(int i, int fd, struct stat *s)
{
	dc_debug(DC_TRACE, "Running preloaded _fxstat for [%d]", fd);
	return dc_fstat(fd, s);
}
#endif /* HAVE__FXSTAT */

int __fxstat(int i, int fd, struct stat *s)
{
	dc_debug(DC_TRACE, "Running preloaded __fxstat for [%d]", fd);
	return dc_fstat(fd, s);
}


int stat(const char *p, struct stat *s)
{
	dc_debug(DC_TRACE, "Running preloaded stat for %s", p);
	return dc_stat(p, s);
}


int lstat(const char *p, struct stat *s)
{
	dc_debug(DC_TRACE, "Running preloaded lstat for %s", p);
	return dc_lstat(p, s);
}

int fstat(int fd, struct stat *s)
{
	dc_debug(DC_TRACE, "Running preloaded fstat for [%d]", fd);
	return dc_fstat(fd, s);
}

#ifdef HAVE_ACL
int acl(const char *path, int cmd, int nentries, void *aclbufp)
{
	dc_debug(DC_TRACE, "Running preloaded acl for path %s, %d.", path, cmd);
	return dc_acl(path, cmd, nentries, aclbufp);
}
#endif /* HAVE_ACL */

#ifdef HAVE_FACL
int facl(int fd, int cmd, int nentries, void *aclbufp)
{
	dc_debug(DC_TRACE, "Running preloaded facl for file ID %s, %d.", fd, cmd);
	return dc_facl(fd, cmd, nentries, aclbufp);
}
#endif /* HAVE_FACL */

struct dirent *readdir( DIR *dir)
{
	dc_debug(DC_TRACE, "Running preloaded readdir");
	return dc_readdir(dir);
}


DIR * opendir(const char *fname)
{
	DIR *dir;
	dc_debug(DC_TRACE, "Running preloaded opendir for %s", fname);
	dir= dc_opendir(fname);
	dc_debug(DC_TRACE, "Running preloaded opendir for %s dir [0x%X]", dir );
	return dir;
}

int closedir(DIR *dir)
{
	dc_debug(DC_TRACE, "Running preloaded closedir for [0x%X]", dir);
	return dc_closedir(dir);
}


long int telldir( DIR *dir)
{
	dc_debug(DC_TRACE, "Running preloaded telldir");
	return dc_telldir(dir);
}

void seekdir(DIR *dir, long int offset)
{
	dc_debug(DC_TRACE, "Running preloaded seekdir");
	dc_seekdir(dir, offset);
}


ssize_t  read(int fd, void *buff, size_t n)
{
	dc_debug(DC_TRACE, "Running preloaded read for [%d]", fd);
	return dc_read(fd, buff, n);
}

ssize_t readv(int fd, const struct iovec *iov, int iovcnt)
{
	dc_debug(DC_TRACE, "Running preloaded readv for [%d]", fd);
	return dc_readv(fd, iov, iovcnt);
}
 
ssize_t  pread(int fd, void  *buff,  size_t n , off_t off)
{
	dc_debug(DC_TRACE, "Running preloaded pread for [%d]", fd);
	return  dc_pread(fd, buff, n, (off_t)off);
}

ssize_t  write(int fd, const void *buff, size_t n)
{
	dc_debug(DC_TRACE, "Running preloaded write for [%d]", fd);
	return dc_write(fd, buff, n);
}

ssize_t writev(int fd, const struct iovec *iov, int iovcnt)
{
	dc_debug(DC_TRACE, "Running preloaded writev for [%d]", fd);
	return dc_writev(fd, iov, iovcnt);
}

ssize_t  pwrite(int fd, const void  *buff,  size_t n, off_t off)
{
	dc_debug(DC_TRACE, "Running preloaded pwrite for [%d]", fd);
	return dc_pwrite(fd, buff, n, (off_t) off);
}

int dup(int fd)
{
	dc_debug(DC_TRACE, "Running preloaded dup for [%d]", fd);
	return dc_dup(fd);
}

int access( const char *path, int mode)
{
	dc_debug(DC_TRACE, "Running preloaded access for %s", path);
	return dc_access(path, mode);
}

int fsync(int fd)
{
	dc_debug(DC_TRACE, "Running preloaded fsync for [%d]", fd);
	return dc_fsync(fd);
}

int close(int fd)
{
	dc_debug(DC_TRACE, "Running preloaded close for [%d]", fd);	
	return dc_close(fd);
}



ssize_t listxattr(const char *path, char *list, size_t size)
{

	dc_debug(DC_TRACE, "Running preloaded listxattr for %s", path);
	errno = ENOTSUP;
	return -1;
}

ssize_t flistxattr(int fd, char *list, size_t size)
{

	dc_debug(DC_TRACE, "Running preloaded listxattr for [%d]", fd);
	errno = ENOTSUP;
	return -1;
}

ssize_t llistxattr(const char *path, char *list, size_t size)
{

	dc_debug(DC_TRACE, "Running preloaded listxattr for %s", path);
	errno = ENOTSUP;
	return -1;
}

ssize_t getxattr (const char *path, const char *name,  void *value, size_t size)
{
	dc_debug(DC_TRACE, "Running preloaded  getxattr for %s", path);
	errno = ENOTSUP;
	return -1;

}

ssize_t lgetxattr (const char *path, const char *name,  void *value, size_t size)
{
	dc_debug(DC_TRACE, "Running preloaded  lgetxattr for %s", path);
	errno = ENOTSUP;
	return -1;

}

ssize_t fgetxattr (int fd,  const char *name,  void *value, size_t size)
{
	dc_debug(DC_TRACE, "Running preloaded  fgetxattr for [%d]", fd);
	errno = ENOTSUP;
	return -1;

}

FILE *fopen(const char *path, const char *mode)
{
	dc_debug(DC_TRACE, "Running preloaded fopen for [%s, %s]", path, mode);
	if(!cleanupEnabled) {
		dc_debug(DC_INFO, "Enabling cleanup atexit");		
		++cleanupEnabled;
		atexit( dc_closeAll );
	}	
	return dc_fopen(path, mode);

}

FILE *fdopen(int fd, const char *mode)
{
	dc_debug(DC_TRACE, "Running preloaded fdopen for [%d, %s]", fd, mode);
    if(!cleanupEnabled) {
        dc_debug(DC_INFO, "Enabling cleanup atexit");
        ++cleanupEnabled;
        atexit( dc_closeAll );
    }
	return dc_fdopen(fd, mode);

}

int  fclose(FILE *stream)
{
	dc_debug(DC_TRACE, "Running preloaded fclose");
	return dc_fclose(stream);

}

size_t  fwrite(const void *buf, size_t i,  size_t n , FILE *stream)
{
	dc_debug(DC_TRACE, "Running preloaded fwrite");
	return dc_fwrite( buf, i, n, stream );
}

size_t  fread(void *buf, size_t i,  size_t n, FILE *stream)
{
	dc_debug(DC_TRACE, "Running preloaded fread");
	return dc_fread( buf, i, n, stream );
}


int  fseek(FILE *stream, long offset, int w)
{
	dc_debug(DC_TRACE, "Running preloaded fseek");
	return dc_fseek( stream, offset, w );
}

long ftell(FILE *stream)
{
	dc_debug(DC_TRACE, "Running preloaded ftell");
	return dc_ftell( stream );
}

int ferror(FILE *stream)
{
	dc_debug(DC_TRACE, "Running preloaded ferror");
	return dc_ferror( stream );
}

int  fflush(FILE *stream)
{
	dc_debug(DC_TRACE, "Running preloaded fflush");
	return dc_fflush( stream );
}


int feof(FILE *stream)
{
	dc_debug(DC_TRACE, "Running preloaded feof");
	return dc_feof( stream );
}

char * fgets( char *s, int size, FILE *stream)
{
	dc_debug(DC_TRACE, "Running preloaded fgets");
	return dc_fgets(s, size, stream);
}

int fgetc(FILE *stream)
{
	dc_debug(DC_TRACE, "Running preloaded fgetc");
	return dc_fgetc(stream);
}


int getc(FILE *stream)
{
	dc_debug(DC_TRACE, "Running preloaded getc");
	return dc_fgetc(stream);
}


int fseeko( FILE *stream, off_t offset, int whence)
{
	dc_debug(DC_TRACE, "Running preloaded fseeko");
	return dc_fseeko( stream, offset, whence );
}

off_t ftello (FILE *stream )
{
	dc_debug(DC_TRACE, "Running preloaded ftello");
	return dc_ftello( stream );
}

int unlink( const char *path)
{
	dc_debug(DC_TRACE, "Running preloaded unlink for path %s", path);
	return dc_unlink(path);
}


int rmdir( const char *path)
{
	dc_debug(DC_TRACE, "Running preloaded rmdir for path %s", path);
	return dc_rmdir(path);
}

int mkdir( const char *path, mode_t mode)
{
	dc_debug(DC_TRACE, "Running preloaded mkdir for path %s, mode 0%o", path, mode);
	return dc_mkdir(path, mode);
}

int chmod( const char *path, mode_t mode)
{
        dc_debug(DC_TRACE, "Running preloaded chmod for path %s, mode 0%o", path, mode);
        return dc_chmod(path, mode);
}

int chown( const char *path, uid_t uid, gid_t gid)
{
        dc_debug(DC_TRACE, "Running preloaded chown for path %s,  %d:%d", path, uid, gid);
        return dc_chown(path, uid, gid);
}
                                                                                
void clearerr(FILE *stream)
{
	dc_errno = DEOK;
}

void rewind( FILE *stream )
{
	dc_debug(DC_TRACE, "Running preloaded rewind");
	(void)dc_fseek( stream, 0L, SEEK_SET );
}

int rename( const char *oldPath, const char *newPath)
{
	dc_debug(DC_TRACE, "Running preloaded renam %s to %s.", oldPath, newPath);
	return dc_rename(oldPath, newPath);
}

