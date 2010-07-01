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
 * $Id: system_io.h,v 1.41 2006-09-26 07:41:11 tigran Exp $
 */
#ifndef __system_calls_h__
#define __system_calls_h__

#include <fcntl.h>
#ifndef WIN32
#    include <dirent.h>
#endif
#include <sys/stat.h>
#include <sys/uio.h> /* needed for readv/writev */
#ifdef LIBC_SYSCALLS

#include <stdio.h>

extern int system_open(const char *, int, mode_t);
extern int system_read(int, void *, size_t);
extern int system_readv(int, const struct iovec *vector, int count);
extern int system_write(int, const void *, size_t);
extern int system_writev(int, const struct iovec *vector, int count);
extern int system_close(int);
extern off_t system_lseek64(int, off64_t, int);
extern int system_pread(int, void *, size_t, off_t);
extern int system_pread64(int, void *, size_t, off64_t);
extern int system_pwrite(int, const void *, size_t, off_t);
extern int system_pwrite64(int, const void *, size_t, off64_t);
extern int system_stat(const char *, struct stat *);
extern int system_fstat(int, struct stat *);
extern int system_stat64(const char *, struct stat64 *);
extern int system_lstat(const char *, struct stat *);
extern int system_lstat64(const char *, struct stat64 *);
extern int system_fstat64(int, struct stat64 *);
extern int system_fsync(int);
extern int system_dup(int);
extern DIR *system_opendir( const char *);
extern struct dirent *system_readdir( DIR *);
extern struct dirent64 *system_readdir64( DIR *);
extern int system_closedir( DIR *);
extern off_t system_telldir(DIR *);
extern void system_seekdir(DIR *, off_t);
extern int system_unlink(const char *);
extern int system_rmdir(const char *);
extern int system_mkdir(const char *, mode_t);
extern int system_chmod(const char *, mode_t);
extern int system_chown(const char *, uid_t, gid_t);
extern int system_access(const char *, int);
extern int system_rename(const char *, const char *);
#ifdef HAVE_ACL
extern int system_acl(const char *, int, int, void *);
#endif /* HAVE_ACL */
#ifdef HAVE_FACL
extern int system_facl(int, int, int, void *);
#endif /* HAVE_FACL */
extern FILE * system_fopen( const char *, const char *);
extern FILE * system_fopen64( const char *, const char *);
extern FILE * system_fdopen( int, const char *);
extern size_t system_fread( void *, size_t , size_t, FILE *);
extern size_t system_fwrite( const void *, size_t, size_t , FILE *);
extern int system_fclose(FILE *);
extern int system_feof(FILE *);
extern int system_ferror(FILE *);
extern int system_fflush(FILE *);
extern off64_t system_ftello64(FILE *);
extern int system_fseeko64(FILE *, off64_t, int);
extern char * system_fgets( char *, int, FILE *);
extern int system_fgetc( FILE *);

#else

#ifndef WIN32
#    include <unistd.h>
#    define system_open(a,b,c) open(a,b,c)
#    define system_read(a,b,c) read(a,b,c)
#    ifdef DCAP_NO_PREAD
         #include <stdio.h> /* for SEEK_SET */
         static int system_pread(int a,void *b,int c, long d) {
              lseek(a,d, SEEK_SET) ;
              return read(a, b, c);
         }

         static int system_pwrite(int a,const void *b,int c, long d) {
              lseek(a,d, SEEK_SET) ;
              return write(a, b, c);
         }

#    else
#        define system_pread(a,b,c,d) pread(a,b,c,d)
#        define system_pwrite(a,b,c,d) pwrite(a,b,c,d)
#    endif /* DCAP_NO_PREAD */
#    define system_write(a,b,c) write(a,b,c)
#    define system_close(a) close(a)
#    define system_lseek(a,b,c) lseek(a,b,c)
#    define system_stat(a,b) stat(a,b)
#    define system_lstat(a,b) lstat(a,b)
#    define system_stat64(a,b) stat64(a,b)
#    define system_lstat64(a,b) lstat64(a,b)
#    define system_stat(a,b) stat(a,b)
#    define system_fstat(a,b) fstat(a,b)
#    define system_fstat64(a,b) fstat64(a,b)
#    define system_fsync(a) fsync(a)
#    define system_dup(a) dup(a)
#    define system_opendir(a) opendir(a)
#    define system_readdir(a) readdir(a)
#    define system_readdir64(a) readdir64(a)
#    define system_closedir(a) closedir(a)
#    define system_telldir(a) telldir(a)
#    define system_seekdir(a,b) seekdir(a,b)
#    define system_fopen(a, b) fopen(a, b)
#    define system_fclose(a) fclose(a)
#    define system_fread( a, b, c, d) fread(a, b, c, d)
#    define system_fwrite( a, b, c, d) fwrite(a , b, c, d)
#    define system_fdopen(a, b) fdopen(a, b)
#    define system_fflush(a) fflush(a)
#    define system_feof(a) feof(a)
#    define system_ferror(a) ferror(a)
#    define system_ftell(a) ftell(a)
#    define system_fseek(a, b, c) fseek (a, b, c)
#    define system_fgets(a, b, c) fgets(a, b, c)
#    define system_unlink(a) unlink(a)
#    define system_rmdir(a)  rmdir(a)
#    define system_mkdir(a, b)  mkdir(a, b)
#else

#    define system_open(a,b,c) _open(a,b,c)
#    define system_read(a,b,c) _read(a,b,c)
#    define system_write(a,b,c) _write(a,b,c)
#    define system_close(a) _close(a)
#    define system_lseek(a,b,c) _lseek(a,b,c)
#    define system_lseek64(a,b,c) _lseeki64(a,b,c)
#    define system_stat(a,b) _stat(a,b)
#    define system_stat64(a,b) _stati64(a,b)
#    define system_lstat(a,b) _stat(a,b) /* under windows there is no lstat*/
#    define system_lstat64(a,b) _stati64(a,b) /* under windows there is no lstat*/
#    define system_fstat(a,b) _fstat(a,b)
#    define system_fstat64(a,b) _fstati64(a,b)
#    define system_fsync(a) _fsync(a)
#    define system_dup(a) _dup(a)
#    define system_access(a,b) _access(a,b)
#    include <io.h>
#    include <stdio.h> /* for SEEK_SET */
    static int system_pread(int a,void *b,int c, long d) {  _lseek(a,d, SEEK_SET) ; return _read(a, b, c); }
    static int system_pwrite(int a,const void *b,int c, long d) {  _lseek(a,d, SEEK_SET) ; return _write(a, b, c); }
    static int system_pread64(int a,void *b,int c, __int64 d) {  _lseeki64(a,d, SEEK_SET) ; return _read(a, b, c); }
    static int system_pwrite64(int a,const void *b,int c, __int64 d) {  _lseeki64(a,d, SEEK_SET) ; return _write(a, b, c); }

#    define system_fopen(a, b) fopen(a, b)
#    define system_fclose(a) fclose(a)
#    define system_fread( a, b, c, d) fread(a, b, c, d)
#    define system_fwrite( a, b, c, d) fwrite(a , b, c, d)
#    define system_fdopen(a, b) fdopen(a, b)
#    define system_fflush(a) fflush(a)
#    define system_feof(a) feof(a)
#    define system_ferror(a) ferror(a)
#    define system_ftell(a) ftell(a)
#    define system_fseek(a, b, c) fseek (a, b, c)
#    define system_fgets(a, b, c) fgets(a, b, c)
#    define system_fgetc(a) fgetc(a)

	static __int64 system_fseeko64( FILE *f, __int64 offset, int orign) {
		return (__int64)fseek(f, (__int32)offset, orign);
	}

	static __int64 system_ftello64( FILE *f) {
		return (__int64)ftell(f);
	}

#endif /* WIN32 */

#endif /* LIBC_SYSCALLS */

#endif
