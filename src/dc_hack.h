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
 * $Id: dc_hack.h,v 1.8 2005-12-19 11:06:28 tigran Exp $
 */


#ifndef DC_HACK_H
#define DC_HACK_H

/*
 *  quick hack to make a application dCache aware
 *  gcc -include dc_hack.h -o someaoo someapp.c -ldcap
 *
 *  This hack is for tests only and do not appropriate for any production code
 */

#warning "Use of dc_hack.h is not appropriate for a production purposes."
#warning "Please modify you code to call DCAP functions directly."


#define open       dc_open
#define create     dc_create
#define close      dc_close
#define read       dc_read
#define write      dc_write
#define fsync      dc_fsync
#define dup        dc_dup
#define access     dc_access
#define unlink     dc_unlink

#define lseek64    dc_lseek64
#define stat64     dc_stat64
#define lstat64    dc_lstat64
#define fstat64    dc_fstat64
#define readdir64  dc_readdir64
#define ftello64   dc_ftello64
#define fseeko64   dc_fseeko64
#define pread64    dc_pread64
#define pwrite64   dc_pwrite64

#define lseek      dc_lseek
#define stat       dc_stat
#define lstat      dc_lstat
#define fstat      dc_fstat
#define readdir    dc_readdir
#define ftello     dc_ftello
#define fseeko     dc_fseeko
#define pread      dc_pread
#define pwrite     dc_pwrite

#define rmdir      dc_rmdir
#define mkdir      dc_mkdir
#define chmod      dc_chmod
#define rename     dc_rename

#define opendir    dc_opendir
#define closedir   dc_closedir
#define telldir    dc_telldir
#define seekdir    dc_seekdir

#define fopen      dc_fopen
#define fdopen     dc_fdopen
#define fclose     dc_fclose
#define fwrite     dc_fwrite
#define fread      dc_fread
#define fseek      dc_fseek
#define ftell      dc_ftell
#define ferror     dc_ferror
#define fflush     dc_fflush
#define feof       dc_feof
#define fgets      dc_fgets
#define fgetc      dc_fgetc


#endif /* DC_HACK_H */
