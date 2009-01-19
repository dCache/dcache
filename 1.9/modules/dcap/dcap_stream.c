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
 * $Id: dcap_stream.c,v 1.21 2006-08-30 09:54:02 tigran Exp $
 */

#include "dcap_shared.h"
#include <stdio.h>

int        dc_open(const char *, int, ...);
int        dc_close(int);
ssize_t    dc_real_read(struct vsp_node *node, void *buff, size_t buflen);
ssize_t    dc_real_write(struct vsp_node *node, const void *buff, size_t buflen);
off64_t  dc_real_lseek(struct vsp_node *node, off64_t offset, int whence);
off64_t  dc_ftello64(FILE *fp);
int        dc_fseeko64(FILE *, off64_t, int);

/*******************************************************************
 *                                                                 *
 *  dc_fxxx functions                                              *
 *                                                                 *
 *  STREAM IO                                                      *
 *******************************************************************/

#ifndef _IO_ERR_SEEN
#    define _IO_ERR_SEEN 0x20
#endif

#ifndef _IO_EOF_SEEN
#    define _IO_EOF_SEEN 0x10
#endif


#ifdef linux
	#define FILE_NO(x) (x)->_fileno
#else
	#define FILE_NO(x) (x)->_file
#endif	


int dc_fclose(FILE *fp)
{

	int status;
	struct vsp_node *node;
	
	node = get_vsp_node(FILE_NO(fp));
	if( node == NULL ) {
		return system_fclose(fp);
	}

	/* FIXME */
	m_unlock(&node->mux);
	
	status =  dc_close(FILE_NO(fp));
	free((char *)fp);

	return status ;
}

int dc_feof(FILE *fp)
{
	int     rc 	;
	struct vsp_node *node;
	
	node = get_vsp_node(FILE_NO(fp));
	if( node == NULL ) {
		return system_feof(fp);
	}
	
#if defined(linux) || defined(__CYGWIN__)
	if ( ((FILE *)fp)->_flags & _IO_EOF_SEEN ) {
#else
	if ( ((FILE *)fp)->_flag & _IOEOF ) {
#endif
		rc = 1 ;
	}else {
		rc = 0 ;
	}

	m_unlock(&node->mux);
	return rc ; 
	
}

FILE   *dc_fopen64(const char *file, const char *mode);

FILE   *dc_fopen(const char *file, const char *mode)
{
	return dc_fopen64(file, mode);
}

FILE   *dc_fopen64(const char *file, const char *mode) 
{
	int fd, rw, oflags ;
	FILE *fp;

	if( isPnfs( file )  || isUrl(file) ) {

		rw= ( mode[1] == '+' ) ; 
		switch(*mode) {
			case 'a':
				oflags= O_APPEND | O_CREAT | ( rw ? O_RDWR : O_WRONLY ) ;
				break ; 
			case 'r':
				oflags= rw ? O_RDWR : O_RDONLY ;
				break ; 
			case 'w':
				oflags= O_TRUNC | O_CREAT | ( rw ? O_RDWR : O_WRONLY ) ; 
				break ; 
			default:
				return NULL ;
		}


		fp = (FILE *)malloc( sizeof(FILE) );
		if( fp == NULL ) {
			return NULL;
		}

		/* break FILE chain */
	#ifdef linux
		fp->_chain = NULL;
		fp->_IO_write_ptr = NULL;
		fp->_IO_write_base = NULL;
		fp->_lock = NULL;
		fp->_flags = 0;
	#else
		fp->_flag = 0;
	#endif
		fd = dc_open(file,oflags, 0644) ;
		if ( fd < 0 ) {
			free(fp);
			return NULL ;
		}

		FILE_NO(fp) = fd;

	} else {
		dc_debug(DC_TRACE, "Running system native fopen [%s, %s]", file, mode);
		fp = system_fopen64( file, mode );
	}

	return fp;
}

FILE   *dc_fdopen(int fd, const char *mode) 
{

	FILE *fp;
	struct vsp_node *node;

	node = get_vsp_node(fd);
	if( node == NULL ) {
		 return system_fdopen(fd, mode);
	}
 

	fp = (FILE *)malloc( sizeof(FILE) );
	if( fp == NULL ) {
		return NULL;
	}

	/* break FILE chain */
#ifdef linux
	fp->_chain = NULL;
	fp->_IO_write_ptr = NULL;
	fp->_IO_write_base = NULL;
	fp->_flags = 0;
#else
	fp->_flag = 0;
#endif

	FILE_NO(fp) = fd;


	m_unlock(&node->mux);
	return fp;
}

size_t dc_fread(void *ptr, size_t size, size_t items, FILE *fp)  
{
	int	rc ;
	struct vsp_node *node;
	
	node = get_vsp_node(FILE_NO(fp));
	if( node == NULL ) {
		return system_fread( ptr, size, items, fp);
	}

	rc= dc_real_read(node,ptr,size*items) ;
	switch(rc) {
		case -1:
		case 0:
#if defined(linux) || defined(__CYGWIN__)
			((FILE *)fp)->_flags |= _IO_EOF_SEEN;
#else
			((FILE *)fp)->_flag |= _IOEOF;
#endif		
			rc = 0;
			break ; 
		default:
			rc= (rc+size-1)/size ;
			break ; 
	}
	
	m_unlock(&node->mux);
	return rc ; 
}



int dc_fseek(FILE *fp, long offset, int whence)  
{	
	return dc_fseeko64(fp, (off64_t)offset, whence);
}

int dc_fseeko(FILE *fp, off_t offset, int whence)  
{	
	return dc_fseeko64(fp, (off64_t)offset, whence);
}


int dc_fseeko64(FILE *fp, off64_t offset, int whence)  
{

	off64_t rc;
	struct vsp_node *node;
	
	node = get_vsp_node(FILE_NO(fp));
	if( node == NULL ) {
		return system_fseeko64( fp, offset, whence);
	}


	if (fp == NULL) {
		return -1;
	}

 	rc = dc_real_lseek(node,offset,whence);
	m_unlock(&node->mux);
	
	return rc < 0 ? -1 : 0;
}

long dc_ftell(FILE *fp)
{
	return (long)dc_ftello64(fp);
}


off_t dc_ftello(FILE *fp)
{
	return (off_t)dc_ftello64(fp);
}

off64_t dc_ftello64(FILE *fp)
{

	off64_t rc;
	struct vsp_node *node;
	
	node = get_vsp_node(FILE_NO(fp));
	
	if( node == NULL ) {
		return system_ftello64(fp);
	}


	if (fp == NULL) {
		return -1;
	}

 	rc = dc_real_lseek( node, (off64_t)0, SEEK_CUR);
	m_unlock(&node->mux);


	return rc;
}

size_t dc_fwrite(const void *ptr, size_t size, size_t items, FILE *fp) 
{
	int rc ;
	
	struct vsp_node *node;
	
	node = get_vsp_node(FILE_NO(fp));
	if( node == NULL ) {
		return system_fwrite( ptr, size, items, fp);
	}

	rc= dc_real_write(node,ptr,size*items) ;

	switch(rc) {
		case -1:
#if defined(linux) || defined(__CYGWIN__)
			((FILE *)fp)->_flags |= _IO_ERR_SEEN ;
#else
			((FILE *)fp)->_flag |= _IOERR ;
#endif
			rc= 0 ; 
			break ; 
		case 0:
#if defined(linux) || defined(__CYGWIN__)
			((FILE *)fp)->_flags |= _IO_EOF_SEEN ; 
#else
			((FILE *)fp)->_flag |= _IOEOF ; 
#endif
			break ; 
		default:
			rc= (rc+size-1)/size ;
			break ; 
	}
	
	m_unlock(&node->mux);
	return rc ; 
}


int dc_ferror(FILE *fp)
{

	struct vsp_node *node;
	
	node = get_vsp_node(FILE_NO(fp));
	if( node == NULL ) {
		return system_ferror(fp);
	}
	

	m_unlock(&node->mux);
	return dc_errno; 
	
}


int dc_fflush(FILE *fp)
{

	struct vsp_node *node;
	
	if(fp == NULL ) {
		return system_fflush(fp);
	}
	
	node = get_vsp_node(FILE_NO(fp));
	if( node == NULL ) {
		return system_fflush(fp);
	}

	m_unlock(&node->mux);
	return 0; 
	
}  


char * dc_fgets(char *s, int size, FILE *fp)
{
	struct vsp_node *node;
	int i;
	char c;
	int n;
	char *rs;
	
	node = get_vsp_node(FILE_NO(fp));
	if( node == NULL ) {
		return system_fgets(s, size, fp);
	}


	for( i = 0; i < size; ){
		n = dc_real_read(node, &c, 1);
		if( n <= 0 ) break;
		 s[i++] = c;
		 if( c == '\n' ) break;
	}
	
	s[i] = '\0';
	
	
	if( (n < 0)  || ( i == 0 ) ){
		rs = NULL;
	}else{
		rs = s;
	}
	
	m_unlock(&node->mux);
	return rs;
}

int dc_fgetc(FILE *fp)
{
	struct vsp_node *node;

	unsigned char c;
	int n;
	
	node = get_vsp_node(FILE_NO(fp));
	if( node == NULL ) {
		return system_fgetc(fp);
	}


	n = dc_real_read(node, &c, sizeof(unsigned char));
#ifndef	WIN32
	/* in MSDOS & Co. new line is \n+\r */
	if( c == '\r' )  c = '\n';
#endif /* WIN32 */

	m_unlock(&node->mux);
	return n <=0 ? EOF : (int)c;
}
