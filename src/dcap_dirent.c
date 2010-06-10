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
 * $Id: dcap_dirent.c,v 1.10 2005-04-11 08:12:23 tigran Exp $
 */


#include "dcap_shared.h"
#include <sys/types.h>
#include <dirent.h>

#define RD_SEPARATOR ':'

ssize_t dc_real_read( struct vsp_node *node, void *buff, size_t buflen);
struct dirent64 *dc_readdir64( DIR *dir);
int char2dirent64(const char *, struct dirent64 *);
int char2dirent(const char *, struct dirent *);


#ifdef linux

struct __dirstream   {
    int fd;         /* File descriptor.  */
                                                                                
    char *data;         /* Directory block.  */
    size_t allocation;      /* Space allocated for the block.  */
    size_t size;        /* Total valid data in the block.  */
    size_t offset;      /* Current offset into the block.  */
                                                                                
    off_t filepos;      /* Position of next entry to read.  */
                                                                                
};

#define DIRENT_FD(x) x->fd
#define DIRENT_DATA(x) x->data

#endif

#ifdef sun

#define DIRENT_FD(x) x->dd_fd
#define DIRENT_DATA(x) x->dd_buf

#endif

#ifdef __DARWIN_UNIX03
#  define DIRENT_FD(x) x->__dd_fd
#  define DIRENT_DATA(x) x->__dd_buf
#endif


DIR * dc_opendir(const char *path)
{
	dcap_url *url;
	struct vsp_node *node;	
	DIR *dir;
	
	url = (dcap_url *)dc_getURL(path);
 	if( url == NULL ) {
		dc_debug(DC_INFO, "Using system native opendir for %s.", path);
		return system_opendir(path);
	}
 
 	node = new_vsp_node( path );
	if( node == NULL ) {
		free(url->file);
		free(url->host);
		if( url->prefix != NULL ) free(url->prefix);
		free(url);
		return NULL;
	}

	node->url = url;
	if (url->type == URL_PNFS) {
		node->pnfsId = (char *)strdup(url->file);
	}else{
		node->pnfsId = (char *)strdup(path);
	}	
	node->asciiCommand = DCAP_CMD_OPENDIR;

	if( cache_open(node) != 0 ) {

		node_unplug( node );
		
		deleteQueue(node->queueID);
		node_destroy(node);
		dc_debug(DC_INFO, "Path %s do not exist.", path);		
		return NULL;
	}	

	dir = (DIR *)malloc( sizeof(DIR) );
	if( dir == NULL ) {
		dc_debug(DC_ERROR, "Memory allocation failed for DIR.");
		errno = ENOMEM;
		return NULL;
	}

	DIRENT_FD(dir) = node->dataFd;

	DIRENT_DATA(dir) = (char *)malloc(sizeof(struct dirent));
	if( DIRENT_DATA(dir) == NULL ) {
		free( dir );
		dir = NULL;
		dc_debug(DC_ERROR, "Memory allocation failed for DIR->data.");
		errno = ENOMEM;
	}

	dc_debug(DC_INFO, "Switching on read ahead.");
	node->ahead = (ioBuffer *)malloc(sizeof(ioBuffer));
	if(node->ahead == NULL) {
		dc_debug(DC_ERROR, "Failed allocate memory for read-ahead, so skipping");
	}else{
		node->ahead->buffer = NULL;
		node->ahead->cur = 0;
		node->ahead->base = 0;
		node->ahead->used = 0;
		node->ahead->size = 0;
		node->ahead->isDirty = 0;
	}

	dc_debug(DC_INFO, "opendir : %s => %d", path, dir != NULL ? DIRENT_FD(dir) : -1);
	m_unlock(&node->mux);
	return dir;	
}

/* MAXPATHLEN + 2x ':' + type (1) + '\0' + pnfsid(24) +len(3 -> MAXPATHLEN == 256 ) */
#define CHAINSIZE 287
struct dirent *dc_readdir( DIR *dir)
{

	static struct dirent ent;
	struct dirent64 *ep;
	
	ep = dc_readdir64(dir);
	
	if( ep == NULL ) {
		return NULL;
	}
			
	memcpy(ent.d_name, ep->d_name, 256);
#if defined(linux) || defined(__DARWIN_UNIX03)
	ent.d_type = ep->d_type;
#endif
	ent.d_reclen = ep->d_reclen;
#ifndef __DARWIN_UNIX03
	ent.d_off = (off_t)ep->d_off;
#endif
	ent.d_ino = (ino_t)ep->d_ino;
	
	return &ent;	
	

}

struct dirent64 *dc_readdir64( DIR *dir)
{
	struct dirent64 *ent = NULL;
	char buf[CHAINSIZE];
	struct vsp_node *node;
	char c;
	int i;
	int n;


#ifdef DC_CALL_TRACE
	showTraceBack();
#endif

	/* nothing wrong ... yet */
	dc_errno = DEOK;	

	node = get_vsp_node(DIRENT_FD(dir));
	if (node == NULL) {
		/* we have not such file descriptor, so lets give a try to system */
		dc_debug(DC_INFO, "Running system native readdir64 for %d", DIRENT_FD(dir));
		return system_readdir64(dir);
	}


	/* get a line */
	i = 0;
	while( 1 ) {
		
		n = dc_real_read(node, &c, 1) ;
		if( n != 1 ) {
			break;
		}
		
		if( (c == '\n') || (c == '\r') ) {
			buf[i] = '\0';
			break;
		}
		
		buf[i++] = c;		
		
	}

	
	if( n == 1 ) {

		dc_debug(DC_TRACE, "Readdir64 : %s, path %s/%s", buf, node->directory, node->file_name);

		if( char2dirent64( buf, (struct dirent64 *)DIRENT_DATA(dir)) ) {
			ent = (struct dirent64 *)DIRENT_DATA(dir);
		}	

	}

	m_unlock(&node->mux);	
	return ent;


}

int dc_closedir(DIR *dir)
{

	struct vsp_node *node;

#ifdef DC_CALL_TRACE
	showTraceBack();
#endif


	/* nothing wrong ... yet */
	dc_errno = DEOK;
	
	if( dir == NULL ) {
		errno = EBADF;
		return -1;
	}

	node = delete_vsp_node(DIRENT_FD(dir));
	if (node == NULL) {
		/* we have not such file descriptor, so lets give a try to system */
		dc_debug(DC_INFO, "Using system native closedir for [%d].", DIRENT_FD(dir));
		return system_closedir(dir);
	}

	
	close_data_socket(node->dataFd);
	deleteQueue(node->queueID);

	node_destroy(node);
	
	
	free(DIRENT_DATA(dir));
	free(dir);
	
	return 0;

}


off_t dc_telldir( DIR *dir)
{
	return system_telldir(dir);
}

void dc_seekdir(DIR *dir, off_t offset)
{
	system_seekdir(dir, offset);
}



/*
 * FIXME: add a line format tests 
 */
/*
	Format:
		pnfsid:type(f,d,u):name len:name
*/

int char2dirent64(const char *line, struct dirent64 *ent)
{
	
	char *s;
	char *ss;
	
	/* valid entry have to be at least 5 character */
	if( (line == NULL) || (strlen(line) < 5) ) {
		return 0;
	}


	if( ent == NULL ) {
		return 0;
	}


	s = strchr(line, RD_SEPARATOR);
	if( s == NULL ) {
		return 0;
	}	

	s++;
#ifdef linux
	switch(s[0]){
		case 'f' :
			ent->d_type = DT_REG;
			break;
		case 'd' :
			ent->d_type = DT_DIR;
			break;
		default:
			ent->d_type = DT_UNKNOWN;
	}
#endif	
	/* move pointer to the <name len>  */
	s +=2;
	
	
	ss = strrchr(line, RD_SEPARATOR);	
	++ss;


	memcpy(ent->d_name, ss, strlen(ss));
	ent->d_name[strlen(ss)] = '\0';
	
	ent->d_reclen = sizeof(ent);
	
	return 1;
}

int char2dirent(const char *line, struct dirent *ent)
{
	
	char *s;
	char *ss;
	
	/* valid entry have to be at least 5 character */
	if( (line == NULL) || (strlen(line) < 5) ) {
		return 0;
	}


	if( ent == NULL ) {
		return 0;
	}


	s = strchr(line, RD_SEPARATOR);
	if( s == NULL ) {
		return 0;
	}	

	s++;
#ifdef linux
	switch(s[0]){
		case 'f' :
			ent->d_type = DT_REG;
			break;
		case 'd' :
			ent->d_type = DT_DIR;
			break;
		default:
			ent->d_type = DT_UNKNOWN;
	}
#endif
	/* move pointer to the <name len>  */
	s +=2;
	
	
	ss = strrchr(line, RD_SEPARATOR);	
	++ss;


	memcpy(ent->d_name, ss, strlen(ss));
	ent->d_name[strlen(ss)] = '\0';
	
	ent->d_reclen = sizeof(ent);
	
	return 1;
}

