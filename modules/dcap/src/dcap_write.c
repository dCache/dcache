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
 * $Id: dcap_write.c,v 1.21 2006-09-26 07:47:27 tigran Exp $
 */

#include <sys/uio.h> /* needed for readv/writev */
#include "dcap_shared.h"

ssize_t dc_real_write( struct vsp_node *node, const void *buff, size_t buflen);
#ifdef HAVE_OFF64_T
ssize_t dc_pwrite64(int fd, const void *buff, size_t buflen, off64_t offset);
off64_t dc_real_lseek(struct vsp_node *node, off64_t offset, int whence);
#else
off_t dc_real_lseek64(struct vsp_node *node, off_t offset, int whence);
#endif
ssize_t dc_pwrite(int fd, const void *buff, size_t buflen, off_t offset);
ssize_t
dc_write(int fd,const void *buff, size_t buflen)
{
	ssize_t n;
	struct vsp_node *node;

#ifdef DC_CALL_TRACE
	showTraceBack();
#endif

	/* nothing wrong ... yet */
	dc_errno = DEOK;	
	
	node = get_vsp_node(fd);
	if (node == NULL) {
		/* we have not such file descriptor, so lets give a try to system */
		return system_write(fd, buff, buflen);
	}
	
	n = dc_real_write(node, buff, buflen);
	m_unlock(&node->mux);
	
	return n;	
	
}

ssize_t
dc_pwrite(int fd, const void *buff, size_t buflen, off_t offset)
{
        ssize_t n = -1;
        struct vsp_node *node;

#ifdef DC_CALL_TRACE
        showTraceBack();
#endif

        /* nothing wrong ... yet */
        dc_errno = DEOK;

        node = get_vsp_node(fd);
        if (node == NULL) {
                /* we have not such file descriptor, so lets give a try to system */
                return system_pwrite(fd, buff, buflen, offset);
        }

        if( dc_real_lseek(node, offset, SEEK_SET) >=0 ) {
                n = dc_real_write(node, buff, buflen);
        }

        m_unlock(&node->mux);

        return n;
	
}

#ifdef HAVE_OFF64_T
ssize_t
dc_pwrite64(int fd, const void *buff, size_t buflen, off64_t offset)
{
	ssize_t n = -1;
	struct vsp_node *node;

#ifdef DC_CALL_TRACE
	showTraceBack();
#endif
	
	/* nothing wrong ... yet */
	dc_errno = DEOK;	
	
	node = get_vsp_node(fd);
	if (node == NULL) {
		/* we have not such file descriptor, so lets give a try to system */
		return system_pwrite64(fd, buff, buflen, offset);
	}

	if( dc_real_lseek(node, offset, SEEK_SET) >=0 ) {
		n = dc_real_write(node, buff, buflen);
	}
	
	m_unlock(&node->mux);
	
	return n;	
	
}
#endif

ssize_t
dc_real_write( struct vsp_node *node, const void *buff, size_t buflen)
{

	int32_t         writemsg[5];
	int             tmp;
	int32_t         datamsg[2];
	int32_t         size;
	int64_t         offt;
	int             msglen;
	size_t          len;
	size_t          dataLen;
	int             use_io_buf = 0;
	size_t          wr_buffer = 0;


	if( (node->ahead == NULL) && ( getenv("DCACHE_WRBUFFER") != NULL ) ) {
		dc_debug(DC_INFO, "Switching on write buffer.");
		if(getenv("DCACHE_WA_BUFFER") != NULL) {
			wr_buffer = atoi( getenv("DCACHE_WA_BUFFER") );
		}
		dc_setNodeBufferSize(node, wr_buffer == 0 ? IO_BUFFER_SIZE : wr_buffer);
	}

	if( (node->ahead != NULL) && ( node->ahead->buffer != NULL) ) {
		use_io_buf = 1;
	}

	if( use_io_buf ) {
		
		if( ! node->ahead->isDirty ) {
		
			if( node->ahead->used ) {
				switch( node->whence ){
					case SEEK_SET:
						break;
					case SEEK_CUR:
						break;
					default:
						node->whence = SEEK_CUR;
						node->seek = -(node->ahead->used - node->ahead->cur);
						break;
				}
			}

			/* keep current position, including seeks */
#ifdef HAVE_OFF64_T
			node->ahead->base = dc_real_lseek(node, (off64_t)0, SEEK_CUR);
#else
			node->ahead->base = dc_real_lseek(node, (off_t)0, SEEK_CUR);
#endif
			node->ahead->isDirty = 1;
			node->ahead->cur = 0;
			node->ahead->used = 0;
		}
	
	
		len = node->ahead->size - node->ahead->cur;
		if( buflen && ( len > buflen ) ) {
			memcpy( node->ahead->buffer +node->ahead->cur ,  buff, buflen );
			dc_debug( DC_IO, "[%d] Filling %ld bytes into IO buffer. Available %ld",
						node->dataFd, buflen, len - buflen);
			node->ahead->cur += buflen;
			if( node->ahead->cur > node->ahead->used ) {
				node->ahead->used = node->ahead->cur;
			}
			return buflen;
		}
		
		if( !buflen ) {
			dc_debug(DC_IO, "[%d] Flushing %d bytes of IO buffer.", node->dataFd, node->ahead->cur);
		}	
	
	}


	/*
		node->unsafeWrite == 0 : regular write operation
		node->unsafeWrite == 1 : unsafeWrite, IO request not sended yet
		node->unsafeWrite >1   : unsafeWrite, IO request sended
	*/

	/* do following part allways for regular write and once of unsafe write */
	if(!node->unsafeWrite || (node->unsafeWrite == 1 ) ){

		if(node->whence == -1) {
			writemsg[0] = htonl(4);
			writemsg[1] = htonl(IOCMD_WRITE);

			msglen = 2;
			dc_debug(DC_IO,"[%d] Sending IOCMD_WRITE.", node->dataFd);

		}else{
		
		
			/* in case of seeks and write, there is no way to keep track of check summ */
			if( node->sum != NULL ) {
				node->sum->isOk = 0 ;
			}
		
		
			writemsg[0] = htonl(16);
			writemsg[1] = htonl(IOCMD_SEEK_WRITE);

			offt = htonll(node->seek);
			memcpy( (char *) &writemsg[2],(char *) &offt, sizeof(offt));

			if(node->whence == SEEK_SET) {
				writemsg[4] = htonl(IOCMD_SEEK_SET);
			}else{
				writemsg[4] = htonl(IOCMD_SEEK_CURRENT);		
			}
			
			dc_debug(DC_IO,"[%d] Sending IOCMD_SEEK_WRITE.", node->dataFd);
			msglen = 5;		
		}

		tmp = sendDataMessage(node, (char *) writemsg, msglen*sizeof(int32_t), ASCII_NULL, NULL);

		if (tmp != COMM_SENT) {
			m_unlock(&node->mux);
			dc_debug(DC_ERROR, "sendDataMessage failed.");		
			return -1;
		}

		datamsg[0] = htonl(4);
		datamsg[1] = htonl(IOCMD_DATA);

		tmp = writen(node->dataFd, (char *) datamsg, sizeof(datamsg), NULL);

		/* do this part only once if unsafeWrite requaried */
		if(node->unsafeWrite) {
			node->unsafeWrite = 2;
		}
	}


	dataLen = buflen;
	if( use_io_buf )
		dataLen  += node->ahead->cur;

	size = htonl(dataLen);

	writen(node->dataFd, (char *) &size, sizeof(size), NULL); /* send data size */
	if( use_io_buf ) {
		writen(node->dataFd, (const char *)node->ahead->buffer, node->ahead->cur, NULL); /* send data */
	}

	writen(node->dataFd, (const char *)buff, buflen, NULL); /* send data */
	
	/* update the ckecksum */
	if( (node->sum != NULL ) && (node->sum->isOk == 1) ) {
		if( use_io_buf ) {
			update_checkSum(node->sum, (unsigned char *)node->ahead->buffer, node->ahead->cur);
		}
		
		/*
		 *  we do not need to calculate checksum if buff == NULL ( flush operation )
		 *  if we do so, then check sum well be reseted to default value;
		 */
		if( buff != NULL ) {
			update_checkSum(node->sum, (unsigned char *)buff, buflen);
		}
	}		
	
	if(!node->unsafeWrite) {
	
		size = htonl(-1); /* send end of data */
		writen(node->dataFd, (char *) &size, sizeof(size), NULL);
		/* FIXME: error detection missing */

		if (get_fin(node) < 0) {
			dc_debug(DC_ERROR, "dc_write: mover did not FIN the data block.");
			return -1;
		}
	}

	if( node->whence == SEEK_SET ) {
		node->pos = dataLen + node->seek;	
	} else { /* SEEK_CUR */
		node->pos += (node->seek + dataLen);
	}
		
	node->seek = 0;
	node->whence = -1;

	if( use_io_buf ) {
		node->ahead->cur = 0;
		node->ahead->used = 0;
		node->ahead->base = 0;
		node->ahead->isDirty = 0;
	}

#ifndef WIN32
	dc_debug(DC_IO, "[%d] Expected position: %lld @ %ld bytes written.", node->dataFd, (long long int)node->pos, dataLen);
#else
	dc_debug(DC_IO, "[%d] Expected position: %lld @ %ld bytes written.", node->dataFd, (long int)node->pos, dataLen);
#endif
	return buflen;
}

ssize_t dc_writev(int fd, const struct iovec *vector, int count) {
	
	ssize_t n;
	struct vsp_node *node;
	char *iobuf;
	int i;
	ssize_t iobuf_len;
	off_t iobuf_pos;


#ifdef DC_CALL_TRACE
	showTraceBack();
#endif

	/* nothing wrong ... yet */
	dc_errno = DEOK;	
	
#ifdef IOV_MAX
	if( (count == 0) || (count > IOV_MAX) ) {
#else
	if(count == 0) {
#endif
		errno = EINVAL;
		return -1;
	}
	
	
	node = get_vsp_node(fd);
	if (node == NULL) {
		/* we have not such file descriptor, so lets give a try to system */
		return system_writev(fd, vector, count);
	}
	
	
	iobuf_len = 0;
	for(i = 0; i < count; i++) {
		iobuf_len += vector[i].iov_len;
	}
	
	/* check for overflow */
	if( iobuf_len < 0 ) {
		errno = EINVAL;
		return -1;
	}
	
	iobuf = (char *)malloc(iobuf_len);
	if(iobuf == NULL) {
		m_unlock(&node->mux);
		return -1;
	}
	
	for(i = 0; i < count; i++) {
		memcpy(iobuf + iobuf_pos, vector[i].iov_base, vector[i].iov_len);
		iobuf_pos += vector[i].iov_len;
	}		
	
	n = dc_real_write(node, iobuf, iobuf_len);
	
	/* we do not need the lock any more */
	m_unlock(&node->mux);		
	free(iobuf);	
	return n;
}
