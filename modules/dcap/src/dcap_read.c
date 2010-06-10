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
 * $Id: dcap_read.c,v 1.20 2007-07-09 19:33:18 tigran Exp $
 */


#include <zlib.h>
#include "dcap_shared.h"

ssize_t dc_real_read( struct vsp_node *node, void *buff, size_t buflen);
#ifdef HAVE_OFF64_T
ssize_t dc_pread64( int fd, void *buff, size_t buflen, off64_t);
extern off64_t dc_real_lseek(struct vsp_node *node, off64_t offset, int whence);
#endif
extern int dc_real_fsync(struct vsp_node *);

ssize_t
dc_read(int fd, void *buff, size_t buflen)
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
		return system_read(fd, buff, buflen);
	}

	n = dc_real_read(node, buff, buflen);
	m_unlock(&node->mux);

	return n;

}


ssize_t
dc_pread(int fd, void *buff, size_t buflen, off_t offset)
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
                return system_pread(fd, buff, buflen, offset);
        }

        if( dc_real_lseek(node, offset, SEEK_SET) >=0 ) {
                n = dc_real_read(node, buff, buflen);
        }

        m_unlock(&node->mux);

        return n;
}

#ifdef HAVE_OFF64_T
ssize_t
dc_pread64(int fd, void *buff, size_t buflen, off64_t offset)
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
		return system_pread64(fd, buff, buflen, offset);
	}

	if( dc_real_lseek(node, offset, SEEK_SET) >=0 ) {
		n = dc_real_read(node, buff, buflen);
	}

	m_unlock(&node->mux);

	return n;

}
#endif


ssize_t
dc_real_read( struct vsp_node *node, void *buff, size_t buflen)
{

	int             tmp;
	int32_t         readmsg[7]; /* keep one buffer for READ and SEEK_AND_READ*/
	int              msglen;
	int64_t         size;
	int32_t         blocksize;
    int32_t         lastBlockSize;
	size_t          totsize;
	char           *input_buffer;
	int            use_ahead = 0;
	ssize_t        nbytes, rest = 0;
	size_t         ra_buffer = 0;

	int             loop = 0; /* workaround for looping bug */
    int             errorState = 0;

	/* reconnect */
	int64_t readSize; /* number of bytes requested to read */


	if( (node->ahead != NULL) && ( node->ahead->buffer == NULL) ) {
		if(getenv("DCACHE_RA_BUFFER") != NULL) {
			ra_buffer = atoi( getenv("DCACHE_RA_BUFFER") );
		}
		dc_setNodeBufferSize(node, ra_buffer == 0 ? IO_BUFFER_SIZE : ra_buffer);
	}

	if( (node->ahead != NULL) && ( node->ahead->buffer != NULL) ) {
		use_ahead = 1;
		dc_real_fsync(node);
	}

	if(node->whence == -1) {

			if(use_ahead) {
				if( (!node->ahead->used) || (node->ahead->cur == node->ahead->used) ) {
					/*
						read-aheas buffer not used or there are no
						unreaded data inside
					*/
					if(buflen >= node->ahead->size) {
						use_ahead = 0;
						size = htonll(buflen);
						dc_debug(DC_IO, "[%d] Buffer .GE. than read-ahead buffer.", node->dataFd);
						node->ahead->used = 0;
						node->ahead->cur = 0;
					}else{
						size = htonll(node->ahead->size);
						dc_debug(DC_IO,"[%d] Initially fetching new buffer.", node->dataFd);
						node->ahead->cur = 0;
					}
				}else{
					rest = node->ahead->used - node->ahead->cur;
					if( buflen <= rest ) {
						dc_debug(DC_IO, "[%d] Using existing buffer to read %ld bytes.", node->dataFd, buflen);
						memcpy(buff, node->ahead->buffer + node->ahead->cur, buflen);
						node->ahead->cur += buflen;
						return buflen;
					} else {
						memcpy(buff, node->ahead->buffer + node->ahead->cur, rest);
						dc_debug(DC_IO, "[%d] Taking the rest %ld first.", node->dataFd, rest);
						node->ahead->cur = 0;
				        if ( buflen - rest >= node->ahead->size ) {
							dc_debug(DC_IO, "[%d] Buffer .GE. than read-ahead buffer + unreaded data.", node->dataFd);
							readSize = buflen - rest;
							size = htonll(readSize);
							use_ahead = 0;
							node->ahead->used = 0;
							node->ahead->cur = 0;
				        } else{
							dc_debug(DC_IO, "[%d] Fetching new buffer then.", node->dataFd);
							readSize = node->ahead->size; /* remember the actual read size for reconnect */
							size = htonll(node->ahead->size);
						}
					}
				}
			}else{
				readSize = buflen; /* remember the actual read size for reconnect */
				size = htonll(buflen);
			}

		readmsg[0] = htonl(12);
		readmsg[1] = htonl(IOCMD_READ);
		dc_debug(DC_IO,"[%d] Sending IOCMD_READ (%ld).", node->dataFd, readSize);
		memcpy( (char *) &readmsg[2], (char *) &size, sizeof(size));
		msglen = 4;

	}else{

		if(use_ahead) {
			if( (!node->ahead->used) || (node->ahead->cur == node->ahead->used) ) {
				if(buflen >= node->ahead->size) {
					use_ahead = 0;
					readSize = buflen; /* remember the actual read size for reconnect */
					size = htonll(buflen);
					dc_debug(DC_IO, "[%d] Buffer .GE. than read-ahead buffer.", node->dataFd);
					node->ahead->used = 0;
					node->ahead->cur = 0;
				}else{
					readSize = node->ahead->size; /* remember the actual read size for reconnect */
					size = htonll(node->ahead->size);
					dc_debug(DC_IO,"[%d] Initially fetching new buffer.", node->dataFd);
					node->ahead->cur = 0;
				}
			}else{
				dc_debug(DC_IO, "[%d] SEEK_AND_READ inside buffer.", node->dataFd);
			}
		}else{
			readSize = buflen; /* remember the actual read size for reconnect */
			size = htonll(buflen);
		}

		memcpy( (char *) &readmsg[5], (char *) &size, sizeof(size));

		readmsg[0] = htonl(24);
		readmsg[1] = htonl(IOCMD_SEEK_READ);
		dc_debug(DC_IO,"[%d] Sending IOCMD_SEEK_READ. (%ld)", node->dataFd, readSize);

		size = htonll(node->seek);
		memcpy( (char *) &readmsg[2],(char *) &size, sizeof(size));

		if(node->whence == SEEK_SET) {
			readmsg[4] = htonl(IOCMD_SEEK_SET);
		}else{
			readmsg[4] = htonl(IOCMD_SEEK_CURRENT);
		}

		msglen = 7;
	}



	/* set timeout */
	dcap_set_alarm(DCAP_IO_TIMEOUT);

	tmp = sendDataMessage(node, (char *) readmsg, msglen*sizeof(int32_t), ASCII_NULL, NULL);


	if (tmp != 0) {
		/* remove timeout */
		dcap_set_alarm(0);
		return -1;
	}

	/* if sendDataMessage successed, update the file pointer */
	if( node->whence == SEEK_SET ) {
		node->pos = node->seek;
	} else { /* SEEK_CUR */
		node->pos += node->seek;
	}


	tmp = get_data(node);
	if (tmp < 0) {
		dc_debug(DC_ERROR, "sendDataMessage failed.");
		/* remove timeout */
		dcap_set_alarm(0);
		return -1;
	}
	/* so here we should start receiving data  */

	if(use_ahead) {
		input_buffer = node->ahead->buffer;
		node->ahead->base = node->pos;
		node->ahead->used = 0;
		node->ahead->cur = 0;
	}else{
		input_buffer = buff + rest;
	}

	totsize = 0;
    lastBlockSize = 0;
	while (1) {

		tmp = readn(node->dataFd, (char *) &blocksize, sizeof(blocksize), NULL);

		if( (tmp < 0 ) && isIOFailed ) {
			dc_debug(DC_ERROR, "Timeout on read [1]. Requested %ld, readed %ld", readSize, totsize);
			if(reconnected(node, DCFT_POS_AND_REED, readSize - totsize) != 0) {
                errorState = 1;
				break;
			}else{
				/* Xa-xa we successed in reconnection! */
				continue;
			}
		}

		blocksize = ntohl(blocksize);
		dc_debug(DC_TRACE, "dc_read: transfer blocksize %d", blocksize);

		if (blocksize == -1) {
			dc_debug(DC_TRACE, "dc_read: data transfer finished, total transferd %d, requested %d",
				totsize, readSize);
            /* here is only place where we can brake without error */
            if ( get_fin(node) == -1 ) {
                /* O-o...... */
               	dc_debug(DC_ERROR, "[%d] read did not FIN", node->dataFd);
                /* try to re-get last block */

                /* roll back */
        		input_buffer -= lastBlockSize;
                totsize -= lastBlockSize;

                node->pos -= lastBlockSize;
                if( use_ahead ) {
                    node->ahead->used -=lastBlockSize;
                }

    			if(reconnected(node, DCFT_POS_AND_REED, readSize - totsize) == 0) {
                    /* Xa-xa we successed in reconnection! */
                    continue;
                }
                /* no way to continue */
                errorState = 1;
            }

            break;
		}


		tmp = readn(node->dataFd, input_buffer, blocksize, NULL);

		if( (tmp < 0 ) && isIOFailed ) {
			dc_debug(DC_ERROR, "Timeout on read [2]. Requested %ld, readed %ld", readSize, totsize);
			if(reconnected(node, DCFT_POS_AND_REED, readSize - totsize) != 0) {
                errorState = 1;
				break;
			}else{
				/* Xa-xa we successed in reconnection! */
				continue;
			}
		}

		if (tmp != blocksize) {
			dc_debug(DC_ERROR, "[%d] dc_read: requested %ld => received %ld. Total to read %ld, done %ld ", node->dataFd, blocksize, tmp, readSize, totsize );
			/* FIXME: in some condition we start to loop. As a quick hack do not loop more than two times */
			loop++;
			if( (loop > 3) || (reconnected(node, DCFT_POS_AND_REED, readSize - totsize) != 0) ) {
                errorState = 1;
				break;
			}else{
				/* Xa-xa we successed in reconnection! */
				continue;
			}
		}


		input_buffer += tmp;
		totsize += tmp;

		/* correct file position */
		node->pos += tmp;
		if( use_ahead ) {
			node->ahead->used +=tmp;
		}

        lastBlockSize = blocksize;
	}


    if( errorState == 1 ) {
        dc_debug(DC_ERROR, "[%d] unrecoverable read error", node->dataFd);
        errno = EIO;
        return -1;
    }

	if(use_ahead) {
	    if(totsize <=  buflen - rest) {
			memcpy((char *)buff + rest, node->ahead->buffer, totsize);
			nbytes = totsize + rest;
			node->ahead->cur = totsize;
		}else{
			memcpy((char *)buff + rest, node->ahead->buffer, buflen - rest);
			node->ahead->cur = buflen - rest;
			nbytes = buflen;
		}
	}else{
		nbytes = totsize + rest;
	}

	node->seek = 0;
	node->whence = -1;

	dc_debug(DC_IO, "[%d] Expected position: %lu @ %lu bytes readed. Returning %lu",
		node->dataFd, node->pos, totsize, nbytes);
	if(use_ahead) {
		dc_debug(DC_IO, "     cur (%ld) used (%ld).", node->ahead->cur, node->ahead->used );
		dc_debug(DC_IO, "     pos (%ld) base (%ld).", node->pos, node->ahead->base );
	}

	/* remove timeout */
	dcap_set_alarm(0);

	return nbytes;
}


ssize_t dc_readv(int fd, const struct iovec *vector, int count) {

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
		return system_readv(fd, vector, count);
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


	n = dc_real_read(node, iobuf, iobuf_len);

	/* we do not need the lock any more */
	m_unlock(&node->mux);

	/* error ? */
	if(n < 0 ) {
		free(iobuf);
		return n;
	}


	iobuf_pos = 0;
	/* copy into each buf as much as it can take */
	for(i = 0; (i < count) && (n<iobuf_pos); i++) {
		size_t cain = vector[i].iov_len > (n - iobuf_pos) ? (n - iobuf_pos) : vector[i].iov_len;
		memcpy(vector[i].iov_base, iobuf + iobuf_pos, cain);
		iobuf_pos += cain;
	}

	free(iobuf);
	return n;
}




ssize_t dc_readTo(int srcfd, int destdf, size_t size)
{

	ssize_t transfer_size=0;
	struct vsp_node *node;
	int32_t         readmsg[4];
	int              msglen;
	int              tmp;
	int32_t         blocksize;
	char           *input_buffer = NULL;
	int input_buffer_len = 0;
	int64_t requestSize;
	unsigned long sum = 1;

	node = get_vsp_node(srcfd);
	if (node == NULL) {
		/* we have not such file descriptor, so lets give a try to system */
		return -1;
	}


	readmsg[0] = htonl(12);
	readmsg[1] = htonl(IOCMD_READ);
	dc_debug(DC_IO,"[%d] Sending IOCMD_READ (%ld).", node->dataFd, size);
	requestSize = htonll(size);
	memcpy( (char *) &readmsg[2], (char *) &requestSize, sizeof(requestSize));
	msglen = 4;


	tmp = sendDataMessage(node, (char *) readmsg, msglen*sizeof(int32_t), ASCII_NULL, NULL);


		if (tmp != 0) {
			goto out;
		}

		tmp = get_data(node);
		if (tmp < 0) {
			goto out;
		}

		while (1) {

			tmp = readn(node->dataFd, (char *) &blocksize, sizeof(blocksize), NULL);

			blocksize = ntohl(blocksize);
			dc_debug(DC_TRACE, "dc_read: transfer blocksize %d", blocksize);

			if (blocksize == -1) {

	            /* here is only place where we can brake without error */
	            if ( get_fin(node) == -1 ) {
	                /* O-o...... */
	               	dc_debug(DC_ERROR, "[%d] read did not FIN", node->dataFd);
	                /* try to re-get last block */
	            }

	            break;
			}

	        if( input_buffer_len < blocksize ) {
	        	input_buffer = realloc(input_buffer, blocksize);
	        }

			tmp = readn(node->dataFd, input_buffer, blocksize, NULL);


			if (tmp != blocksize) {
				dc_debug(DC_ERROR, "[%d] dc_read: requested %ld => received %ld.", node->dataFd, blocksize, tmp );
				/* FIXME: in some condition we start to loop. As a quick hack do not loop more than two times */
				goto out;
			}

			sum = adler32(sum, (unsigned char *)input_buffer, blocksize);

			dc_debug(DC_INFO, "block len = %d, checksum is: 0x%.8x",blocksize, sum );

			transfer_size+=blocksize;
			writen(destdf,input_buffer, blocksize, NULL );
		}
out:

	m_unlock(&node->mux);

	if(input_buffer != NULL) {
		free(input_buffer);
	}

	return transfer_size;
}


int dc_readv2(int fd, iovec2 *vector, int count) {

	int rc;
	struct vsp_node *node;
	int32_t  *readvmsg = NULL;
	size_t totalToRead = 0;
	size_t totalRecieved = 0;
	int msglen;
	int i;
	int j;
	int32_t         blocksize;

	int v = 0; /* indes of current buffer */
	int vPos = 0; /* position in current buffer */
	int bPos = 0; /* position in current transfer block */

	int vectorIndex = 0; /* offset of current vector */
	int vectorCount; /* number of vectors to process */

#ifdef DC_CALL_TRACE
	showTraceBack();
#endif

	/* nothing wrong ... yet */
	dc_errno = DEOK;



	if( count  == 0 ) {
		/* nothing to do */
		return 0;
	}

	node = get_vsp_node(fd);
	if (node == NULL) {
		/* we have not such file descriptor, so lets give a try to system */
		for( i = 0; i < count; i++) {
			rc = system_pread( fd, vector[i].buf, vector[i].len, vector[i].offset);
			if( rc != vector[i].len ) {
				return -1;
			}
		}

		return 0;
	}


	/*
	 * while maximal command block size at pool is 8K we can send only
	 * 1024 requests at once.
	 * if sincle reav2 contains more vectors to read, split it into multiple requests,
	 * other wise ROOT ( main client of readv2 ) will fall back to single reads
	 */

	while(vectorIndex < count) {

		v = vectorIndex; /* indes of current buffer */
		vPos = 0; /* position in current buffer */
		bPos = 0; /* position in current transfer block */
		totalToRead = 0; /* byte to read in current chunk*/

#ifdef IOV_MAX
		vectorCount = ((count - vectorIndex) > IOV_MAX) ? IOV_MAX : (count - vectorIndex);
#else
		vectorCount = count - vectorIndex;
#endif
		dc_debug(DC_IO, "total to read %d, chunk %d, index %d", count, vectorCount, vectorIndex);

		readvmsg = malloc(12 + vectorCount*12); /* header + for each request */
		if(readvmsg == NULL) {
			dc_debug(DC_ERROR, "Failed to allocate memory for readv2");
			dc_errno = DEMALLOC;
			m_unlock(&node->mux);
			return -1;
		}
		msglen = 3 + vectorCount*3;

		readvmsg[0] = htonl(8+vectorCount*12); /* bytes following */
		readvmsg[1] = htonl(IOCMD_READV);
		readvmsg[2] = htonl(vectorCount);
		for( i = vectorIndex, j=3; i < vectorIndex+vectorCount; i++ ) {
			int64_t offset = htonll(vector[i].offset);
			memcpy( (char *) &readvmsg[j], (char *) &offset, sizeof(offset));
			j+=2;
			readvmsg[j] = htonl(vector[i].len);
			j++;
			totalToRead += vector[i].len;
		}


		dc_debug(DC_IO, "dc_readv2: %d blocks, %d bytes in total", count, totalToRead);
		dcap_set_alarm(DCAP_IO_TIMEOUT);

		rc = sendDataMessage(node, (char *) readvmsg, msglen*sizeof(int32_t), ASCII_NULL, NULL);

		if (rc != 0) {
			/* remove timeout */
			dcap_set_alarm(0);
			free(readvmsg);
			m_unlock(&node->mux);
			return -1;
		}


		rc = get_data(node);
		if (rc < 0) {
			dc_debug(DC_IO, "sendDataMessage failed.");
			/* remove timeout */
			dcap_set_alarm(0);
			free(readvmsg);
			m_unlock(&node->mux);
			return -1;
		}



		totalRecieved = 0;

		while( totalRecieved < totalToRead) {

			rc = readn(node->dataFd, (char *) &blocksize, sizeof(blocksize), NULL);
			blocksize = ntohl(blocksize);
			bPos = 0;
			dc_debug(DC_IO, "dc_readv2: transfer blocksize %d", blocksize);

			if(vector[v].len ==  vPos) {
				vPos = 0;
				v++;
			}
			if(vPos == 0 )	{
				dc_debug(DC_IO, "dc_readv2: feeling %d size=%d offset=%lld", v, vector[v].len, vector[v].offset);
			}
			while( blocksize > 0 ) {


				if( blocksize <= (vector[v].len - vPos) ) {
					rc = readn(node->dataFd, (char *)&vector[v].buf[vPos] , blocksize, NULL);
					vPos += rc;
					blocksize -= rc;
					totalRecieved+=rc;
				} else {
					rc = readn(node->dataFd, &vector[v].buf[vPos] , vector[v].len - vPos, NULL);
					vPos += rc;
					blocksize -= rc;
					totalRecieved+=rc;
				}

			}

		}

	    if ( get_fin(node) == -1 ) {
	    	dc_debug(DC_ERROR, "Failed go get FIN block");
	    }

	    vectorIndex += vectorCount;

	}

	dcap_set_alarm(0);
	free(readvmsg);
	m_unlock(&node->mux);

	return 0;
}
