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
 * $Id: dcap_lseek.c,v 1.19 2004-11-04 14:34:31 tigran Exp $
 */

#include "dcap_shared.h"

off_t dc_real_lseek(struct vsp_node *node, off_t offset, int whence);
#ifdef HAVE_LSEEK64
off64_t dc_real_lseek64(struct vsp_node *node, off64_t offset, int whence);
#endif
extern int dc_real_fsync( struct vsp_node *node);
#ifdef HAVE_LSEEK64
off64_t dc_lseek64(int fd,off64_t offset, int whence);
#endif
off_t dc_lseek(int fd, off_t offset, int whence);

off_t dc_lseek(int fd, off_t offset, int whence) {

	off_t o = dc_lseek64( fd, (off_t)offset, whence);
	return (off_t) o;

}



#ifdef HAVE_LSEEK64
off64_t
dc_lseek64(int fd, off64_t offset, int whence)
{
	off64_t n;
	struct vsp_node *node;

#ifdef DC_CALL_TRACE
	showTraceBack();
#endif

	/* nothing wrong ... yet */
	dc_errno = DEOK;

	node = get_vsp_node(fd);
	if (node == NULL) {
		/* we have not such file descriptor, so lets give a try to system */
		return system_lseek64(fd, offset, whence);
	}

	n = dc_real_lseek(node, offset, whence);
	m_unlock(&node->mux);

	return n;

}
#endif

#ifdef HAVE_LSEEK64
off64_t
dc_real_lseek(struct vsp_node *node, off64_t offset, int whence)
{

	int64_t         off;
	int32_t         lseekmsg[5];
	int32_t         size;
	int             tmp;
	int             use_ahead = 0;
	ConfirmationBlock result;


	if( (node->ahead != NULL) && ( node->ahead->buffer != NULL) && node->ahead->used) {
		use_ahead = 1;
	}


	if( whence == SEEK_SET ) {
		/* if already at requested position, do nothing  */
		result.lseek = dc_real_lseek( node, 0, SEEK_CUR );
		if ( result.lseek == offset ) {
			dc_debug(DC_IO, "[%d] SEEK_SET to the current position, skipping", node->dataFd);
			return offset;
		}

	}


	if( (whence == SEEK_CUR) && (offset == 0) ) {
		/* position request, do nothing.... */
		if( use_ahead && node->ahead->used) {


			switch( node->whence ) {
				/* node->ahead->base already contains seek information */
				case SEEK_SET:
					off = node->seek + node->ahead->cur;
					break;
				case SEEK_CUR:
					off = node->ahead->base + node->ahead->cur;
					break;
				default:
					off = node->ahead->base + node->ahead->cur;
			}


		}else{
			switch( node->whence ) {

				case SEEK_SET:
					off = node->seek;
					break;
				case SEEK_CUR:
					off = node->pos + node->seek;
					break;
				default:
					off = node->pos;
			}
		}

		return off;
	}



	if( use_ahead ) {
		dc_real_fsync(node);
	}

    /* funish unsafe write operation
          if node->unsafeWrite flag > 1 : transaction in progress */

	if(node->unsafeWrite > 1) {

		size = htonl(-1); /* send end of data */
		writen(node->dataFd, (char *) &size, sizeof(size), NULL);
		/* FIXME: error detection missing */

		if (get_fin(node) < 0) {
			dc_debug(DC_ERROR, "dc_lseek: mover did not FIN the data block.");
			dc_errno = DEUOF;
			return -1;
		}


		node->unsafeWrite = 1;
	}



	/* to be fast in seek+read, do not realy seeks now. Do it only read
	  calls as SEEK_AND_READ command */

	switch (whence) {
	case SEEK_SET:
		/* TODO: what happens, if user will requests
		   offset out of file size ?*/

		if( use_ahead && ( (off64_t)(node->ahead->base + node->ahead->used) > offset) &&
					(offset >= (off64_t)node->ahead->base) ) {
			node->ahead->cur = offset - node->ahead->base;
			dc_debug(DC_IO, "[%d] SEEK_SET inside Read-ahead buffer. Expected position %ld",
					node->dataFd, node->ahead->base + node->ahead->cur);
			node->whence = -1;
			return offset;
		}

		if( use_ahead ) {
			node->ahead->used = 0;
			node->ahead->cur = 0;
			node->ahead->base = 0;
		}

		node->seek = offset;
		node->whence = SEEK_SET;
		dc_debug(DC_IO, "[%d] Expected seek offset: %ld.", node->dataFd, node->seek);
		return offset;


	case SEEK_CUR:
		if(node->whence == -1) {
			if( use_ahead && (node->ahead->cur + offset >= 0) &&
						(node->ahead->cur + offset  <= node->ahead->used ) ) {
				node->ahead->cur += offset;
				dc_debug(DC_IO, "[%d] SEEK_CUR inside Read-ahead buffer. Expected position %ld",
						node->dataFd, node->ahead->base + node->ahead->cur);

				result.lseek = node->ahead->base + node->ahead->cur;
				return (off64_t)result.lseek;
			}
		}

		if(use_ahead) {
			node->seek  = offset +  node->ahead->cur - node->ahead->used;
			node->ahead->used = 0;
		}else{
			node->seek += offset;
		}
		if(node->whence != SEEK_SET ) {
			node->whence = SEEK_CUR;
			result.lseek = node->pos + node->seek;
		}else{ /* SEEK_SET */
			result.lseek = node->seek;
		}

		dc_debug(DC_IO, "[%d] SEEK_CUR offset: %ld expected position %ld.", node->dataFd, node->seek, node->pos + node->seek);

		return (off64_t)result.lseek;

	case SEEK_END:

		/* we have to replay sone thing on SEEK_END,
		   so lets make real seek*/
		lseekmsg[0] = htonl(16);
		lseekmsg[1] = htonl(IOCMD_SEEK);
		lseekmsg[4] = htonl(IOCMD_SEEK_END);
		break;
	default:
		dc_debug(DC_ERROR, "dc_lseek: illegal value of whence parameter [%d].", whence);
		dc_debug(DC_ERROR, "          Valid values are %d ( SEEK_SET), %d (SEEK_CUR), %d (SEEK_END)", SEEK_SET, SEEK_CUR, SEEK_END);
		errno = EINVAL;
		return -1;
	}

	off = htonll(offset);
	memcpy( (char *) &lseekmsg[2],(char *) &off, sizeof(off));

	tmp = sendDataMessage(node, (char *) lseekmsg, sizeof(lseekmsg), ASCII_NULL, &result);

	if (tmp != COMM_SENT) {
		dc_debug(DC_ERROR, "sendDataMessage failed.");
		return -1;
	}

	node->pos = (off64_t)result.lseek;
	if( node->ahead != NULL ) {
		node->ahead->cur = 0;
		node->ahead->used = 0;
		node->ahead->isDirty = 0;
		node->ahead->base = 0;
	}
	return (off64_t)result.lseek;
}
#endif

