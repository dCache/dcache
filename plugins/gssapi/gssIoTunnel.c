/*
 * $Id: gssIoTunnel.c,v 1.9.2.1 2006-09-19 19:37:07 podstvkv Exp $
 */

/*
 *       Copyright (c) 2000,2001,2002 DESY Hamburg DMG-Division
 *               All rights reserved.
 *
 *       THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF
 *                 DESY Hamburg DMG-Division
 *
 * Copyright (c) 1997 - 2002 Kungliga Tekniska H�gskolan (Royal Institute of
 * Technology, Stockholm, Sweden). All rights reserved.
 *
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>
#include <string.h>
#include "base64.h"
#include "util.h"
#include <gssapi.h>
#include "tunnelQueue.h"
#if defined(GSIGSS) &&  defined(GLOBUS_BUG)
#include <globus_module.h>
#endif /* GLOBUS_BUG */
#ifdef MIT_KRB5
#include <gssapi_krb5.h>
#include <gssapi_generic.h>
#endif				/* MIT_KRB5 */

static int      gssAuth(int sock,tunnel_ctx_t* ctx, const char *hostname, const char *service);

#define MAXBUF 16384

int
eInit(int fd)
{

	int             ret;
	struct sockaddr_in remote;
	socklen_t       addrlen;
	struct in_addr *addr;
	struct hostent *hostEnt;

#if defined(GSIGSS) &&  defined(GLOBUS_BUG)
	/* work arount globus bug */
	(void) globus_module_activate(GLOBUS_GSI_GSSAPI_MODULE);
#endif /* GLOBUS_BUG */

	/* we need the host name of the machine where we are connected */

	addrlen = sizeof(remote);
	if (getpeername(fd, (struct sockaddr *) & remote, &addrlen) < 0
	    || addrlen != sizeof(remote)) {
#ifdef SHOW_ERROR
		perror("getpeername");
#endif
		return -1;
	}
	addr = (struct in_addr *) & (remote.sin_addr);
	hostEnt = (struct hostent *) gethostbyaddr((const char *) addr, sizeof(struct in_addr), AF_INET);

	if (hostEnt == NULL) {
		/* Can't resolve address. */
#ifdef SHOW_ERROR
		perror("can't resolv address\n");
#endif
		return -1;
	}

	tunnel_ctx_t* tunnel_ctx = createGssContext(fd);
	if( tunnel_ctx == NULL ) {
		return -1;
	}

	ret =  gssAuth(fd, tunnel_ctx, (const char *)hostEnt->h_name, "host");

	if( ret == 1) {
		tunnel_ctx->isAuthentificated = 1;
	} /* else -> talking plain...(base64) */

	return 1;

}

ssize_t
eRead(int fd, void *buf, size_t size)
{
	char            line[MAXBUF];
	char            c;
	int             i;
	int             len;

	static char            *data;
	static int             pos = 0;
	static int             used = 0;

	tunnel_ctx_t* tunnel_ctx = getGssContext(fd);
	if( tunnel_ctx == NULL ) {
		return -1;
	}

	gss_buffer_desc enc_buff, data_buf;
	OM_uint32 maj_stat, min_stat;

	if( pos == used ) {

		if(data == NULL) {
			data = malloc(MAXBUF);
		}


		i = 0;
		do {
			len = read(fd, &c, 1);

			if( len < 0 ) {
				return -1;
			}

			if( len != 0 ) {
				line[i] = c;
				i++;
			}

		} while ( (i < MAXBUF -1) && (c != '\n') && (c != '\r') && (len > 0) );


		line[i] = '\0';

		if (i > 0) {

			if(tunnel_ctx->isAuthentificated) {

				enc_buff.value = malloc(i);
				enc_buff.length = base64_decode(line + 4, enc_buff.value);

				maj_stat = gss_unwrap(&min_stat, tunnel_ctx->context_hdl,
								&enc_buff, &data_buf, NULL, NULL);


				if (GSS_ERROR(maj_stat)) {
					gss_print_errors(maj_stat);
				}


				memcpy(data, data_buf.value, data_buf.length);
				gss_release_buffer(&min_stat, &enc_buff);
			}else{
				data_buf.length =  base64_decode(line + 4, data);
			}


			used = data_buf.length;
			pos = 0;
			if(tunnel_ctx->isAuthentificated){
				gss_release_buffer(&min_stat, &data_buf);
			}

		} else {

			return -1;
		}


	}


	if( size > used - pos) {
		len = used - pos ;
	}else{
		len = size;
	}

	memcpy(buf, data+pos, len);
	pos +=len;
	return len;

}


ssize_t
eWrite(int fd, const void *buf, size_t size)
{
	ssize_t         ret = 0;

	gss_buffer_desc enc_buff, data_buf;
	OM_uint32 maj_stat, min_stat;

	int             len;
	char           *str = NULL;
	static const char prefix[] = "enc ";
	static const char nl = '\n';

	tunnel_ctx_t* tunnel_ctx = getGssContext(fd);
	if( tunnel_ctx == NULL ) {
		return -1;
	}

	if(tunnel_ctx->isAuthentificated) {

		data_buf.value = (void *)buf;
		data_buf.length = size;

		maj_stat = gss_wrap(&min_stat, tunnel_ctx->context_hdl, 1, GSS_C_QOP_DEFAULT,
						&data_buf, NULL, &enc_buff);


		if (GSS_ERROR(maj_stat)) {
			gss_print_errors(maj_stat);
		}

	}else{
		enc_buff.value = (void *)buf;
		enc_buff.length = size;
	}


	len = base64_encode(enc_buff.value, enc_buff.length, &str);

	if(tunnel_ctx->isAuthentificated){
		gss_release_buffer(&min_stat, &enc_buff);
	}

	write(fd, prefix, 4);
	write(fd, str, len);
	write(fd, &nl, 1);
	free(str);
	return size;
}


int
eDestroy(int fd)
{

	OM_uint32       maj_stat, min_stat;
	tunnel_ctx_t* tunnel_ctx = getGssContext(fd);

	maj_stat = gss_delete_sec_context(&min_stat,  &tunnel_ctx->context_hdl, GSS_C_NO_BUFFER);
	destroyGssContext(fd);

#if defined(GSIGSS) &&  defined(GLOBUS_BUG)
	/* work arount globus bug */
	(void) globus_module_deactivate(GLOBUS_GSI_GSSAPI_MODULE);
#endif /* GLOBUS_BUG */


	if( maj_stat != GSS_S_COMPLETE ) {
		gss_print_errors(maj_stat);
		return -1;
	}

	return 0;
}






static int
import_name(const char *kname, const char *host, gss_name_t * target_name)
{
	OM_uint32       maj_stat, min_stat;
	gss_buffer_desc name;

	name.length = asprintf((char **) &name.value, "%s@%s", kname, host);
	maj_stat = gss_import_name(&min_stat,
				   &name,
#ifdef MIT_KRB5
				   gss_nt_service_name,
#else				/* heimdal */
				   GSS_C_NT_HOSTBASED_SERVICE,
#endif				/* MIT_KRB5 */
				   target_name);
	if (GSS_ERROR(maj_stat)) {
		gss_print_errors(maj_stat);
		return -1;
	}
	free(name.value);
	return 0;
}


int
gssAuth(int sock, tunnel_ctx_t* tunnel_ctx, const char *hostname, const char *service)
{
	struct sockaddr_in remote, local;
	socklen_t       addrlen;

	gss_buffer_desc real_input_token, real_output_token;
	gss_buffer_t    input_token = &real_input_token, output_token = &real_output_token;
	OM_uint32       maj_stat, min_stat;
	gss_name_t      server = GSS_C_NO_NAME;
	gss_channel_bindings_t input_chan_bindings;



	if (import_name(service, hostname, &server) < 0) {
		return -1;
	}
	addrlen = sizeof(local);
	if (getsockname(sock, (struct sockaddr *) & local, &addrlen) < 0
	    || addrlen != sizeof(local)) {
#ifdef SHOW_ERROR
		perror("sockname");
#endif
		return -1;
	}
	addrlen = sizeof(remote);
	if (getpeername(sock, (struct sockaddr *) & remote, &addrlen) < 0
	    || addrlen != sizeof(remote)) {
#ifdef SHOW_ERROR
		perror("getpeer");
#endif
		return -1;
	}
	input_token->length = 0;
	input_token->value = NULL;

	output_token->length = 0;
	output_token->value = NULL;

#ifdef GSIGSS
	input_chan_bindings = GSS_C_NO_CHANNEL_BINDINGS;
#else
	input_chan_bindings = malloc(sizeof(struct gss_channel_bindings_struct));

	sockaddr_to_gss_address((struct sockaddr *) & local,
				&input_chan_bindings->initiator_addrtype,
				&input_chan_bindings->initiator_address);
	sockaddr_to_gss_address((struct sockaddr *) & remote,
				&input_chan_bindings->acceptor_addrtype,
				&input_chan_bindings->acceptor_address);

	input_chan_bindings->application_data.length = 0;
	input_chan_bindings->application_data.value = NULL;
#endif
	while (!tunnel_ctx->isAuthentificated) {
		maj_stat =
			gss_init_sec_context(&min_stat,
					     GSS_C_NO_CREDENTIAL,
					     &tunnel_ctx->context_hdl,
					     server,
					     GSS_C_NO_OID,
				     	 GSS_C_MUTUAL_FLAG | GSS_C_SEQUENCE_FLAG
					     | GSS_C_DELEG_FLAG,
					     0,
					     input_chan_bindings,
					     input_token,
					     NULL,
					     output_token,
					     NULL,
					     NULL);

		if (tunnel_ctx->context_hdl == NULL) {
			/* send a waste to the server */
			eWrite(sock, "123", 3);
			return -1;
		}
		if ((maj_stat != GSS_S_CONTINUE_NEEDED) && (maj_stat != GSS_S_COMPLETE)) {
			gss_print_errors(maj_stat);
			/* send a waste to the server */
			eWrite(sock, "123", 3);
			return -1;
		}
		if (output_token->length > 0) {
			eWrite(sock, output_token->value, output_token->length);
			gss_release_buffer(&min_stat, output_token);
		}
		if (maj_stat & GSS_S_CONTINUE_NEEDED) {
			if( input_token->value == NULL ) {
				input_token->value = malloc(MAXBUF);
			}

            if( input_token->value == NULL ) {
                return -1;
            }

			input_token->length = eRead(sock, input_token->value, MAXBUF);
			if( (input_token->length < 0 ) || (input_token->length > MAXBUF) ) {
				/* incorrect length */
				free(input_token->value);
				input_token->value = NULL;
				return -1;
			}
		} else {
			tunnel_ctx->isAuthentificated = 1;
		}

	}

	return 1;
}


int
gss_check(int sock)
{
	struct sockaddr_in remote, local;
	socklen_t       addrlen;
	char           *name;

	gss_buffer_desc input_token, output_token;
	gss_cred_id_t   delegated_cred_handle = GSS_C_NO_CREDENTIAL;
	OM_uint32       maj_stat, min_stat;
	gss_name_t      client_name;
	gss_buffer_desc export_name;
	gss_channel_bindings_t input_chan_bindings;

	tunnel_ctx_t* tunnel_ctx = createGssContext(sock);
	if( tunnel_ctx == NULL ) {
		return -1;
	}

#ifndef MIT_KRB5
# if 0 /*VP This does not work neither with GT4 nor Heimdal */
	delegated_cred_handle = malloc(sizeof(*delegated_cred_handle));
	memset((char *) delegated_cred_handle, 0,
	       sizeof(*delegated_cred_handle));
# endif
#endif	/* ! MIT_KRB5 */


	addrlen = sizeof(local);
	if (getsockname(sock, (struct sockaddr *) & local, &addrlen) < 0
	    || addrlen != sizeof(local)) {
#ifdef SHOW_ERROR
		perror("getsockname");
#endif
		return -1;
	}
	addrlen = sizeof(remote);
	if (getpeername(sock, (struct sockaddr *) & remote, &addrlen) < 0
	    || addrlen != sizeof(remote)) {
#ifdef SHOW_ERROR
		perror("getpeername");
#endif
		return -1;
	}
	input_chan_bindings = malloc(sizeof(struct gss_channel_bindings_struct));

	sockaddr_to_gss_address((struct sockaddr *) & local,
				&input_chan_bindings->initiator_addrtype,
				&input_chan_bindings->initiator_address);
	sockaddr_to_gss_address((struct sockaddr *) & remote,
				&input_chan_bindings->acceptor_addrtype,
				&input_chan_bindings->acceptor_address);

	input_chan_bindings->application_data.length = 0;
	input_chan_bindings->application_data.value = NULL;

	do {

		input_token.value = malloc(MAXBUF);
		input_token.length = eRead(sock, input_token.value, MAXBUF);

		maj_stat = gss_accept_sec_context(&min_stat,
						  &tunnel_ctx->context_hdl,
						  GSS_C_NO_CREDENTIAL,
						  &input_token,
						  input_chan_bindings,
						  &client_name,
						  NULL,
						  &output_token,
						  NULL,
						  NULL,
						  &delegated_cred_handle);

		if (GSS_ERROR(maj_stat)) {
			gss_print_errors(maj_stat);
		}

		gss_release_buffer(&min_stat, &input_token);

		if (output_token.length != 0) {
			eWrite(sock, output_token.value, output_token.length);
			printf("sended token %d\n", output_token.length);
			gss_release_buffer(&min_stat, &output_token);
		}

		if (maj_stat == GSS_S_COMPLETE) {
			printf("GSS OK\n");
			if (GSS_ERROR(maj_stat)) {
				gss_print_errors(maj_stat);
			}
			maj_stat = gss_export_name(&min_stat, client_name, &export_name);

			if (GSS_ERROR(maj_stat)) {
				gss_print_errors(maj_stat);
			}
			name = realloc(export_name.value, export_name.length + 1);
			name[export_name.length] = '\0';
#if 0
			printf("name = %s\n", name); fflush(stdout);
#endif
		}




	} while( maj_stat == GSS_S_CONTINUE_NEEDED ) ;

	return 0;
}
