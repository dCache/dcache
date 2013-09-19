/*	gsi.h
 
	Globus GSI support
 
	Copyright (C) Massimo Cafaro & Daniele Lezzi, University of Lecce, Italy
	and Robert van Engelen, Florida State University, USA, 2002-2003
 
	Usage (client & server):
	err = soap_register_plugin(&soap, globus_gsi);
	Usage (server):
	err = gsi_listener(&soap, port, backlog);
	master = gsi_listen(&soap);
	slave = gsi_accept(&soap);
	
*/

#ifndef __GSI_H
#define __GSI_H

#include "stdsoap2.h"
#include "globus_io.h"
#include "gssapi.h"

#ifdef __cplusplus
extern "C" {
#endif

#define GSI_PLUGIN_ID "GSI-2.0"	/* GSI plugin identification */

struct gsi_plugin_data
  {
    globus_io_attr_t io_attr;	/* globus io attribute */
    globus_io_handle_t listen_handle;	/* globus litening handle */
    globus_io_handle_t conn_handle;	/* globus connection handle */

    globus_bool_t server_mode;	/* used to distinguish client from server: if GLOBUS_TRUE server, else client */
    const char *client_identity;	/* distinguished name of client connecting to server: filled in by globus_io_secure_authorization_callback */
    const char *server_identity;	/* distinguished name of server we are connecting to: filled in by globus_io_secure_authorization_callback */
    globus_io_secure_authorization_data_t authorization_data;
    char *proxy_filename;

  };

int gsi_set_secure_socket_reuse_addr (struct soap *soap, globus_bool_t reuse);
int gsi_set_secure_authorization_mode (struct soap *soap,
                                       globus_io_secure_authorization_mode_t
                                       authorization_mode,
                                       globus_io_secure_authorization_callback_t
                                       globus_io_secure_authorization_callback);
int gsi_set_secure_authentication_mode (struct soap *soap,
                                        globus_io_secure_authentication_mode_t
                                        authentication_mode);
int gsi_set_secure_protection_mode (struct soap *soap,
                                    globus_io_secure_protection_mode_t
                                    protect_mode);
int gsi_set_secure_channel_mode (struct soap *soap,
                                 globus_io_secure_channel_mode_t
                                 channel_mode);
int gsi_set_secure_delegation_mode (struct soap *soap,
                                    globus_io_secure_delegation_mode_t
                                    deleg_mode);

int globus_gsi (struct soap *soap, struct soap_plugin *p, void *arg);
int gsi_listen (struct soap *soap);
int gsi_listener (struct soap *soap, int port, int backlog);
int gsi_accept (struct soap *soap);
int gsi_copy (struct soap *soap, struct soap_plugin *q,
              struct soap_plugin *p);
int gsi_connection_caching(struct soap *soap);
int gsi_reset_connection_caching(struct soap *soap);

#ifdef __cplusplus
}
#endif

#endif  /* __GSI_H */
