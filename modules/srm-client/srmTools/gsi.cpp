/*	gsi.c
 
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

#include "gsi.h"

#ifdef __cplusplus
extern "C" {
#endif

static void gsi_delete (struct soap *soap, struct soap_plugin *p);
static int gsi_connect (struct soap *soap, const char *endpoint,
                        const char *hostname, int port);
static int gsi_disconnect (struct soap *soap);
static int gsi_send (struct soap *soap, const char *s, size_t n);
static size_t gsi_recv (struct soap *soap, char *s, size_t n);
static int gsi_error (const char *msg, globus_result_t res);


/* globus_gsi() plugin registration */


int
globus_gsi (struct soap *soap, struct soap_plugin *p, void *arg)
{

  globus_result_t res;
  struct gsi_plugin_data *my_data;

  globus_assert (globus_module_activate (GLOBUS_IO_MODULE) == GLOBUS_SUCCESS);
  p->id = GSI_PLUGIN_ID;	/* MUST be set to register */
  p->data = malloc (sizeof (struct gsi_plugin_data));	/* MUST be set to register */
  my_data = (struct gsi_plugin_data *) p->data;
  if (!my_data)
    {
      free (my_data);
      my_data = NULL;		/* registry failed */
      return SOAP_EOM;
    }

  my_data->client_identity = NULL;
  my_data->server_identity = NULL;
  my_data->server_mode = GLOBUS_FALSE;

  res = globus_io_tcpattr_init (&my_data->io_attr);
  if (res != GLOBUS_SUCCESS)
    return gsi_error ("Failed to initialize tcp attribute", res);

  soap->fopen = gsi_connect;
  soap->fclose = gsi_disconnect;
  soap->fsend = gsi_send;
  soap->frecv = gsi_recv;

  p->fcopy = gsi_copy;
  p->fdelete = gsi_delete;

  return SOAP_OK;
}



//Called by soap_copy
/* gsi_copy() */
int
gsi_copy (struct soap *soap, struct soap_plugin *q, struct soap_plugin *p)
{
  struct gsi_plugin_data *gsi_data;
  struct gsi_plugin_data *gsi_src_data = (struct gsi_plugin_data *) p->data;

  gsi_data =
    (struct gsi_plugin_data *) malloc (sizeof (struct gsi_plugin_data));


  if (!gsi_data)
    return SOAP_EOM;

  q->next = p->next;
  q->id = p->id;


  gsi_data->io_attr = gsi_src_data->io_attr;
  gsi_data->listen_handle = gsi_src_data->listen_handle;
  gsi_data->conn_handle = gsi_src_data->conn_handle;

  gsi_data->server_mode = gsi_src_data->server_mode;
  gsi_data->authorization_data = gsi_src_data->authorization_data;

  if (gsi_src_data->proxy_filename)
    gsi_data->proxy_filename = strdup (gsi_src_data->proxy_filename);
  else
    gsi_data->proxy_filename = NULL;

  if (gsi_src_data->client_identity)
    gsi_data->client_identity = strdup (gsi_src_data->client_identity);
  else
    gsi_data->client_identity = NULL;
  if (gsi_src_data->server_identity)
    gsi_data->server_identity = strdup (gsi_src_data->server_identity);
  else
    gsi_data->server_identity = NULL;

  q->data = gsi_data;

  return SOAP_OK;
}


/* gsi_delete() de-register operation called by soap_done() */
static void
gsi_delete (struct soap *soap, struct soap_plugin *p)
{
  if ((char *) ((struct gsi_plugin_data *) (p->data))->client_identity)
    free ((char *) ((struct gsi_plugin_data *) (p->data))->client_identity);
  if ((char *) ((struct gsi_plugin_data *) (p->data))->server_identity)
    free ((char *) ((struct gsi_plugin_data *) (p->data))->server_identity);
  if ((char *) ((struct gsi_plugin_data *) (p->data))->proxy_filename)
    free ((char *) ((struct gsi_plugin_data *) (p->data))->proxy_filename);

  free (p->data);

}

int
gsi_listener (struct soap *soap, int port, int backlog)
{
  struct gsi_plugin_data *data =
          (struct gsi_plugin_data *) soap_lookup_plugin (soap, GSI_PLUGIN_ID);
  globus_result_t res;
  data->server_mode = GLOBUS_TRUE;
  res =
    globus_io_tcp_create_listener ((unsigned short int *) &port, backlog,
                                   &data->io_attr, &data->listen_handle);
  if (res != GLOBUS_SUCCESS)
    return gsi_error ("GSI listener error", res);
  return SOAP_OK;
}

int
gsi_listen (struct soap *soap)
{
  struct gsi_plugin_data *data =
          (struct gsi_plugin_data *) soap_lookup_plugin (soap, GSI_PLUGIN_ID);
  globus_result_t res;
  if (!data)
    return -1;
  //globus_libc_printf ("Listening\n");
  res = globus_io_tcp_listen (&data->listen_handle);
  if (res != GLOBUS_SUCCESS)
{
      globus_io_tcpattr_destroy (&data->io_attr);
      gsi_error ("GSI bind erorr", res);
      soap_receiver_fault (soap, "GSI bind error", NULL);
      return -1;
    }
  soap->master = data->listen_handle.fd;
  return data->listen_handle.fd;
}

int
gsi_accept (struct soap *soap)
{
  struct gsi_plugin_data *data =
          (struct gsi_plugin_data *) soap_lookup_plugin (soap, GSI_PLUGIN_ID);
  int ip[4];
  unsigned short port;
  globus_result_t res;

  if (!data)
    return -1;


  res =
    globus_io_tcp_accept (&data->listen_handle, &data->io_attr,
                          &data->conn_handle);
  if (res != GLOBUS_SUCCESS)
{
      //gsi_error ("GSI accept error", res);
      soap_receiver_fault (soap, "GSI accept error", NULL);
      return -1;
    }

  globus_io_tcp_get_remote_address (&data->conn_handle, ip, &port);
  soap->ip =
    ((unsigned long) ip[0]) << 24 + ((unsigned long) ip[1]) << 16 +
    ((unsigned long) ip[2]) << 8 + ip[3];
  soap->port = port;

  soap->socket = data->conn_handle.fd;
  return data->conn_handle.fd;
}



//GSI_CONNECT
static int
gsi_connect (struct soap *soap, const char *endpoint, const char *hostname,
             int port)
{
  struct gsi_plugin_data *data =
          (struct gsi_plugin_data *) soap_lookup_plugin (soap, GSI_PLUGIN_ID);
  globus_result_t res;

  data->server_mode = GLOBUS_FALSE;


  res =
    globus_io_tcp_connect ((char *) hostname, (unsigned short int) port,
                           &data->io_attr, &data->conn_handle);
  if (res != GLOBUS_SUCCESS)
{
      gsi_error ("Connection failed", res);
      soap_sender_fault (soap, "GSI connect error", NULL);
      return -1;		/* failure */
    }


  return 0;			/* success */
}

static int
gsi_disconnect (struct soap *soap)
{
  globus_result_t res;
  struct gsi_plugin_data *data =
          (struct gsi_plugin_data *) soap_lookup_plugin (soap, GSI_PLUGIN_ID);

  if (data->conn_handle.state == GLOBUS_IO_HANDLE_STATE_CONNECTED)
{
      res = globus_io_close (&data->conn_handle);
      if (res != GLOBUS_SUCCESS)
        return
          gsi_error
          ("GSI disconnect error while closing the connection handle", res);
    }

  if (data->server_identity)
    {
      free ((char *) data->server_identity);
      data->server_identity = NULL;
    }
  if (data->client_identity)
    {
      free ((char *) data->client_identity);
      data->client_identity = NULL;
    }
  return SOAP_OK;
}

static int
gsi_send (struct soap *soap, const char *s, size_t n)
{
  struct gsi_plugin_data *data =
          (struct gsi_plugin_data *) soap_lookup_plugin (soap, GSI_PLUGIN_ID);
  globus_result_t res;
  globus_size_t nwritten;


  while (n > 0)
{
      res =
        globus_io_write (&data->conn_handle, (globus_byte_t *) s,
                         (globus_size_t) n, &nwritten);
      if (res != GLOBUS_SUCCESS)
        {
          gsi_error ("A write error occurred", res);
          return SOAP_EOF;
        }
      if (nwritten <= 0)
        return SOAP_EOF;
      n -= nwritten;
      s += nwritten;
    }
  return SOAP_OK;
}

static size_t
gsi_recv (struct soap *soap, char *s, size_t n)
{
  struct gsi_plugin_data *data =
          (struct gsi_plugin_data *) soap_lookup_plugin (soap, GSI_PLUGIN_ID);
  globus_result_t res;
  globus_size_t nread = 0;


  if (&data->conn_handle)
{
      res =
        globus_io_read (&data->conn_handle, (globus_byte_t *) s,
                        (globus_size_t) n, 1, &nread);
      if (res != GLOBUS_SUCCESS)
        {
          gsi_error ("A read error occurred", res);
          return 0;
        }
    }
  if (nread > 0)
    return (size_t) nread;
  return 0;
}


int
gsi_set_secure_socket_reuse_addr (struct soap *soap, globus_bool_t reuse)
{
  globus_result_t res;
  struct gsi_plugin_data *data;

  data = (struct gsi_plugin_data *) soap_lookup_plugin (soap, GSI_PLUGIN_ID);
  res = globus_io_attr_set_socket_reuseaddr (&data->io_attr, reuse);
  if (res != GLOBUS_SUCCESS)
    return gsi_error ("Failed to set REUSEADDR option", res);

  return 0;
}


int
gsi_set_secure_authorization_mode (struct soap *soap,
                                   globus_io_secure_authorization_mode_t
                                   authorization_mode,
                                   globus_io_secure_authorization_callback_t
                                   globus_io_secure_authorization_callback)
{
  globus_result_t res;
  struct gsi_plugin_data *data;

  data = (struct gsi_plugin_data *) soap_lookup_plugin (soap, GSI_PLUGIN_ID);
  res =
    globus_io_secure_authorization_data_initialize (&data->
        authorization_data);
  if (res != GLOBUS_SUCCESS)
    return gsi_error ("Failed to initialize secure authorization data", res);

  if (globus_io_secure_authorization_callback)
    {
      res =
        globus_io_secure_authorization_data_set_callback (&data->
            authorization_data,
            globus_io_secure_authorization_callback,
            data);
      if (res != GLOBUS_SUCCESS)
        return gsi_error ("Failed to set secure authentication data callback",
                          res);
    }

  res =
    globus_io_attr_set_secure_authorization_mode (&data->io_attr,
        authorization_mode,
        &data->authorization_data);
  if (res != GLOBUS_SUCCESS)
    return gsi_error ("Failed to set secure authorization mode", res);

  return 0;
}


int
gsi_set_secure_authentication_mode (struct soap *soap,
                                    globus_io_secure_authentication_mode_t
                                    authentication_mode)
{
  globus_result_t res;
  struct gsi_plugin_data *data;

  data = (struct gsi_plugin_data *) soap_lookup_plugin (soap, GSI_PLUGIN_ID);

  res =
    globus_io_attr_set_secure_authentication_mode (&data->io_attr,
        authentication_mode,
        GSS_C_NO_CREDENTIAL);
  if (res != GLOBUS_SUCCESS)
    return gsi_error ("Failed to set secure authentication mode", res);


  return 0;
}

int
gsi_set_secure_protection_mode (struct soap *soap,
                                globus_io_secure_protection_mode_t
                                protect_mode)
{
  globus_result_t res;
  struct gsi_plugin_data *data;

  data = (struct gsi_plugin_data *) soap_lookup_plugin (soap, GSI_PLUGIN_ID);

  res =
    globus_io_attr_set_secure_protection_mode (&data->io_attr, protect_mode);
  if (res != GLOBUS_SUCCESS)
    return gsi_error ("Failed to set secure protection mode", res);


  return 0;
}

int
gsi_set_secure_channel_mode (struct soap *soap,
                             globus_io_secure_channel_mode_t channel_mode)
{
  globus_result_t res;
  struct gsi_plugin_data *data;

  data = (struct gsi_plugin_data *) soap_lookup_plugin (soap, GSI_PLUGIN_ID);
  res = globus_io_attr_set_secure_channel_mode (&data->io_attr, channel_mode);
  if (res != GLOBUS_SUCCESS)
    return gsi_error ("Failed to set secure channel mode", res);


  return 0;
}

int
gsi_set_secure_delegation_mode (struct soap *soap,
                                globus_io_secure_delegation_mode_t
                                delegation_mode)
{
  globus_result_t res;
  struct gsi_plugin_data *data;

  data = (struct gsi_plugin_data *) soap_lookup_plugin (soap, GSI_PLUGIN_ID);

  res =
    globus_io_attr_set_secure_delegation_mode (&data->io_attr,
        delegation_mode);
  if (res != GLOBUS_SUCCESS)
    return gsi_error ("Failed to set secure delegation mode", res);


  return 0;
}


static int
gsi_error (const char *msg, globus_result_t res)
{
  globus_object_t *err = globus_error_get (res);
  char *s = globus_object_printable_to_string (err);
  globus_libc_printf ("%s: %s\n", msg, s);
  free (s);
  globus_object_free (err);
  return SOAP_PLUGIN_ERROR;
}


int gsi_connection_caching(struct soap *soap)
{
        struct gsi_plugin_data *data;
        globus_result_t res;

        data = (struct gsi_plugin_data *) soap_lookup_plugin (soap, GSI_PLUGIN_ID);
        soap->keep_alive = 1;

        res = globus_io_attr_set_socket_keepalive (&data->io_attr, GLOBUS_TRUE);

        if (res != GLOBUS_SUCCESS){
                gsi_error ("Failed to set keepalive attribute", res);
                return -1;
        }

        return 0;
}


int gsi_reset_connection_caching(struct soap *soap)
{

        struct gsi_plugin_data *data;
        globus_result_t res;

        soap_clr_omode(soap, SOAP_IO_KEEPALIVE);

        data = (struct gsi_plugin_data *) soap_lookup_plugin (soap, GSI_PLUGIN_ID);
        soap->keep_alive = 0;

        res = globus_io_attr_set_socket_keepalive (&data->io_attr, GLOBUS_FALSE);
        if (res != GLOBUS_SUCCESS){
                gsi_error ("Failed to reset keepalive attribute", res);
                return -1;
        }

        return 0;
}

#ifdef __cplusplus
}
#endif
