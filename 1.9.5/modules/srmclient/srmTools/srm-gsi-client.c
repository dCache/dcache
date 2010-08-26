// srm-gsi-client.c::
// derived from submit-gsi-client.c as example.
// Alex Kulyavtsev, FNAL
//
// submit-gsi-client.c::
// This software copyright 2002-2003
// by Massimo Cafaro & Daniele Lezzi
// HPCC
// University of Lecce, Italy
//
// and
//
// Robert Van Engelen
// Florida State University

#include "config.h"
#include "gsi.h"
#include "srmWSH.h"
#include "stdio.h"
#include <sys/time.h>

#include "srmImpl.h"

//---------------
using namespace srm;

globus_bool_t globus_io_secure_authorization_callback_client (void *arg,
    globus_io_handle_t	*handle,
    globus_result_t	 res,
    char 		*identity,
    gss_ctx_id_t 	*context
);

  static char *USAGE = "USAGE:\n"
  "srmget [-g] [-p] [-d <delay>]\n"
  " -g -- get files\n"
  " -p -- put files\n"
  " -d <delay> -- set delay for check status\n";


int
main (int argc, char **argv)
{
  //-----------------
  // gSOAP / gsi plugin data:
  struct soap *soap;
  static struct gsi_plugin_data *data;

  //------------------
  // srmcp client data:

  int      i;
  srm_int  requestId;
  int      all_done = 0;

  srm_array_of_strings protocols = new_srm_array_of_strings(1);
  RequestStatus rs;

  struct _srm_host_info _srm_host = {"stkendca3a.fnal.gov", "24128", "srm/managerv1" };
  srm_host_info srm_host =  & _srm_host; 

  int callGet = 0;
  int callPut = 0;
  int delay   = 5;

  int c;

// port 25129 - cdfendca3
// port 24128 - stkendca
// port 24129 - stkendca, ssl
// port 25129 - stkendca, gsiftp

  //---------------
  // Parse options

  while ((c = getopt (argc, argv, "gpd:?")) != EOF) {
    switch (c)
      {
      case 'g':
	callGet++;
	break;
      case 'p':
	callPut++;
	break;
      case 'd':
	delay = (unsigned short int) atoi (optarg);
	break;

	//      free (somestring);
	//	somestring = strdup (optarg);

      case '?':
      default:
	fprintf (stderr, "%s", USAGE);
	return (1);
      }
  }

  //---------------------------------

  // Init the gsoap runtime environment
  soap = soap_new ();

  // Register the GSI plugin
  if (soap_register_plugin (soap, globus_gsi)) {
    soap_print_fault (soap, stderr);
    exit (1);
  }


  // Setup the GSI channel */
  gsi_set_secure_authentication_mode( soap,
		     GLOBUS_IO_SECURE_AUTHENTICATION_MODE_GSSAPI);
  gsi_set_secure_channel_mode       ( soap, 
		     GLOBUS_IO_SECURE_CHANNEL_MODE_GSI_WRAP);
  gsi_set_secure_protection_mode    ( soap,
		     GLOBUS_IO_SECURE_PROTECTION_MODE_PRIVATE);
  // if delegation needed::
  gsi_set_secure_delegation_mode    ( soap,
		     GLOBUS_IO_SECURE_DELEGATION_MODE_FULL_PROXY);
  gsi_set_secure_authorization_mode ( soap,
		     GLOBUS_IO_SECURE_AUTHORIZATION_MODE_CALLBACK,
		     globus_io_secure_authorization_callback_client);

  //==============================================
  if( ! callGet && ! callPut )
    printf("Not get(), not put() - nothing to do ...\n" );


if( callGet ){
  char * file0 = "srm://stkendca3a.fnal.gov:25129/"
    "/pnfs/fnal.gov/usr/test/NeST/real/srmtest/aik/ts01.file1";
  char * file1 = "srm://stkendca3a.fnal.gov:25129/"
    "/pnfs/fnal.gov/usr/test/NeST/real/srmtest/aik/ts01.file6";

  srm_array_of_strings surls     = new_srm_array_of_strings(2);

  set_element_srm_array_of_string( surls,     0,      	(srm_string)(file0));
  set_element_srm_array_of_string( surls,     1,	(srm_string)(file1));
  set_element_srm_array_of_string( protocols, 0,	(srm_string)(&"gsiftp"));

  printf("## call get()\n");

  rs = get( surls, protocols, srm_host );
  if(rs == NULL) {
    printf(" get(): received NULL Request Status\n");
    return 1;
  }

  printf("requestId = %d\n",rs->requestId );
  for(i=0 ; i < rs->fileStatuses->length; i++ ) {
    printf("  FILE # %d ID IS    = %d\n",   i, rs->fileStatuses->array[i]->fileId);
    printf("  FILE # %d ID size  = %lld\n", i, rs->fileStatuses->array[i]->size);
    printf("  FILE # %d ID SURL  = %s\n",   i, rs->fileStatuses->array[i]->SURL);
  }
  requestId =  rs->requestId;


  all_done = 0;
  while( ! all_done) {
    free_RequestStatus(rs);
    all_done = 1;
    printf(" sleep %d sec ...\n", delay);
    fflush(stdout);
    sleep( delay );

    rs = getRequestStatus( requestId, srm_host );
    if(rs == NULL) {
      printf(" get(): received NULL Request Status\n");
      return 1;
    }

    printf("requestId = %d\n",  rs->requestId );
    printf("submitTime = %s\n", rs->submitTime );
    for(i=0 ; i < rs->fileStatuses->length; i++ ) {
      printf("  FILE # %d ID IS    = %d\n",   i, rs->fileStatuses->array[i]->fileId);
      printf("  FILE # %d ID size  = %lld\n", i, rs->fileStatuses->array[i]->size);
      printf("  FILE # %d ID SURL  = %s\n",   i, rs->fileStatuses->array[i]->SURL);
      printf("  FILE # %d ID TURL  = %s\n",   i, rs->fileStatuses->array[i]->TURL);
      printf("  FILE # %d ID state = %s\n",   i, rs->fileStatuses->array[i]->state);
      if( rs->fileStatuses->array[i]->TURL == NULL) {
        printf("   not done yet\n");
        all_done = 0;
      }else
	printf("   -- done\n");
    }
  }

  free_RequestStatus( rs );
  printf("## get() done\n\n");

} // end '-g' -- call get()

  //---------------------

  if( callPut ) {
    printf("## call put()\n");

    {  
    srm_long 	size[2] 	= {615879L,615879L};
    srm_boolean dontwant[2] 	= {1,1};

    srm_array_of_strings sources   = new_srm_array_of_strings(2);
    srm_array_of_strings dests     = new_srm_array_of_strings(2);
    srm_array_of_longs sizes       = new_srm_array_of_longs  (2,size);
    srm_array_of_booleans wantPerm = new_srm_array_of_booleans(2,dontwant);


    // sources:: "file:///thishost/fullpath/file.ext"
    set_element_srm_array_of_string( sources, 0,  
				     (srm_string)("file:///home/aik/srm/cppClientDevel/run/file1.dat"));
    set_element_srm_array_of_string( sources, 1,  
				     (srm_string)("file:///home/aik/srm/cppClientDevel/run/file2.dat"));

    // destinations - SURL:: "srm:///targethost/fullpath/file.ext"
    set_element_srm_array_of_string( dests,   0,  
				     (srm_string)("/pnfs/fnal.gov/usr/test/NeST/real/srmtest/aik/ts02.file1.dat"));
    set_element_srm_array_of_string( dests,   1,  
				     (srm_string)("/pnfs/fnal.gov/usr/test/NeST/real/srmtest/aik/ts02.file2.dat"));

    rs = put( sources, dests, sizes, wantPerm, protocols, srm_host );

    do{

      if(rs == NULL) {
	printf(" put(): received NULL Request Status\n");
	return 1;
      }

      requestId =  rs->requestId;
      printf("requestId = %d\n", rs->requestId);

      all_done = 1;
      for ( i=0; i < rs->fileStatuses->length; i++ ) {
	printf("  FILE # %d ID IS    = %d\n",   i, rs->fileStatuses->array[i]->fileId);
	printf("  FILE # %d ID size  = %lld\n", i, rs->fileStatuses->array[i]->size);
	printf("  FILE # %d ID SURL  = %s\n",   i, rs->fileStatuses->array[i]->SURL);
	printf("  FILE # %d ID TURL  = %s\n",   i, rs->fileStatuses->array[i]->TURL);
	printf("  FILE # %d ID state = %s\n",   i, rs->fileStatuses->array[i]->state);
	if( rs->fileStatuses->array[i]->TURL == NULL) {
	  printf("   not done yet\n"); 
	  all_done = 0; 
	}else
	  printf("   - done\n"); 
      }

      free_RequestStatus(rs);

      if( ! all_done ){
	printf(" sleep %d sec ...\n", delay);
	fflush(stdout);
	sleep(delay);

	rs = getRequestStatus( requestId, srm_host );
      }
    }while( ! all_done );

  }

  free_RequestStatus(rs);

  } // end '-p' -- put() option

  //========================================================
  // Deallocate the gsoap runtime environment as usual

  soap_destroy(soap);

  soap_end  (soap);
  soap_done (soap);

  return 0;
}


globus_bool_t
globus_io_secure_authorization_callback_client (void *arg,
    globus_io_handle_t * handle,
    globus_result_t res,
    char *identity,
    gss_ctx_id_t * context)
{
  globus_byte_t *buf;
  struct gsi_plugin_data *data = (struct gsi_plugin_data *) arg;

  data->server_identity = strdup (identity);
  globus_libc_printf ("Connected to: %s\n", identity);
  return GLOBUS_TRUE;
}
