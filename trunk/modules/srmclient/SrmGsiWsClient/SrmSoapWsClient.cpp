#include "config.h"
#include "gsi.h"

#include "srmWSH.h"
#include "SrmSoapWsClient.hpp"

//  initialization for "struct Namespace namespaces[] "
#include "soapSRMServerV1.nsmap"

namespace srm {

  //============================================================================
  // Helper function for callback
  static globus_bool_t
  globus_io_secure_authorization_callback_client (
    void *arg,
    globus_io_handle_t * handle,
    globus_result_t res,
    char *identity,
    gss_ctx_id_t * context)
  {
    globus_byte_t *buf;
    struct gsi_plugin_data *data = (struct gsi_plugin_data *) arg;

    //    do nothing for now, or uncomment to get printout::
//     data->server_identity = strdup (identity);
//     globus_libc_printf ("gsi callback client:: connected to: %s\n", identity);

    return GLOBUS_TRUE;
  }

  //
  int SrmSoapWsClient::setupGSI()
  {
    // Register the GSI plugin
    if (soap_register_plugin (_soap, globus_gsi)) {
      soap_print_fault ( _soap, stderr );
      soap_print_fault_location ( _soap, stderr );
      return 1;
    }

    // Setup the GSI channel */
    gsi_set_secure_authentication_mode( _soap,
					GLOBUS_IO_SECURE_AUTHENTICATION_MODE_GSSAPI);
    gsi_set_secure_channel_mode       ( _soap, 
					GLOBUS_IO_SECURE_CHANNEL_MODE_GSI_WRAP);
    gsi_set_secure_protection_mode    ( _soap,
					GLOBUS_IO_SECURE_PROTECTION_MODE_PRIVATE);

    // if delegation needed::
    gsi_set_secure_delegation_mode    ( _soap,
					GLOBUS_IO_SECURE_DELEGATION_MODE_FULL_PROXY);
    gsi_set_secure_authorization_mode ( _soap,
					GLOBUS_IO_SECURE_AUTHORIZATION_MODE_CALLBACK,
					globus_io_secure_authorization_callback_client);
    return 0;
  }


  //=================================================================
  // Default Constructor
  SrmSoapWsClient::SrmSoapWsClient( )
  {
    _soap = soap_new (); 
//-dbg     cerr << "Soap constructed (default constructor), soap=" << _soap << endl;
  }

  // Constructor
  SrmSoapWsClient::SrmSoapWsClient( struct soap *s )
  {
    _soap = s; 
//-dbg     cerr << "Soap constructed " << _soap << endl;
  }


  // Destructor
  SrmSoapWsClient::~SrmSoapWsClient()
  {
    if ( _soap ) {
      soap_destroy( _soap );

      soap_end  ( _soap );
      soap_done ( _soap );

      _soap = NULL;
    }
  }

  //------------------------------------------------------------------------------------------

  int SrmSoapWsClient::ping( bool *rs )
  {
    int ret;
    class tns__pingResponse out;

    ret = soap_call_tns__ping ( _soap, _getSrmURL(), "ping", &out );  // (resp)
    *rs = out._Result;

    if ( ret )
      _processSoapFault();
    return ret;
  }

  //
  int SrmSoapWsClient::getProtocols( ArrayOfstring **rs )
  {
    int ret;
    class tns__getProtocolsResponse out;

    ret = soap_call_tns__getProtocols ( _soap, _getSrmURL(), "getProtocols", &out );  // (resp)
    *rs = out._Result;

    if ( ret )
      _processSoapFault();
    return ret;
  }

  int SrmSoapWsClient::getRequestStatus( int arg0, RequestStatus **rs )
  {
    int ret;
    class tns__getRequestStatusResponse out;

    ret = soap_call_tns__getRequestStatus ( _soap, _getSrmURL(), "getRequestStatus", 
					    arg0, &out );
    *rs = out._Result;

    if ( ret )
      _processSoapFault();

    return ret;
  }
  

  int SrmSoapWsClient::get(    ArrayOfstring  *arg0,   ArrayOfstring   *arg1,  RequestStatus   **rs )
  {
    int ret;
    class tns__getResponse out;

    ret = soap_call_tns__get ( _soap, _getSrmURL(), "get", 
			       arg0, arg1, &out);
    *rs = out._Result;

    if ( ret )
      _processSoapFault();
    return ret;
  }

  int SrmSoapWsClient::put(    ArrayOfstring  *arg0,   ArrayOfstring   *arg1,  ArrayOflong     *arg2, 
			       ArrayOfboolean *arg3,   ArrayOfstring   *arg4,  RequestStatus   **rs )
  {
    int ret;
    class tns__putResponse out;

    ret = soap_call_tns__put ( _soap, _getSrmURL(), "put", 
			       arg0, arg1, arg2, arg3, arg4, &out );
    *rs = out._Result;

    if ( ret )
      _processSoapFault();
    return ret;
  }

  int SrmSoapWsClient::copy(   ArrayOfstring  *arg0,   ArrayOfstring   *arg1,  ArrayOfboolean  *arg2,  
			       RequestStatus  **rs )
  {
    int ret;
    class tns__copyResponse out;

    ret = soap_call_tns__copy ( _soap, _getSrmURL(), "copy", 
				arg0, arg1, arg2, &out );
    *rs = out._Result;

    if ( ret )
      _processSoapFault();
    return ret;
  }
    
  int SrmSoapWsClient::getEstPutTime( ArrayOfstring *arg0,  ArrayOfstring *arg1, ArrayOflong   *arg2, 
				      ArrayOfboolean*arg3,  ArrayOfstring *arg4, RequestStatus **rs )
  {
    int ret;
    class tns__getEstPutTimeResponse out;
  
    ret = soap_call_tns__getEstPutTime ( _soap, _getSrmURL(), "getEstPutTime", 
					 arg0, arg1, arg2, arg3, arg4,  &out );
    *rs = out._Result;

    if ( ret )
      _processSoapFault();
    return ret;
  }

  int SrmSoapWsClient::getEstGetTime( ArrayOfstring *arg0,  ArrayOfstring *arg1, RequestStatus **rs )
  {
    int ret;
    class tns__getEstGetTimeResponse out;

    ret = soap_call_tns__getEstGetTime ( _soap, _getSrmURL(), "getEstGetTime", 
					 arg0, arg1, &out );
    *rs = out._Result;

    if ( ret )
      _processSoapFault();
    return ret;
  }
    
  int SrmSoapWsClient::pin(    ArrayOfstring  *arg0,   RequestStatus **rs )
  {
    int ret;
    class tns__pinResponse out;

    ret = soap_call_tns__pin ( _soap, _getSrmURL(), "pin", 
			       arg0, &out );
    *rs = out._Result;

    if ( ret )
      _processSoapFault();
    return ret;
  }

  int SrmSoapWsClient::unPin(  ArrayOfstring  *arg0, int arg1, RequestStatus **rs )
  {
    int ret;
    class tns__unPinResponse out;

    ret = soap_call_tns__unPin ( _soap, _getSrmURL(), "unPin", 
				 arg0, arg1, &out );
    *rs = out._Result;

    if ( ret )
      _processSoapFault();
    return ret;
  }
    
  int SrmSoapWsClient::getFileMetaData( ArrayOfstring *arg0, ArrayOfFileMetaData **rs )
  {
    int ret;
    class tns__getFileMetaDataResponse out;

    ret = soap_call_tns__getFileMetaData ( _soap, _getSrmURL(), "getFileMetaData", 
					   arg0, &out );  // (resp)
    *rs = out._Result;

    if ( ret )
      _processSoapFault();
    return ret;
  }

  int SrmSoapWsClient::setFileStatus(   int arg0, int arg1,  char *arg2, RequestStatus **rs )
  {
    int ret;
    class tns__setFileStatusResponse out;

    ret = soap_call_tns__setFileStatus ( _soap, _getSrmURL(), "setFileStatus", 
			    arg0, arg1, arg2, &out );
    *rs = out._Result;

    if ( ret )
      _processSoapFault();
    return ret;
  }

  int SrmSoapWsClient::mkPermanent(     ArrayOfstring *arg0, RequestStatus **rs )
  {
    int ret;
    class tns__mkPermanentResponse out;

    ret = soap_call_tns__mkPermanent ( _soap, _getSrmURL(), "mkPermanent", 
				       arg0, &out );
    *rs = out._Result;

    if ( ret )
      _processSoapFault();
    return ret;
  }

  // soap_call_tns__advisoryDelete() has no result (class has no result member).
  //
  int SrmSoapWsClient::advisoryDelete( ArrayOfstring *arg0  )
  {
    int ret;
    class tns__advisoryDeleteResponse out;

    ret = soap_call_tns__advisoryDelete ( _soap, _getSrmURL(), "advisoryDelete", 
					  arg0, &out );  // formal datamember for result
    if ( ret )
      _processSoapFault();
    return ret;
  }

  //============================================================================
  // Private Functions
  //

  void SrmSoapWsClient::_processSoapFault()
  {
    soap_print_fault( _soap, stderr );
  }

  const char *  SrmSoapWsClient::_getSrmURL()
  { 
    return  (const char*) _srmURL.c_str(); 
  };

  //----------------------------------------------------------------------------
  // Helper functions to construct arguments
  //

  VArrayOfstring::VArrayOfstring( vector<string>& vs ) 
    : ArrayOfstring(), __v( vs.size() )
  {
    __ptr    = &__v[0];
    __size   = vs.size(); 
    __offset = 0;
    for( int j=0; j<__size; j++ ) 
      __ptr[j] = (char *)(vs[j].c_str());  
  };

  //--------------------------

  VArrayOfboolean::VArrayOfboolean( vector<bool> &v )
    : ArrayOfboolean(), __v( new bool[v.size()] )
  {
    __ptr    = &__v[0];
    __size   = v.size();
    __offset = 0;

    for( int j=0; j<__size; j++ )
      __ptr[j] = v[j];
  };

 
  //--------------------------


  VArrayOflong::VArrayOflong( vector<LONG64>& v ) 
    : ArrayOflong(), __v( v.size() )
  {
    __ptr    = &__v[0];
    __size   = v.size(); 
    __offset = 0;

    for( int j=0; j<__size; j++ )
      __v[j] = v[j];
  };

  VArrayOflong::VArrayOflong( vector<long>& v ) 
    : ArrayOflong(), __v( v.size() )
  {
    __ptr    = &__v[0];
    __size   = v.size(); 
    __offset = 0;

    for( int j=0; j<__size; j++ )
      __v[j] = v[j];
  };

} // end namespace srm
