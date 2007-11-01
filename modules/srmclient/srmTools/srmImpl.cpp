#include "srmWSH.h"
#include "srmImpl.h"

//include "struct Namespace namespaces[] " initialization
#include "soapSRMServerV1.nsmap"

namespace srm {

/***************************************************************************
 * srm private functions definitions
 ***************************************************************************/

static srm_string  srm_strcpy(  char* str );
static void        srm_freestr( srm_string str );

static srm_boolean fillSrmUrl( char* srmurl, srm_host_info host_info );

// Wrap/construct : srm to gSOAP
static class ArrayOfstring  *srm2g_string_array(  srm_array_of_strings  string_array );
static class ArrayOflong    *srm2g_long_array(    srm_array_of_longs    long_array );
static class ArrayOfboolean *srm2g_boolean_array( srm_array_of_booleans boolean_array );
// "destructors"
static void  freeArrayOfstring(  class ArrayOfstring   *array);
static void  freeArrayOflong(    class ArrayOflong     *array);
static void  freeArrayOfboolean( class ArrayOfboolean  *array);

// Unwrap/construct:: gSOAP to SRM
static  RequestStatus           g2srm_RequestStatus( class ns11__RequestStatus *ns11_rs );

static  RequestFileStatus       g2srm_RequestFileStatus( class ns11__RequestFileStatus* ns11_rfs );
static srm_array_of_RequestFileStatuses g2srm_ArrayOfRequestFileStatuses( class ArrayOfRequestFileStatus *fileStatuses );
// "destructors"
static void  free_RequestFileStatus(          RequestFileStatus                rfs );
static void  free_ArrayOfRequestFileStatuses( srm_array_of_RequestFileStatuses arr_rfs );

/***************************************************************************
 * srm functions implementations
 ***************************************************************************/

//
srm_boolean ping( srm_host_info host_info )
//   SOAP_FMAC1 int SOAP_FMAC2 
//     soap_call_tns__ping(struct soap*, 
// 			const char*, const char*, struct tns__pingResponse *);
{
  return SRM_TRUE;
}

//srm_array_of_strings getProtocols( srm_host_info host_info );
//
RequestStatus getRequestStatus( srm_int       requestId,
				srm_host_info host_info )
{
  char srmurl[SRM_MAX_URL_LEN];
  RequestStatus rs = NULL;

  if( fillSrmUrl( srmurl, host_info) ) {
    int ret;
    class tns__getRequestStatusResponse out;
    {
      struct soap soap;
      soap_init(&soap);

      ret = soap_call_tns__getRequestStatus( &soap, 
		 (const char*)srmurl, "getRequestStatus", (int)requestId, &out );
      if ( ret ) 
	soap_print_fault( &soap, stderr );
      else
	rs = g2srm_RequestStatus( out._Result );

      soap_end(  &soap );
      soap_done( &soap );
    }
  }
  return rs;
}

//
RequestStatus get( srm_array_of_strings surls,
		   srm_array_of_strings protocols,
		   srm_host_info        host_info )
{
  char srmurl[SRM_MAX_URL_LEN];
  RequestStatus rs = NULL;

  if( fillSrmUrl( srmurl, host_info) ) {
    int   ret;
    class tns__getResponse out;

    class ArrayOfstring *arg0 = srm2g_string_array( surls );
    class ArrayOfstring *arg1 = srm2g_string_array( protocols );

    {
      struct soap soap;
      
      soap_init( &soap );
      
      ret = soap_call_tns__get ( &soap, (const char*)srmurl, "get", 
				 arg0, arg1, &out);
      if ( ret == SOAP_OK )
	rs = g2srm_RequestStatus( out._Result );
      else
	soap_print_fault( &soap, stderr );
      
      soap_end(  &soap );
      soap_done( &soap );
    }
    freeArrayOfstring( arg0 );
    freeArrayOfstring( arg1 );
  }
  return rs;
}


//
RequestStatus put( srm_array_of_strings  sources,
		   srm_array_of_strings  dests,
		   srm_array_of_longs    sizes,
		   srm_array_of_booleans wantPerm,
		   srm_array_of_strings  protocols,
		   srm_host_info         host_info )
{
  char srmurl[SRM_MAX_URL_LEN];
  RequestStatus rs = NULL;

  if( fillSrmUrl( srmurl, host_info) ) {
    int   ret;
    class tns__putResponse out;

    class ArrayOfstring  *arg0 = srm2g_string_array(  sources );
    class ArrayOfstring  *arg1 = srm2g_string_array(  dests );
    class ArrayOflong    *arg2 = srm2g_long_array(    sizes );
    class ArrayOfboolean *arg3 = srm2g_boolean_array( wantPerm );
    class ArrayOfstring  *arg4 = srm2g_string_array(  protocols );

    {
      struct soap soap;
      
      soap_init( &soap );

      ret = soap_call_tns__put( &soap, (const char*)srmurl, "put",
				arg0, arg1, arg2, arg3, arg4, &out );
      if ( ret == SOAP_OK )
	rs = g2srm_RequestStatus( out._Result );
      else
	soap_print_fault( &soap, stderr );

      soap_end(  &soap );
      soap_done( &soap );
    }
    freeArrayOfstring(  arg0 );
    freeArrayOfstring(  arg1 );
    freeArrayOflong(    arg2 );
    freeArrayOfboolean( arg3 );
    freeArrayOfstring(  arg4 );
  }
  return rs;
}

//
RequestStatus copy( srm_array_of_strings   srcSURLS,
		    srm_array_of_strings   destSURLS,
		    srm_array_of_booleans  wantPerm,
		    srm_host_info          host_info )
{
  char srmurl[SRM_MAX_URL_LEN];
  RequestStatus rs = NULL;

  if( fillSrmUrl( srmurl, host_info) ) {
    int   ret;
    class tns__copyResponse  out;

    class ArrayOfstring   *arg0 = srm2g_string_array(  srcSURLS );
    class ArrayOfstring   *arg1 = srm2g_string_array(  destSURLS );
    class ArrayOfboolean  *arg2 = srm2g_boolean_array( wantPerm );
    
    {
      struct soap soap;

      soap_init(&soap);
 
      ret = soap_call_tns__copy( &soap, (const char*)srmurl, "copy",  
				 arg0, arg1, arg2, &out);
      if ( ret )
	soap_print_fault( &soap, stderr );
      else
	rs = g2srm_RequestStatus( out._Result );

      soap_end(  &soap );
      soap_done( &soap );
    }
    freeArrayOfstring(  arg0 );
    freeArrayOfstring(  arg1 );
    freeArrayOfboolean( arg2 );
  }
  return rs;
}

// RequestStatus getEstGetTime( srm_array_of_strings SURLS ,
// 			     srm_array_of_strings protocols,
// 			     srm_host_info        host_info );

// RequestStatus getEstPutTime( srm_array_of_strings  src_names,
// 			     srm_array_of_strings  dest_names,
// 			     srm_array_of_longs    sizes,
// 			     srm_array_of_booleans wantPermanent,
// 			     srm_array_of_strings  protocols,
// 			     srm_host_info         host_info );

//
RequestStatus setFileStatus ( srm_int       requestId,
			      srm_int       fileId,
			      srm_string    state,
			      srm_host_info host_info )
{
  char *statestr = (char*) state;
  char  srmurl[SRM_MAX_URL_LEN];
  class tns__setFileStatusResponse out;

  RequestStatus rs = NULL;

  if(    strcmp("Failed",  statestr)
      && strcmp("Pending", statestr)
      && strcmp("Ready",   statestr)
      && strcmp("Running", statestr)
      && strcmp("Done",    statestr) 
  )
    return NULL;     /* String NOT found, the state is invalid, just do nothing */

  if( fillSrmUrl( srmurl,  host_info)  ) {
    struct soap soap;
    soap_init(&soap);
    
    if ( soap_call_tns__setFileStatus ( &soap, (const char*)srmurl, "setFileStatus",
					(int)requestId, (int)fileId, state, &out))
      soap_print_fault( &soap, stderr );
    else
      rs = g2srm_RequestStatus( out._Result );

    soap_end(  &soap );
    soap_done( &soap );
  }
  return rs;
}

/***************************************************************************
 * srm convinience function implementations
 ***************************************************************************/

// RequestStatus destructor
// 
void free_RequestStatus( RequestStatus rs )
{
  if ( rs ) {
    srm_freestr( rs->type );
    srm_freestr( rs->state );
    // 'str_time' is implemented as string, see constructor
    srm_freestr( rs->submitTime );
    srm_freestr( rs->startTime );
    srm_freestr( rs->finishTime );

    free_ArrayOfRequestFileStatuses( rs->fileStatuses );
    srm_freestr( rs->errorMessage );
    delete rs;
  }
}

// FileMetaData destructor
// 
void free_FileMetaData( FileMetaData p )
{
  if ( p ) {
    srm_freestr( p->SURL );
    srm_freestr( p->owner );
    srm_freestr( p->group );
    srm_freestr( p->checksumType );
    srm_freestr( p->checksumValue );

    delete p;
  }
}

// srm_array_of_FileMetaDatas  destructor
//
void free_srm_array_of_FileMetaDatas( srm_array_of_FileMetaDatas p )
{
  if( p ){
    if( p->array ) {
      for( int i=0; i < p->length; i++ ) 
	free_FileMetaData( p->array[i] );
      delete [] p->array;
    }
    delete p;
  }
}

//--------------------------------------------------------------------------
// srm_array_of_strings constructor
//
srm_array_of_strings new_srm_array_of_strings(srm_int length)
{
  srm_array_of_strings aos = new struct _array_of_strings;
  aos->length = length;
  if( (aos->array = new srm_string[length]) )
    memset( aos->array, 0, sizeof(srm_string)*length );
  return aos;
}

// srm_array_of_strings destructor
//
void free_srm_array_of_strings( srm_array_of_strings aos)
{
  if( aos ) {
    if ( aos->array )
      delete [] aos->array;
    delete    aos;
  }
}

// set element of srm_array_of_strings
//
srm_boolean set_element_srm_array_of_string(srm_array_of_strings aos,
					    srm_int              pos,
					    srm_string           string)
{
  if ( aos                                      // assert array address is OK
       && pos >= 0 && pos < aos->length ) {     //   and index is within array boundary
    aos->array[pos] = string;                   // - set element of string
    return SRM_TRUE;
  }else
    return SRM_FALSE;
}

// get element of srm_array_of_strings
//
srm_string get_element_srm_array_of_string( srm_array_of_strings aos, srm_int pos )
{
  return ( aos                                  // assert array address is OK
	   && pos >=0 && pos < aos->length  )   //   and index is within array boundary
    ? aos->array[pos]                           // - return element of string address
    : NULL;                                     //          or error
}

//--------------------------------------------------------------------------
// srm_array_of_booleans   constructor
//
srm_array_of_booleans new_srm_array_of_booleans( srm_int length, srm_boolean *values )
{
  srm_array_of_booleans boolean_array = NULL;   // default return code is 'fail' or 'no array'

  // Assert arguments:
  //  if length is positive, 'values' must be set;
  //  if length is zero, I don't care about 'values' setting
  if( length == 0 
      || (length >0 && values) ) {

    boolean_array = new struct _array_of_boolean;
    if( boolean_array ) {                       // mem allocation OK.
      boolean_array->length = length;

      if( length == 0 ) {                       // no mem. for array required
	boolean_array->array = NULL;
      }else{                                    // allocate memory for array
	boolean_array->array = new srm_boolean[length];
	if( boolean_array->array ) {            // allocation OK, copy array
	  memcpy( boolean_array->array, values, length* sizeof(srm_boolean) );
	}else{                                  // mem. allocation for array failed, ...
	  delete boolean_array;                 // delete mem. already allocated
	  boolean_array = NULL;                 // and set mem. pointer to indicate 'failed' Ret.Code
	}
      }
    }
  }
  return boolean_array;
}

// srm_array_of_booleans   destructor
//
void free_srm_array_of_booleans( srm_array_of_booleans boolean_array )
{
  if ( boolean_array ) {
    if ( boolean_array->array )
      delete [] boolean_array->array;
    delete boolean_array;
  }
}

// srm_array_of_longs   constructor
//
srm_array_of_longs new_srm_array_of_longs( srm_int length, srm_long *values )
{
  srm_array_of_longs long_array = NULL;         // default return code is 'fail' or 'no array'

  // Assert arguments:
  //  if length is positive, 'values' must be set;
  //  if length is zero, I don't care about 'values' setting
  if( length == 0 
      || (length >0 && values) ) {

    long_array = new struct _array_of_long;
    if( long_array ) {                          // mem allocation OK.
      long_array->length = length;

      if( length == 0 ) {                       // no mem. for array required
	long_array->array = NULL;
      }else{                                    // allocate memory for array
	long_array->array = new srm_long[length];
	if( long_array->array ) {               // allocation OK, copy array
	  memcpy( long_array->array, values, length* sizeof(srm_long) );
	}else{                                  // mem. allocation for array failed, ...
	  delete long_array;                    // delete mem. already allocated
	  long_array = NULL;                    // and set mem. pointer to indicate 'failed' Ret.Code
	}
      }
    }
  }
  return long_array;
}

// srm_array_of_longs   destructor
//
void free_srm_array_of_longs( srm_array_of_longs long_array )
{
  if ( long_array ) {
    if ( long_array->array )
      delete [] long_array->array;
    delete    long_array;
  }
}

/***************************************************************************
 * srm private functions implementations
 ***************************************************************************/

// srm_string   constructor
//
static srm_string srm_strcpy(char* str)
{
  srm_string srmstr = NULL;

  if( str 
      && (srmstr=(srm_string) malloc(strlen(str)+1)) 
  )
    strcpy( (char*) srmstr, str );

  return srmstr;
}

// srm_string   destructor
// 
static void srm_freestr( srm_string str )
{
  if ( str )
    free( str );
}

// fillSrmUrl( srmurl, host_info)
//   fills "srmurl" by information in "host_info"
//
static srm_boolean fillSrmUrl( char* srmurl, srm_host_info host_info)
{
  srm_boolean ret = SRM_FALSE;

  // It is wrong idea to define "default host" - fragment of code deleted.

  if ( host_info ) {
    int n = snprintf( srmurl, SRM_MAX_URL_LEN, "http://%s:%s/%s",
		  (char*) (host_info->hostname),
		  (char*) (host_info->port),
		  (char*) (host_info->path));

    ret = ( n >= 0 &&  n < SRM_MAX_URL_LEN )
      ? SRM_TRUE
      : SRM_FALSE;
  }
  return ret;
}

//------------------------------------------------------------------------
// Constructors /  srm to gSOAP wrappers 
// and destructors

//
static class ArrayOfstring *srm2g_string_array( srm_array_of_strings string_array )
{
  class ArrayOfstring *array = NULL;

  if ( string_array 
       && (array = new ArrayOfstring() ) ) {
    int len = (int) string_array->length;

    array->__offset = 0;
    array->__size   = len;
    array->__ptr    = new char*[len];
    for( int i=0; i < len; i++ ) 
      array->__ptr[i] =  string_array->array[i];
  }
  return array;
}

//
static void freeArrayOfstring( class ArrayOfstring *array )
{
  if( array ) {
    if( array->__ptr ) 
      delete [] array->__ptr;

    delete array;
  }
}

//
static class ArrayOflong *srm2g_long_array( srm_array_of_longs long_array )
{
  class ArrayOflong *array = NULL;

  if ( long_array 
       && (array = new ArrayOflong() ) ) {
    int len = (int) long_array->length;

    array->__size = len;
    array->__offset = 0;
    array->__ptr = new LONG64[len];
    for(int i=0; i < len; i++ )
      array->__ptr[i] = (LONG64) long_array->array[i];
  }
  return array;
}

//
static void freeArrayOflong( class ArrayOflong *array )
{
  if( array ) {
    if( array->__ptr )
      delete [] array->__ptr;
    delete array;
  }
}

//
static class ArrayOfboolean *srm2g_boolean_array( srm_array_of_booleans boolean_array )
{
  class ArrayOfboolean *array = NULL;

  if ( boolean_array 
  && ( array = new ArrayOfboolean() ) ) {
    int len = (int) boolean_array->length;

    array->__size = len;
    array->__offset = 0;
    array->__ptr = new bool[len];

    for( int i=0; i<len; i++ ) {
      array->__ptr[i] = ( boolean_array->array[i] ) 
	? true
	: false;
    }
  }
  return array;
}

//
static void freeArrayOfboolean( struct ArrayOfboolean *array )
{
  if( array ) {
    if ( array->__ptr )
      delete [] array->__ptr;
    
    delete array;
  }
}

//------------------------------------------------------------------------
// Constructors / gSOAP to srm unwrappers 
// and destructors

// RequestStatus constructor
//
static RequestStatus g2srm_RequestStatus( class ns11__RequestStatus *ns11_rs )
{
  if ( ns11_rs == NULL )
    return NULL;

  RequestStatus rs = new _RequestStatus;
  if ( rs ) {
    rs->requestId      = (srm_int)ns11_rs->requestId;
    rs->type           = srm_strcpy(ns11_rs->type);
    rs->state          = srm_strcpy(ns11_rs->state);

    /* for some reason this package did not handle the date type correctly */
    rs->submitTime     = (srm_date) srm_strcpy( ns11_rs->submitTime );
    rs->startTime      = (srm_date) srm_strcpy( ns11_rs->startTime );
    rs->finishTime     = (srm_date) srm_strcpy( ns11_rs->finishTime );
    rs->estTimeToStart = (srm_int)  ns11_rs->estTimeToStart;
    rs->fileStatuses   = g2srm_ArrayOfRequestFileStatuses( ns11_rs->fileStatuses );
    rs->errorMessage   = srm_strcpy( ns11_rs->errorMessage );
    rs->retryDeltaTime = (srm_int) ns11_rs->retryDeltaTime;
  }
  return rs;
}

// RequestFileStatus constructor
//
static RequestFileStatus g2srm_RequestFileStatus(class ns11__RequestFileStatus* ns11_rfs)
{
  RequestFileStatus rfs = NULL;

  // Assert argument
  if( ns11_rfs == NULL )
    return NULL;

//   rfs = new struct _RequestFileStatus;
  rfs = new _RequestFileStatus;
  if ( rfs ) {
    rfs->state             = srm_strcpy( ns11_rfs->state );
    rfs->fileId            = (srm_int)   ns11_rfs->fileId;
    rfs->TURL              = srm_strcpy( ns11_rfs->TURL );
    rfs->estSecondsToStart = (srm_int)   ns11_rfs->estSecondsToStart;
    rfs->sourceFilename    = srm_strcpy( ns11_rfs->sourceFilename );
    rfs->destFilename      = srm_strcpy( ns11_rfs->destFilename );
    rfs->queueOrder        = (srm_int)   ns11_rfs->queueOrder;

    /* FileMetaData part */
    rfs->SURL              = srm_strcpy( ns11_rfs->SURL );
    rfs->size              = (srm_long)  ns11_rfs->size;
    rfs->owner             = srm_strcpy( ns11_rfs->owner );
    rfs->group             = srm_strcpy( ns11_rfs->group );
    rfs->permMode          = (srm_int)   ns11_rfs->permMode;
    rfs->checksumType      = srm_strcpy( ns11_rfs->checksumType );
    rfs->checksumValue     = srm_strcpy( ns11_rfs->checksumValue );
    rfs->isPinned          = (srm_boolean) ns11_rfs->isPinned;
    rfs->isPermanent       = (srm_boolean) ns11_rfs->isPermanent;
    rfs->isCached          = (srm_boolean) ns11_rfs->isCached;
  }
  return rfs;
}

// RequestFileStatus   destructor
//
static void free_RequestFileStatus( RequestFileStatus rfs )
{
  if( rfs ){
    srm_freestr( rfs->state );
    srm_freestr( rfs->TURL );
    srm_freestr( rfs->sourceFilename );
    srm_freestr( rfs->destFilename );

    srm_freestr( rfs->SURL );
    srm_freestr( rfs->owner );
    srm_freestr( rfs->group );
    srm_freestr( rfs->checksumType );
    srm_freestr( rfs->checksumValue );

    delete rfs;
  }
}

//  srm_array_of_RequestFileStatuses  constructor
//
static srm_array_of_RequestFileStatuses 
g2srm_ArrayOfRequestFileStatuses( class ArrayOfRequestFileStatus *fileStatuses )
{ 
  srm_array_of_RequestFileStatuses srm_file_statuses = NULL;
  
  if( fileStatuses == NULL )
    return NULL;

  srm_file_statuses = new struct _array_of_RequestFileStatuses;
  if ( srm_file_statuses  ) {
    int len, i, offset;

    len = fileStatuses->__size;
    srm_file_statuses->length = len;
    srm_file_statuses->array  = new  RequestFileStatus[len];
    offset = fileStatuses->__offset;

    for( i=0; i<len; i++ ) {
      srm_file_statuses->array[i] = 
	g2srm_RequestFileStatus( &(fileStatuses->__ptr[offset+i]) );
    }
  }
  return srm_file_statuses;
}

// srm_array_of_RequestFileStatuses  destructor
//
static void free_ArrayOfRequestFileStatuses( srm_array_of_RequestFileStatuses arr_rfs )
{
  if( arr_rfs ){
    if( arr_rfs->array ) {
      for( int i=0; i < arr_rfs->length; i++ ) 
	free_RequestFileStatus( arr_rfs->array[i] );
      delete [] arr_rfs->array;
    }
    delete arr_rfs;
  }
}

} // namespace srm
