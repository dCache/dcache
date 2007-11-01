/***************************************************************************
 srmImpl.h
 ***************************************************************************/

/***************************************************************************
 * srm types definitions
 ***************************************************************************/
#ifndef SRM_IMPL_H
#  define SRM_IMPL_H

#ifdef __cplusplus
extern "C" {
#endif

#include "srmWSStub.h"

#ifdef __cplusplus
namespace srm {
#endif

typedef xsd__string     srm_string;
typedef xsd__long       srm_long;
typedef xsd__int        srm_int;
typedef xsd__dateTime   srm_date;
typedef xsd__boolean    srm_boolean;

typedef struct ns11__FileMetaData      _FileMetaData;
typedef struct ns11__RequestFileStatus _RequestFileStatus;
/* typedef struct ns11__RequestStatus     _RequestStatus; */

typedef _FileMetaData       *FileMetaData;
typedef _RequestFileStatus  *RequestFileStatus;
/* typedef _RequestStatus      *RequestStatus; */


typedef struct _array_of_RequestFileStatuses
{
  srm_int       length;
  RequestFileStatus * array;
} * srm_array_of_RequestFileStatuses;


typedef struct _RequestStatus
{
  srm_int       requestId;
  srm_string    type;
  srm_string    state;
  srm_date      submitTime;
  srm_date      startTime;
  srm_date      finishTime;
  srm_int       estTimeToStart;
  srm_array_of_RequestFileStatuses
                fileStatuses;
  srm_string    errorMessage;
  srm_int       retryDeltaTime;
} * RequestStatus;


  /*
   * Arrays:
   */

typedef struct _array_of_strings 
{
  srm_int       length;
  srm_string*   array;
} * srm_array_of_strings;

typedef struct _array_of_long 
{
  srm_int       length;
  srm_long *    array;
} * srm_array_of_longs;

typedef struct _array_of_boolean 
{
  srm_int       length;
  srm_boolean * array;
} * srm_array_of_booleans;

typedef struct _array_of_FileMetaData
{
  srm_int length;
  FileMetaData* array;
} * srm_array_of_FileMetaDatas;


/*
 * More datatypes:
 */

struct _srm_host_info
{
  srm_string    hostname;
  srm_string    port;
  srm_string    path;
};

typedef struct _srm_host_info _srm_host_info;
typedef struct _srm_host_info *srm_host_info;


/***************************************************************************
 * some constants and defenitions
 ***************************************************************************/

#ifndef SRM_MAX_URL_LEN
#  define SRM_MAX_URL_LEN  1024
#endif

#define SRM_TRUE  ((srm_boolean)1)
#define SRM_FALSE ((srm_boolean)0)

/***************************************************************************
 * srm function declarations
 ***************************************************************************/
// Destructors for objects returned by functions:

void free_RequestStatus( RequestStatus rs );
void free_FileMetaData(  FileMetaData fmd );
void free_srm_array_of_FileMetaDatas( srm_array_of_FileMetaDatas );

//
srm_boolean ping( srm_host_info host_info );

srm_array_of_strings getProtocols( srm_host_info host_info );

//
RequestStatus getRequestStatus( srm_int         requestId,
				srm_host_info   host_info );

RequestStatus get( srm_array_of_strings  surls,
		   srm_array_of_strings  protocols,
		   srm_host_info         host_info );

RequestStatus put( srm_array_of_strings  sources,
		   srm_array_of_strings  dests,
		   srm_array_of_longs    sizes,
		   srm_array_of_booleans wantPerm,
		   srm_array_of_strings  protocols,
		   srm_host_info         host_info );

RequestStatus copy( srm_array_of_strings  srcSURLS,
		    srm_array_of_strings  destSURLS,
		    srm_array_of_booleans wantPerm,
		    srm_host_info         host_info );
//
RequestStatus getEstGetTime( srm_array_of_strings SURLS ,
			     srm_array_of_strings protocols,
			     srm_host_info        host_info );

RequestStatus getEstPutTime( srm_array_of_strings  src_names,
			     srm_array_of_strings  dest_names,
			     srm_array_of_longs    sizes,
			     srm_array_of_booleans wantPermanent,
			     srm_array_of_strings  protocols,
			     srm_host_info         host_info );
//
RequestStatus setFileStatus( srm_int       requestId,
			     srm_int       fileId,
			     srm_string    state,
			     srm_host_info host_info );

srm_array_of_FileMetaDatas getFileMetaData( srm_array_of_strings SURLS,
					    srm_host_info        host_info );
//
RequestStatus mkPermanent( srm_array_of_strings SURLS,
			   srm_host_info        host_info );

void       advisoryDelete( srm_array_of_strings SURLS,
		           srm_host_info        host_info );
//
RequestStatus pin(   srm_array_of_strings TURLS,
		     srm_host_info        host_info );

RequestStatus unPin( srm_array_of_strings TURLS,
		     srm_int              requestID,
		     srm_host_info        host_info );

/***************************************************************************
 * srm convinience function declarations
 ***************************************************************************/
//
// "Constructor/destructor, set/get' fiunctions for array of string, boolean, long
//

srm_array_of_strings  new_srm_array_of_strings(  srm_int length );
void free_srm_array_of_strings( srm_array_of_strings  string_array );

srm_array_of_booleans new_srm_array_of_booleans( srm_int length, srm_boolean *values );
void free_srm_array_of_booleans( srm_array_of_booleans boolean_array );

srm_array_of_longs    new_srm_array_of_longs(    srm_int length, srm_long    *values );
void free_srm_array_of_longs(    srm_array_of_longs    long_array );

srm_boolean set_element_srm_array_of_string( srm_array_of_strings string_array,
					     srm_int              pos,
					     srm_string           string );

srm_string  get_element_srm_array_of_string( srm_array_of_strings aos,
					     srm_int              pos );

#ifdef __cplusplus
} /* extern "C" */

} // namespace srm

#endif  /* __cplusplus */


#endif /*  SRM_IMPL_H */
