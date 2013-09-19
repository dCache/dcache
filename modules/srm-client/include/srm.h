/***************************************************************************
                          srm.h  -  description
                             -------------------
    begin                : Wed Jul 24 2002
    copyright            : (C) 2002 by Timur Perelmutov
    email                : timur@fnal.gov
 ***************************************************************************/

/***************************************************************************
 * srm types definitions
 ***************************************************************************/
#ifndef SRM_H
#  define SRM_H

#ifdef __cplusplus
extern "C" {
#endif

typedef int srm_boolean;


typedef char* srm_date;
typedef char* srm_string;
typedef long long srm_long;
typedef int srm_int;

typedef struct _array_of_strings
{
  srm_int length;
  srm_string* array;
} * srm_array_of_strings;

typedef struct _array_of_long
{
  srm_int length;
  srm_long* array;
} * srm_array_of_longs;

typedef struct _array_of_boolean
{
  srm_int length;
  srm_boolean* array;
} * srm_array_of_booleans;

typedef struct _FileMetaData
{
  srm_string SURL;
  srm_long size;
  srm_string owner;
  srm_string group;
  srm_int permMode;
  srm_string checksumType;
  srm_string checksumValue;
  srm_boolean isPinned;
  srm_boolean isPermanent;
  srm_boolean isCached;
} * FileMetaData;

typedef struct _array_of_FileMetaData
{
  srm_int length;
  FileMetaData* array;
} * srm_array_of_FileMetaDatas;

typedef struct _RequestFileStatus
{
  /* these fields are the same as in   FileMetaData
   * it is safe to cast  RequestFileStatus to    FileMetaData
  */
  srm_string SURL;
  srm_long size;
  srm_string owner;
  srm_string group;
  srm_int permMode;
  srm_string checksumType;
  srm_string checksumValue;
  srm_boolean isPinned;
  srm_boolean isPermanent;
  srm_boolean isCached;
  /* these are additional fields */
  srm_string state;
  srm_int fileId;
  srm_string TURL;
  srm_int estSecondsToStart;
  srm_string sourceFilename;
  srm_string destFilename;
  srm_int queueOrder;
} *RequestFileStatus;


typedef struct _array_of_RequestFileStatuses
{
  srm_int length;
  RequestFileStatus* array;
} * srm_array_of_RequestFileStatuses;

typedef struct _RequestStatus
{
  srm_int requestId;
  srm_string type;
  srm_string state;
  srm_date submitTime;
  srm_date startTime;
  srm_date finishTime;
  srm_int estTimeToStart;
  srm_array_of_RequestFileStatuses fileStatuses;
  srm_string errorMessage;
  srm_int retryDeltaTime;
} *RequestStatus;

typedef struct _srm_host_info
{
  srm_string hostname;
  srm_string port;
  srm_string path;
} *srm_host_info;

/***************************************************************************
 * some constants and defenitions
 ***************************************************************************/
#ifndef DEFAULT_SRM_HOSTNAME
#  define DEFAULT_SRM_HOSTNAME   "fnisd1.fnal.gov"
#endif

#ifndef DEFAULT_SRM_PORT
#  define DEFAULT_SRM_PORT   "24129"
#endif

#ifndef DEFAULT_SRM_PATH
#  define DEFAULT_SRM_PATH   "srm/managerv1"
#endif

#ifndef SRM_MAX_URL_LEN
#  define SRM_MAX_URL_LEN  1024
#endif

#define SRM_TRUE 1
#define SRM_FALSE 0

/***************************************************************************
 * srm function declarations
 ***************************************************************************/

  RequestStatus get( srm_array_of_strings surls,
  		      srm_array_of_strings protocols,
            srm_host_info host_info );

  RequestStatus put( srm_array_of_strings sources,
			      srm_array_of_strings dests,
			      srm_array_of_longs sizes,
			      srm_array_of_booleans wantPerm,
			      srm_array_of_strings protocols,
            srm_host_info host_info );

  RequestStatus copy( srm_array_of_strings srcSURLS,
            srm_array_of_strings destSURLS,
			      srm_array_of_booleans wantPerm,
            srm_host_info host_info );

  RequestStatus mkPermanent( srm_array_of_strings SURLS,
            srm_host_info host_info );

  RequestStatus getRequestStatus( srm_int requestId,
            srm_host_info host_info );

  srm_boolean ping(
            srm_host_info host_info);

  RequestStatus pin( srm_array_of_strings TURLS,
            srm_host_info host_info );

  RequestStatus unPin( srm_array_of_strings TURLS,
            srm_int requestID,
            srm_host_info host_info );

  RequestStatus getEstGetTime( srm_array_of_strings SURLS ,
            srm_array_of_strings protocols,
            srm_host_info host_info );

  RequestStatus getEstPutTime( srm_array_of_strings src_names,
			  		srm_array_of_strings dest_names,
		  			srm_array_of_longs sizes,
	  				srm_array_of_booleans wantPermanent,
  					srm_array_of_strings protocols,
            srm_host_info host_info);

  srm_array_of_FileMetaDatas getFileMetaData( srm_array_of_strings SURLS,
            srm_host_info host_info );

  RequestStatus setFileStatus( srm_int requestId,
	  				srm_int fileId,
  					srm_string state,
            srm_host_info host_info );

  void advisoryDelete( srm_array_of_strings SURLS,
            srm_host_info host_info);

  srm_array_of_strings getProtocols(
            srm_host_info host_info);


/***************************************************************************
 * srm convinience function declarations
 ***************************************************************************/

  srm_array_of_strings new_srm_array_of_strings(srm_int length);
  srm_array_of_booleans new_srm_array_of_booleans(srm_int length, srm_boolean* values);
  srm_array_of_longs new_srm_array_of_longs(srm_int length, srm_long* values);
  void free_srm_array_of_booleans( srm_array_of_booleans boolean_array);
  void free_srm_array_of_longs( srm_array_of_longs long_array);
  void free_srm_array_of_strings( srm_array_of_strings string_array);

  srm_boolean set_element_srm_array_of_string(srm_array_of_strings string_array,
            srm_int pos,
            srm_string string);

  srm_string get_element_srm_array_of_string(srm_array_of_strings aos,
            srm_int pos);


  void free_RequestStatus( RequestStatus    rs);
#ifdef __cplusplus
} /* extern "C" */
#endif  /* __cplusplus */


#endif
