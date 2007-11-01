#include "soapH.h"
#include "srm.h"
#include "soapSRMServerV1.nsmap"

/***************************************************************************
 * srm private functions definitions
 ***************************************************************************/

static srm_string srm_strcpy(char* str);
static void srm_freestr(srm_string str);

static class ArrayOfstring *srm2g_string_array(    srm_array_of_strings string_array);
static class ArrayOflong *srm2g_long_array(srm_array_of_longs long_array);
static class ArrayOfboolean *srm2g_boolean_array(srm_array_of_booleans boolean_array);
static void   freeArrayOfstring(  class ArrayOfstring *  array);
static void   freeArrayOflong(    class ArrayOflong *    array);
static void   freeArrayOfboolean( class ArrayOfboolean * array);

static srm_boolean fillSrmUrl(char* srmurl,   srm_host_info host_info);
static RequestFileStatus g2srm_RequestFileStatus(class ns11__RequestFileStatus* ns11_rfs);
static srm_array_of_RequestFileStatuses g2srm_ArrayOfRequestFileStatuses(
              class ArrayOfRequestFileStatus *fileStatuses);
static RequestStatus g2srm_RequestStatus(class ns11__RequestStatus *ns11_rs);
static void free_RequestFileStatus(RequestFileStatus rfs);
static void free_ArrayOfRequestFileStatuses(  srm_array_of_RequestFileStatuses arr_rfs);

/***************************************************************************
 * srm functions implementations
 ***************************************************************************/


void free_RequestStatus( RequestStatus    rs)
{
  srm_freestr(rs->type);
  srm_freestr(rs->state);
  free_ArrayOfRequestFileStatuses(rs->fileStatuses);
  srm_freestr(rs->errorMessage);
  delete rs;
}

RequestStatus get( srm_array_of_strings surls,
  		      srm_array_of_strings protocols,
            srm_host_info host_info )
{

  struct soap soap;
  class ArrayOfstring *arg0 = srm2g_string_array(surls);
  class ArrayOfstring *arg1 = srm2g_string_array(protocols);
  class tns__getResponse out;
  RequestStatus rs;
  char srmurl[SRM_MAX_URL_LEN];
  //printf("get 1\n");
  soap_init(&soap);
  if(! fillSrmUrl( srmurl,  host_info)  )
  {
    freeArrayOfstring(arg0);
    freeArrayOfstring(arg1);
    soap_end(&soap);
    soap_done(&soap);
    //printf("get return NULL 1\n");
    return NULL;
  }

  //printf("get 2\n");
  if (soap_call_tns__get ( &soap, (const char*)srmurl, "get", 
                arg0, arg1, &out) != SOAP_OK)
  {
    //printf("get 3\n");
    freeArrayOfstring(arg0);
    freeArrayOfstring(arg1);
		soap_print_fault(&soap,stderr);
    soap_end(&soap);
    soap_done(&soap);
    //printf("get return NULL 2\n");
    return NULL;

  }
  //printf("get 3\n");

  freeArrayOfstring(arg0);
  //printf("get 4\n");

  freeArrayOfstring(arg1);
  //printf("get 5\n");

  rs = g2srm_RequestStatus(out._Result);
  //printf("get 6\n");

  soap_end(&soap);
  //printf("get 7\n");
  soap_done(&soap);
  //printf("get return rs\n");
  return rs;
}

 RequestStatus getRequestStatus( srm_int requestId,
            srm_host_info host_info )
  {
	struct soap soap;
    class tns__getRequestStatusResponse out;
    RequestStatus rs;
    char srmurl[SRM_MAX_URL_LEN];

	soap_init(&soap);
    if(! fillSrmUrl( srmurl,  host_info)  )
    {
    soap_end(&soap);
    soap_done(&soap);
    return NULL;
    }
	if (soap_call_tns__getRequestStatus(&soap, (const char*)srmurl, "getRequestStatus", (int)requestId, &out))
    {
		soap_print_fault(&soap,stderr);
        soap_end(&soap);
        soap_done(&soap);
        return NULL;
    }
    rs = g2srm_RequestStatus(out._Result);
    soap_end(&soap);
    soap_done(&soap);
    return rs;
}

RequestStatus setFileStatus( srm_int requestId,
                            srm_int fileId,
                            srm_string state,
                            srm_host_info host_info )
{
    struct soap soap;
    class tns__setFileStatusResponse out;
    RequestStatus rs;
    char srmurl[SRM_MAX_URL_LEN];
    char *statestr = (char*)state;

    if(   strcmp("Failed",statestr) != 0 &&
          strcmp("Pending",statestr) != 0 &&
          strcmp("Ready",statestr) != 0 &&
          strcmp("Running",statestr) != 0 &&
          strcmp("Done",statestr) != 0 )
    {
        /* the state is not valid, just do nothing */

        return NULL;
    }

    soap_init(&soap);

    if(! fillSrmUrl( srmurl,  host_info)  )
    {
        soap_end(&soap);
        soap_done(&soap);
        return NULL;
    }

    if (soap_call_tns__setFileStatus ( &soap, (const char*)srmurl, "setFileStatus",
                                        (int)requestId,(int)fileId,state, &out))
    {
        soap_print_fault(&soap,stderr);
        soap_end(&soap);
        soap_done(&soap);
        return NULL;

    }

    rs = g2srm_RequestStatus(out._Result);
    soap_end(&soap);
    soap_done(&soap);
    return rs;
}

RequestStatus put( srm_array_of_strings sources,
			      srm_array_of_strings dests,
			      srm_array_of_longs sizes,
			      srm_array_of_booleans wantPerm,
			      srm_array_of_strings protocols,
                  srm_host_info host_info )
{

	struct soap soap;
    class ArrayOfstring *arg0 = srm2g_string_array(sources);
    class ArrayOfstring *arg1 = srm2g_string_array(dests);
    class ArrayOflong *arg2 = srm2g_long_array(sizes);
    class ArrayOfboolean *arg3 = srm2g_boolean_array(wantPerm);
    class ArrayOfstring *arg4 = srm2g_string_array(protocols);
    class tns__putResponse out;
    RequestStatus rs;
    char srmurl[SRM_MAX_URL_LEN];

	soap_init(&soap);

    if( ! fillSrmUrl( srmurl,  host_info)  )
    {
        freeArrayOfstring(arg0);
        freeArrayOfstring(arg1);
        freeArrayOflong(arg2);
        freeArrayOfboolean(arg3);
        freeArrayOfstring(arg4);
        soap_end(&soap);
        soap_done(&soap);
        return NULL;
    }

	if (soap_call_tns__put(&soap, (const char*)srmurl, "put",
       arg0,
       arg1,
       arg2,
       arg3,
       arg4,
       &out))
    {
        freeArrayOfstring(arg0);
        freeArrayOfstring(arg1);
        freeArrayOflong(arg2);
        freeArrayOfboolean(arg3);
        freeArrayOfstring(arg4);
        soap_print_fault(&soap,stderr);
        soap_end(&soap);
        soap_done(&soap);
        return NULL;

    }

    freeArrayOfstring(arg0);
    freeArrayOfstring(arg1);
    freeArrayOflong(arg2);
    freeArrayOfboolean(arg3);
    freeArrayOfstring(arg4);
    rs = g2srm_RequestStatus(out._Result);
    soap_end(&soap);
    soap_done(&soap);
    return rs;
}

  srm_boolean ping(
            srm_host_info host_info)
  {
    //SOAP_FMAC1 int SOAP_FMAC2 soap_call_tns__ping(struct soap*, const char*, const char*, struct tns__pingResponse *);
    return SRM_TRUE;
  }

 RequestStatus copy( srm_array_of_strings srcSURLS,
            srm_array_of_strings destSURLS,
			      srm_array_of_booleans wantPerm,
            srm_host_info host_info )
  {
	struct soap soap;
    class ArrayOfstring *arg0 = srm2g_string_array(srcSURLS);
    class ArrayOfstring *arg1 = srm2g_string_array(destSURLS);
    class ArrayOfboolean *arg2 = srm2g_boolean_array(wantPerm);
    class tns__copyResponse out;
    RequestStatus rs;
    char srmurl[SRM_MAX_URL_LEN];

	soap_init(&soap);

    if( ! fillSrmUrl( srmurl,  host_info)  )
    {
        freeArrayOfstring(arg0);
        freeArrayOfstring(arg1);
        freeArrayOfboolean(arg2);
        soap_end(&soap);
        soap_done(&soap);
        return NULL;
    }

	if (soap_call_tns__copy(&soap, (const char*)srmurl, "copy",
       arg0,
       arg1,
       arg2,
       &out))
    {
        freeArrayOfstring(arg0);
        freeArrayOfstring(arg1);
        freeArrayOfboolean(arg2);
        soap_print_fault(&soap,stderr);
        soap_end(&soap);
        soap_done(&soap);
        return NULL;

    }

    freeArrayOfstring(arg0);
    freeArrayOfstring(arg1);
    freeArrayOfboolean(arg2);
    rs = g2srm_RequestStatus(out._Result);
    soap_end(&soap);
    soap_done(&soap);
    return rs;
  }





/***************************************************************************
 * srm convinience function implementations
 ***************************************************************************/

  srm_array_of_strings new_srm_array_of_strings(srm_int length)
  {
    srm_array_of_strings aos = new struct _array_of_strings;
    aos->length = length;
    aos->array =  new srm_string[length];
    memset(aos->array,0,sizeof(srm_string)*length);
    return aos;
  }

  srm_boolean set_element_srm_array_of_string(srm_array_of_strings aos,
            srm_int pos,
            srm_string string)
  {
    if(aos == NULL)
    {
      return SRM_FALSE;
    }

    if(pos <0 && pos >= aos->length)
    {
      return SRM_FALSE;
    }
    aos->array[pos] = string;
    return SRM_TRUE;
  }

  srm_string get_element_srm_array_of_string(srm_array_of_strings aos,
            srm_int pos)
  {
    if(aos == NULL)
    {
      return NULL;
    }

    if(pos <0 && pos >= aos->length)
    {
      return NULL;
    }
    return  aos->array[pos];
  }

  void free_srm_array_of_strings( srm_array_of_strings aos)
  {
     if(aos == NULL)
     {
       return;
     }

     delete [] aos->array;
     delete aos;
  }

  srm_array_of_booleans new_srm_array_of_booleans(srm_int length, srm_boolean* values)
  {
    srm_array_of_booleans boolean_array;

    if(length <0 || (length >0 && values == NULL))
    {
      return NULL;
    }
    boolean_array = new struct _array_of_boolean;
    if( boolean_array == NULL)
    {
      return NULL;
    }
    boolean_array->length = length;
    if(length == 0)
    {
      boolean_array->array =NULL;
      return  boolean_array;
    }
    boolean_array->array = new srm_boolean[length];
    if(  boolean_array->array == NULL)
    {
      delete boolean_array;
      return NULL;
    }

    memcpy(boolean_array->array, values, length* sizeof(srm_boolean));

    return boolean_array;
  }

  srm_array_of_longs new_srm_array_of_longs(srm_int length, srm_long* values)
  {
    srm_array_of_longs long_array;

    if(length <0 || (length >0 && values == NULL))
    {
      return NULL;
    }
    long_array = new struct _array_of_long;
    if( long_array == NULL)
    {
      return NULL;
    }
    long_array->length = length;
    if(length == 0)
    {
        long_array->array = NULL;
        return  long_array;
    }
    long_array->array = new srm_long[length];
    if(  long_array->array == NULL)
    {
      delete long_array;
      return NULL;
    }

    memcpy(long_array->array, values, length* sizeof(srm_long));

    return long_array;
  }

  void free_srm_array_of_booleans( srm_array_of_booleans boolean_array)
  {
    delete [] boolean_array->array;
    delete boolean_array;
  }

  void free_srm_array_of_longs( srm_array_of_longs long_array)
  {
    delete [] long_array->array;
    delete long_array;
  }


/***************************************************************************
 * srm private functions implementations
 ***************************************************************************/

static srm_string srm_strcpy(char* str)
{
  if(str == NULL)
  {
    //printf("srm_strcpy NULL\n");
    return NULL;
  }
  
  //printf("srm_strcpy %s\n",str);
  srm_string srmstr;


  srmstr = (srm_string)malloc(strlen(str)+1);
  strcpy( (char*) srmstr,str);
  return srmstr;
}

static void srm_freestr(srm_string str)
{
  free(str);
}


static srm_boolean fillSrmUrl(char* srmurl,   srm_host_info host_info)
{
  int n;
  if(host_info == NULL)
  {
    n = snprintf(srmurl, SRM_MAX_URL_LEN,"http://%s:%s/%s",
              DEFAULT_SRM_HOSTNAME,
              DEFAULT_SRM_PORT,
              DEFAULT_SRM_PATH);
  }
  else
  {
    n = snprintf(srmurl, SRM_MAX_URL_LEN,"http://%s:%s/%s",
              (char*) (host_info->hostname),
              (char*) (host_info->port),
              (char*) (host_info->path));
  }

  if (n > -1 && n < SRM_MAX_URL_LEN)
  {
    return SRM_TRUE;
  }
  return SRM_FALSE;
  //"http://131.225.81.191:24333/srm/managerv1"
}



static class ArrayOfstring *srm2g_string_array(    srm_array_of_strings string_array)
{
  class ArrayOfstring *array ;
  int i;
  int len;
  if (string_array == NULL)
  {
    return NULL;
  }

  
  array = new ArrayOfstring();
  if(array == NULL)
  {
     return NULL;
  }
  array->__offset = 0;
  len = (int) string_array->length;
  array->__size=len;
  array->__ptr = new char*[len];
  for(i = 0;i<len;++i)
  {
    array->__ptr[i] =  string_array->array[i];
  }
  return      array;

}

static void  freeArrayOfstring(  class ArrayOfstring * array)
{
  if(array->__ptr != NULL)
  {
    delete [] array->__ptr;
  }
  delete array;
}


static class ArrayOflong *srm2g_long_array(srm_array_of_longs long_array)
{
  class ArrayOflong *array;
  int i;
  int len;
  if (long_array == NULL)
  {
    return NULL;
  }

  array = new ArrayOflong();
  if( array == NULL)
  {
    return NULL;
  }

  len = (int) long_array->length;
  array->__size = len;
  array->__offset = 0;
  array->__ptr = new LONG64[len];
  for(i = 0;i<len;++i)
  {
    array->__ptr[i] = (LONG64)  long_array->array[i];
  }
  return      array;

}

static void  freeArrayOflong(    class ArrayOflong * array)
{
  if(array->__ptr != NULL)
  {
    delete [] array->__ptr;
  }
  delete array;
}

static class ArrayOfboolean *srm2g_boolean_array(srm_array_of_booleans boolean_array)
{
  class ArrayOfboolean *array;
  int i;
  int len;
  if (boolean_array == NULL)
  {
    return NULL;
  }

  array = new ArrayOfboolean();
  if( array == NULL)
  {
    return NULL;
  }
  len = (int) boolean_array->length;
  array->__size = len;
  array->__offset = 0;
  array->__ptr =new bool[len];
  for(i = 0;i<len;++i)
  {
    if( boolean_array->array[i])
    {
      array->__ptr[i]= true;
    }
    else
    {
      array->__ptr[i]= false;
    }
  }

  return      array;
}

static void  freeArrayOfboolean(    struct ArrayOfboolean * array)
{
  if(array->__ptr != NULL)
  {
    delete [] array->__ptr;
  }
  delete array;
}


static RequestFileStatus g2srm_RequestFileStatus(class ns11__RequestFileStatus* ns11_rfs)
{
  //int j=0;
  //printf("g2srm_RequestFileStatuses %d\n",++j);//1
  RequestFileStatus rfs;
  //printf("g2srm_RequestFileStatuses %d\n",++j);//2

  if( ns11_rfs == NULL)
  {
    return NULL;
  }

  //printf("g2srm_RequestFileStatuses %d\n",++j);//3
  rfs = new struct _RequestFileStatus;
  //printf("g2srm_RequestFileStatuses %d\n",++j);//4

  if(rfs == NULL)
  {
     return NULL;
  }


  //printf("g2srm_RequestFileStatuses %d\n",++j);//5
  rfs->state = srm_strcpy(ns11_rfs->state);
  //printf("g2srm_RequestFileStatuses %d\n",++j);//6
  rfs->fileId = (srm_int)ns11_rfs->fileId;

  //printf("g2srm_RequestFileStatuses %d\n",++j);//7
  rfs->TURL = srm_strcpy(ns11_rfs->TURL);
  //printf("g2srm_RequestFileStatuses %d\n",++j);//8
  rfs->estSecondsToStart = (srm_int)ns11_rfs->estSecondsToStart;
  //printf("g2srm_RequestFileStatuses %d\n",++j);//9
  rfs->sourceFilename = srm_strcpy(ns11_rfs->sourceFilename);
  rfs->destFilename = srm_strcpy(ns11_rfs->destFilename);
  rfs->queueOrder = (srm_int)ns11_rfs->queueOrder;

  /* FileMetaData part */
  //printf("g2srm_RequestFileStatuses %d\n",++j);//10
  rfs->SURL = srm_strcpy(ns11_rfs->SURL);
  //printf("g2srm_RequestFileStatuses %d\n",++j);//11
  rfs->size = (srm_long)ns11_rfs->size;
  //printf("g2srm_RequestFileStatuses %d\n",++j);//12
  rfs->owner = srm_strcpy(ns11_rfs->owner);
  rfs->group = srm_strcpy( ns11_rfs->group);
  rfs->permMode = (srm_int)ns11_rfs->permMode;
  rfs->checksumType = srm_strcpy(ns11_rfs->checksumType);
  rfs->checksumValue = srm_strcpy(ns11_rfs->checksumValue);
  rfs->isPinned = (srm_boolean)ns11_rfs->isPinned;
  rfs->isPermanent = (srm_boolean)ns11_rfs->isPermanent;
  rfs->isCached = (srm_boolean)ns11_rfs->isCached;
  /**/
  return rfs;

}



static srm_array_of_RequestFileStatuses g2srm_ArrayOfRequestFileStatuses(
              class ArrayOfRequestFileStatus *fileStatuses)
{ 
  //int j=0;
  //printf("srm_array_of_RequestFileStatuses %d\n",++j);//1
  int len,i,offset;
  //printf("srm_array_of_RequestFileStatuses %d\n",++j);//2
  srm_array_of_RequestFileStatuses srm_file_statuses;
  //printf("srm_array_of_RequestFileStatuses %d\n",++j);//3
  if(  fileStatuses == NULL)
  {
    return NULL;
  }

  //printf("srm_array_of_RequestFileStatuses %d\n",++j);//4
  srm_file_statuses = new struct _array_of_RequestFileStatuses;

  //printf("srm_array_of_RequestFileStatuses %d\n",++j);//5
  if(     srm_file_statuses == NULL)
  {
    return NULL;
  }

  //printf("srm_array_of_RequestFileStatuses %d\n",++j);//6
  len = fileStatuses->__size;
  //printf("srm_array_of_RequestFileStatuses %d\n",++j);//7
  srm_file_statuses->length = len;
  //printf("srm_array_of_RequestFileStatuses %d\n",++j);//8
  srm_file_statuses->array = new  RequestFileStatus[len];
  //printf("srm_array_of_RequestFileStatuses %d\n",++j);//9
  offset = fileStatuses->__offset;
  //printf("srm_array_of_RequestFileStatuses %d\n",++j);//10
  for(i=0;i<len;++i)
  {
    srm_file_statuses->array[i] = 
        g2srm_RequestFileStatus(&(fileStatuses->__ptr[offset+i]));
  }
  //printf("srm_array_of_RequestFileStatuses %d\n",++j);
  return      srm_file_statuses;
}



static RequestStatus g2srm_RequestStatus(class ns11__RequestStatus *ns11_rs)
{
  //int i = 0;
  //printf("g2srm_RequestStatus %d\n",++i);//1
  if(ns11_rs == NULL)
  {
     return NULL;
  }

  //printf("g2srm_RequestStatus %d\n",++i);//2
  RequestStatus rs = new _RequestStatus;

  //printf("g2srm_RequestStatus %d\n",++i);//3
  if(rs == NULL)
  {
     return NULL;
  }


  //printf("g2srm_RequestStatus %d\n",++i);//4
  rs->requestId = (srm_int)ns11_rs->requestId;
 // printf("g2srm_RequestStatus %d\n",++i);//5
  rs->type = srm_strcpy(ns11_rs->type);
  //printf("g2srm_RequestStatus %d\n",++i);//6
  rs->state = srm_strcpy(ns11_rs->state);
  //printf("g2srm_RequestStatus %d\n",++i);//7
  /* for some reason this package did not handle the date type correctly */
  rs->submitTime = (srm_date)srm_strcpy(ns11_rs->submitTime);
  rs->startTime = (srm_date)srm_strcpy(ns11_rs->startTime);
  rs->finishTime = (srm_date)srm_strcpy(ns11_rs->finishTime);
  rs->estTimeToStart = (srm_int)ns11_rs->estTimeToStart;
  //printf("g2srm_RequestStatus %d\n",++i);//8
  rs->fileStatuses = g2srm_ArrayOfRequestFileStatuses(ns11_rs->fileStatuses);
  //printf("g2srm_RequestStatus %d\n",++i);
  rs->errorMessage = srm_strcpy(ns11_rs->errorMessage);
  //printf("g2srm_RequestStatus %d\n",++i);
  rs->retryDeltaTime = (srm_int)ns11_rs->retryDeltaTime;
  //printf("g2srm_RequestStatus %d\n",++i);
  return rs;

}

static void free_RequestFileStatus(RequestFileStatus rfs)
{
  if(rfs == NULL)
  {
    return;
  }
  srm_freestr(rfs->state);
  srm_freestr(rfs->TURL);
  srm_freestr(rfs->sourceFilename);
  srm_freestr(rfs->destFilename);
  srm_freestr(rfs->SURL);
  srm_freestr(rfs->owner);
  srm_freestr(rfs->group);
  srm_freestr(rfs->checksumType);
  srm_freestr(rfs->checksumValue);
  delete rfs;
}


static void free_ArrayOfRequestFileStatuses(  srm_array_of_RequestFileStatuses arr_rfs)
{
  int i;
  if(arr_rfs == NULL)
  {
     return;
  }
  if(arr_rfs->array != NULL)
  {
    for(i = 0; i<arr_rfs->length; ++i)
    {
       free_RequestFileStatus(arr_rfs->array[i]);
    }
    delete [] arr_rfs->array;
  }
  delete arr_rfs;

}

