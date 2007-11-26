//gsoap tns schema namespace: http://tempuri.org/diskCacheV111.srm.server.SRMServerV1
//gsoap mime schema namespace: http://schemas.xmlsoap.org/wsdl/mime/
//gsoap tme schema namespace: http://www.themindelectric.com/
//gsoap soapenc schema namespace: http://schemas.xmlsoap.org/soap/encoding/
//gsoap ns11 schema namespace: http://www.themindelectric.com/package/diskCacheV111.srm/
//gsoap http schema namespace: http://schemas.xmlsoap.org/wsdl/http/
//gsoap ns13 schema namespace: http://www.themindelectric.com/package/
//gsoap ns12 schema namespace: http://www.themindelectric.com/package/java.lang/

//gsoap tns service namespace: http://srm.1.0.ns

//gsoap tns service location: http://stkendca3a.fnal.gov:24128/srm/managerv1
//gsoap tns service name: soapSRMServerV1

/*start primitive data types*/
typedef char * xsd__string;
typedef int xsd__int;
typedef char * xsd__dateTime;
typedef LONG64 xsd__long;
typedef bool xsd__boolean;

/*end primitive data types*/

class tns__getResponse {
   public: 
	class ns11__RequestStatus * _Result;
};

class tns__advisoryDeleteResponse {
   public: 
};

class ns11__RequestStatus {
   public: 
	xsd__int  requestId;
	xsd__string  type;
	xsd__string  state;
	xsd__dateTime  submitTime;
	xsd__dateTime  startTime;
	xsd__dateTime  finishTime;
	xsd__int  estTimeToStart;
	class ArrayOfRequestFileStatus * fileStatuses;
	xsd__string  errorMessage;
	xsd__int  retryDeltaTime;
};

class ArrayOfFileMetaData {
   public: 
	class ns11__FileMetaData * __ptr;
	int  __size;
	int  __offset;
};

class ArrayOfstring {
   public: 
	xsd__string * __ptr;
	int  __size;
	int  __offset;
};

class ArrayOflong {
   public: 
	xsd__long * __ptr;
	int  __size;
	int  __offset;
};

class tns__putResponse {
   public: 
	ns11__RequestStatus * _Result;
};

class ArrayOfRequestFileStatus {
   public: 
	class ns11__RequestFileStatus * __ptr;
	int  __size;
	int  __offset;
};

class tns__mkPermanentResponse {
   public: 
	ns11__RequestStatus * _Result;
};

class tns__copyResponse {
   public: 
	ns11__RequestStatus * _Result;
};

class tns__getEstGetTimeResponse {
   public: 
	ns11__RequestStatus * _Result;
};

class tns__getEstPutTimeResponse {
   public: 
	ns11__RequestStatus * _Result;
};

class tns__pinResponse {
   public: 
	ns11__RequestStatus * _Result;
};

class tns__pingResponse {
   public: 
	xsd__boolean  _Result;
};

class tns__getFileMetaDataResponse {
   public: 
	ArrayOfFileMetaData * _Result;
};

typedef xsd__boolean  tns__Boolean ;

class tns__getRequestStatusResponse {
   public: 
	ns11__RequestStatus * _Result;
};

class tns__getProtocolsResponse {
   public: 
	ArrayOfstring * _Result;
};

class tns__setFileStatusResponse {
   public: 
	ns11__RequestStatus * _Result;
};

typedef xsd__long  tns__Long ;

class ns11__FileMetaData {
   public: 
	xsd__string  SURL;
	xsd__long  size;
	xsd__string  owner;
	xsd__string  group;
	xsd__int  permMode;
	xsd__string  checksumType;
	xsd__string  checksumValue;
	xsd__boolean  isPinned;
	xsd__boolean  isPermanent;
	xsd__boolean  isCached;
};

class tns__unPinResponse {
   public: 
	ns11__RequestStatus * _Result;
};

typedef xsd__int  tns__Integer ;

class ArrayOfboolean {
   public: 
	xsd__boolean * __ptr;
	int  __size;
	int  __offset;
};

class ns11__RequestFileStatus : public ns11__FileMetaData { 
	xsd__string  state;
	xsd__int  fileId;
	xsd__string  TURL;
	xsd__int  estSecondsToStart;
	xsd__string  sourceFilename;
	xsd__string  destFilename;
	xsd__int  queueOrder;
};

//gsoap tns service method-action: ping "ping"
tns__ping( tns__pingResponse * out );
//gsoap tns service method-action: getEstPutTime "getEstPutTime"
tns__getEstPutTime( ArrayOfstring * arg0, ArrayOfstring * arg1, ArrayOflong * arg2, ArrayOfboolean * arg3, ArrayOfstring * arg4, tns__getEstPutTimeResponse * out );
//gsoap tns service method-action: put "put"
tns__put( ArrayOfstring * arg0, ArrayOfstring * arg1, ArrayOflong * arg2, ArrayOfboolean * arg3, ArrayOfstring * arg4, tns__putResponse * out );
//gsoap tns service method-action: getRequestStatus "getRequestStatus"
tns__getRequestStatus( xsd__int  arg0, tns__getRequestStatusResponse * out );
//gsoap tns service method-action: copy "copy"
tns__copy( ArrayOfstring * arg0, ArrayOfstring * arg1, ArrayOfboolean * arg2, tns__copyResponse * out );
//gsoap tns service method-action: getEstGetTime "getEstGetTime"
tns__getEstGetTime( ArrayOfstring * arg0, ArrayOfstring * arg1, tns__getEstGetTimeResponse * out );
//gsoap tns service method-action: getProtocols "getProtocols"
tns__getProtocols( tns__getProtocolsResponse * out );
//gsoap tns service method-action: advisoryDelete "advisoryDelete"
tns__advisoryDelete( ArrayOfstring * arg0, tns__advisoryDeleteResponse * out );
//gsoap tns service method-action: pin "pin"
tns__pin( ArrayOfstring * arg0, tns__pinResponse * out );
//gsoap tns service method-action: unPin "unPin"
tns__unPin( ArrayOfstring * arg0, xsd__int  arg1, tns__unPinResponse * out );
//gsoap tns service method-action: setFileStatus "setFileStatus"
tns__setFileStatus( xsd__int  arg0, xsd__int  arg1, xsd__string  arg2, tns__setFileStatusResponse * out );
//gsoap tns service method-action: mkPermanent "mkPermanent"
tns__mkPermanent( ArrayOfstring * arg0, tns__mkPermanentResponse * out );
//gsoap tns service method-action: get "get"
tns__get( ArrayOfstring * arg0, ArrayOfstring * arg1, tns__getResponse * out );
//gsoap tns service method-action: getFileMetaData "getFileMetaData"
tns__getFileMetaData( ArrayOfstring * arg0, tns__getFileMetaDataResponse * out );
