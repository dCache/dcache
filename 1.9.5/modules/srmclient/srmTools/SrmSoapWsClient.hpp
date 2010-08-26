// $Id: SrmSoapWsClient.hpp,v 1.3 2003-10-07 21:09:16 cvs Exp $
//
// File: SrmSoapWsClient.cpp
//
// Purpose: class to implement Client side communications  
//          to Storage Resource Manager (SRM) server
//

#ifndef _SrmSoapWsClient_H__Defined_
#define _SrmSoapWsClient_H__Defined_

#include <vector>
#include <iterator>
#include <string>

#include "srmWSStub.h"

using namespace std;

namespace srm {

  typedef xsd__string     srmCString;
  typedef xsd__long       srmLong;
  typedef xsd__int        srmInt;
  typedef xsd__dateTime   srmDateTime;
  typedef xsd__boolean    srmBoolean;

//   typedef struct ns11__FileMetaData      FileMetaData;
//   typedef struct ns11__RequestFileStatus RequestFileStatus;
typedef class ns11__RequestStatus     RequestStatus;

//   	ArrayOfFileMetaData *_Result;
// 	ArrayOfstring *_Result;
// 	bool _Result;


  class SrmSoapWsClient{
  public:
    SrmSoapWsClient();
    SrmSoapWsClient( struct soap *s );
    ~SrmSoapWsClient();

    struct soap * getSoap() { return _soap; };

    int setupGSI();

    // Web Services Proxy Functions
    // All but few functions have response of type "RequestStatus *",
    //   that few exceptions are marked with "// (response)"

    // set of functions with gSOAP style args (like ArrayOfstring, etc. )

    // perm -- want permanent
    // prot -- protocol(s)

    int ping( bool *out);                     // (response)
    int getProtocols( ArrayOfstring **out);   // (response)

    int getRequestStatus(   int rqID,   RequestStatus  **out);
    int get(    ArrayOfstring  *sURL,   ArrayOfstring   *prot,  RequestStatus   **out);
    int put(    ArrayOfstring  *src,    ArrayOfstring   *dst,   ArrayOflong      *size, 
	        ArrayOfboolean *perm,   ArrayOfstring   *prot,  RequestStatus   **out);
    int copy(   ArrayOfstring  *sSURL,  ArrayOfstring   *dSURL, ArrayOfboolean   *perm,  
		RequestStatus **out);
    
    int getEstGetTime( ArrayOfstring *SURL,      ArrayOfstring *prot,     RequestStatus **out);
    int getEstPutTime( ArrayOfstring *srcName,   ArrayOfstring *dstName,  ArrayOflong    *size, 
		       ArrayOfboolean*perm,      ArrayOfstring *prot,     RequestStatus **out);
    
    int pin(    ArrayOfstring  *TURLs,  RequestStatus **out);
    int unPin(  ArrayOfstring  *TURLs,  int RequestID,          RequestStatus **out);
    
    int getFileMetaData( ArrayOfstring *SURLs,   ArrayOfFileMetaData **out); // (response)
    int setFileStatus(   int RequestID, int fileID,   char *state,        RequestStatus **out);
    int mkPermanent(     ArrayOfstring *SURLs,                  RequestStatus **out );
    int advisoryDelete(  ArrayOfstring *SURLs );  // no response expected

    int  setSrmURL( string& s ) { _srmURL = s; return 0; };

  private:
    struct soap *_soap;
    string _srmURL;

    static struct gsi_plugin_data *_gsiPluginData;

    const char *  _getSrmURL();

    void _processSoapFault();
  };

  int setupGSI( class SrmSoapWsClient *cs );

  //
  //
  //
  class SrmHostInfo {
  public:
    // No default constructor

    SrmHostInfo( string protocol, string host, string port, string path )
      : _protocol(protocol), _hostname(host), _port(port), _path(path) 
    { 
      _hostInfo = protocol+ "://" +host+ ":" +port+ "/" +path;
    };

    string getProtocol() { return _protocol; };
    string getHost() { return _hostname; };
    string getPort() { return _port; };
    string getPath() { return _path; };
    string getInfo() { return _hostInfo; };

  private:
    string    _protocol;
    string    _hostname;
    string    _port;
    string    _path;
    string    _hostInfo;
  };


  // Helper classes
  //
  // Types of arguments for SRM proxy functions are standard gSOAP classes, like ArrayOfxxx
  // Classes below inheret from gSOAP clases ArrayOfxxx, 
  //  but have added constructor with argument ( vector<xxx> ).
  // 
  // For long and bool constructor copies content of vector. 
  //
  // Constructor of VArrayOfstring is shallow,
  //   it does not copy strings itself anywere, and uses character arrays whichever vector<string>
  //   has inside. It copies only pointers to that character C-string representation.
  //   That is, you can not modify, append or delete (what else?) vector<string> used to construct
  //   VArrayOfstring, otherwise wait for trouble.

  class VArrayOfstring : public ArrayOfstring {
  public:
    VArrayOfstring( vector<std::string>& );
  private:
    vector<char *> __v; // keeps pointers to original C-strings in vector<string>
  };

  class VArrayOflong : public ArrayOflong {
  public:
    VArrayOflong( vector<LONG64>& );
    VArrayOflong( vector<long>& );
  private:
    vector<LONG64> __v;
  };

  class VArrayOfboolean : public ArrayOfboolean {
  public:
    VArrayOfboolean( vector<bool> &v );
    virtual ~VArrayOfboolean() { delete [] __v; };
  private:
    bool *__v;
  };

} // namespace srm

#endif /* end  _SrmSoapWsClient_H__Defined_ */
