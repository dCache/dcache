//
// srmcp client for Storage Resource Manager, v1.0
// put/get file to/from SRM server
//
//
// Usage: 
//  srmcp [options] <file> <file>
//  srmcp --help
//    to see list of currently available options
//
// Alex Kulyavtsev, FNAL
//

#include "config.h"
#include "srmWSH.h"
#include "stdio.h"
#include <sys/time.h>

#include "stdlib.h"
#include "unistd.h"
#include <sys/wait.h>

#include "SrmSoapWsClient.hpp"
#include "SrmWsClientConfig.hpp"
#include "url.hpp"

#include <vector>
#include <iterator>
#include <string>
#include <strstream>

// some include files in gsi.h conflict with #include <string>, so it must go after it
//   and "SrmSoapWsClient.hpp":
#include "gsi.h"

//---------------
using namespace srm;

  // Keep track of the file CVS version in the binary file
  static char cvsid[] = "$Id: srmcp.cpp,v 1.8 2003-11-03 16:03:26 cvs Exp $";


class srmURL : public url {
public:

  enum {
    SrmSchemeUnknown=0, // default on construction
    SrmSchemeFILE,
    SrmSchemeFTP,
    SrmSchemeKFTP,
    SrmSchemeGSIFTP,
    SrmSchemeHTTP,
    SrmSchemeHTTPS,
    SrmSchemeSRM,
    SrmSchemeMAX
  } srmScheme_t;

  srmURL( const string& u )
    : url( u ) {};
  int getScheme();
};

int srmURL::getScheme()
{
  int schNum = SrmSchemeUnknown;
  string sch = this->scheme();

  if ( sch == "file" )        schNum = SrmSchemeFILE;
  else if ( sch == "ftp" )    schNum = SrmSchemeFTP;
  else if ( sch == "kftp" )   schNum = SrmSchemeKFTP;
  else if ( sch == "gsiftp" ) schNum = SrmSchemeGSIFTP;
  else if ( sch == "http" )   schNum = SrmSchemeHTTP;
  else if ( sch == "https" )  schNum = SrmSchemeHTTPS;
  else if ( sch == "srm" )    schNum = SrmSchemeSRM;
    
  return schNum;
}

int
main (int argc, char *argv[] )
{
  //------------------
  // srmcp client data:

  int delay   = 1; // set initial delay
  //---------------

  int      i;
  int      ret;
  int      all_done = 0;
  int      errors = 0;
  string   rqState;
  string   fileState;

  //  srmInt   requestId;
  RequestStatus *prs, *prsDone = 0;
  string   funcName = "some-call-to-srm()";

  int retCode = -1;
  //----------------
  int   ind;
  class SrmWsClientConfig *cfg = new SrmWsClientConfig();

  //--------------------
  ind = cfg->parseCLOptions0( argc, argv );

  ret = cfg->readConfig();
  if ( ret ) {
    cerr << "Can not read configuration, exiting" << endl;
    exit(1);
  }

  ind = cfg->parseCLOptions( argc, argv );

  if ( cfg->srmDebug ){
    cout << "Configuration: " << endl;
    cfg->printConfig();  // Print updated configuration:
  }

  //--------------------

  if ( cfg->urlCopyScript == "" ){
    cerr << "srmcp(): urlCopyScript is not defined, exiting" << endl;
    exit(1);
  }
  const char *executable = cfg->urlCopyScript.c_str();

  //----------------
  int narg = argc -ind;
  if ( narg < 2 || narg > 2 ) {
    if ( narg < 2  ) 
      cerr << "Too few arguments. Must be src and dst at least. Exiting. " << endl << endl;
    if ( narg > 2  ) 
      cerr << "Too many arguments. Exiting. " << endl << endl;
    cfg->usage( "srmcp" );
    exit(1);
  }
  char ** argf;
  argf = argv + ind;

  class srmURL *u1 = new srmURL( argf[0]);
  class srmURL *u2 = new srmURL( argf[1]);
  class srmURL *srmURL  = NULL;
  class srmURL *fileURL = NULL;

  // Binary mask,          // srm is 
  const int doLOCAL= 0;    //        ...not involved, both are local files
  const int doGET  = 1;    //        ...Source
  const int doPUT  = 2;    //        ...Destination
  const int doCOPY = 3;    //        ... both, (doGET | doPUT) - bitwise OR

  int toDo = doLOCAL;
  int schN;

  schN = u1->getScheme();
  if ( schN == srmURL::SrmSchemeSRM )
    toDo |= doGET;
  else if ( schN != srmURL::SrmSchemeFILE ) {
    cout << "Protocol=" << schN << " is not served, exiting" << endl;
    cout << "Must be 'srm:' or 'file:'" << endl;
    cout << "URL=" << u1->getURL() << endl;
    exit(1);
  }

  schN = u2->getScheme();
  if ( schN == srmURL::SrmSchemeSRM )
    toDo |= doPUT;
  else if ( schN != srmURL::SrmSchemeFILE ) {
    cout << "Protocol=" << schN << " is not served, exiting" << endl;
    cout << "Must be 'srm:' or 'file:'" << endl;
    cout << "URL=" << u2->getURL() << endl;
    exit(1);
  }

  switch ( toDo ) {
  case doGET:
    srmURL  = u1;
    fileURL = u2;
    funcName= "get()";
    break;
  case doPUT:
    fileURL = u1;
    srmURL  = u2;
    funcName= "put()";
    break;

  case doLOCAL:
    cout << "Will not copy file: to file:, exiting" << endl;
    exit(1);
  case doCOPY:
    cout << "Will not copy srm: to srm:, exiting" << endl;
    exit(1);
  default:
    cout << "srmcp(), internal error - can't decide to get() or put() ... exiting" << endl;
    exit(1);
  }

  //----------------
  string srmProtocol = cfg->webServiceProtocol;
  string srmHName = srmURL->host();
  string srmPort  = srmURL->port();
  string srmPath  = cfg->webServicePath;
  class SrmHostInfo srmHostInfo = SrmHostInfo( srmProtocol, srmHName, srmPort, srmPath );

  string srmHost = srmHostInfo.getInfo();

  if ( cfg->srmDebug )
    cout << "SRM Host Info =" << srmHost << endl;

  //---------------------------------
  // Init the gsoap runtime environment

  class SrmSoapWsClient *cs = new SrmSoapWsClient();

  if ( cfg->useGsiSsl ) {
    if ( cfg->srmDebug )
      cout << "Use GsiSSL" << endl;
      
    if ( cs->setupGSI() ) {
      cerr << "Can not register GSI plugin, exiting" << endl;
      exit (1);
    }

    if ( cs->setSrmURL( srmHost ) ) {
      cerr << "Can not set SRM URL, exiting" << endl;
      exit (1);
    }
  }
  //==============================================

  class VArrayOfstring protocols( cfg->srmProtocols );

  vector<string> v_srcs(1);
  vector<string> v_dsts(1);
  vector<long> v_size(1);      // or   vector<LONG64> v_size(2);
  vector<bool> v_perm(1);

  v_srcs[0] = argf[0]; // fix me,  v_srcs.push_back( argf[j] );
  v_dsts[0] = argf[1]; // fix me

  v_size[0] = 0; // File size (for space reservation) - not impl. - fix me
  v_perm[0] = 0; // Make file permanent - not impl.

  //---
  class VArrayOfstring  srcs(  v_srcs );
  class VArrayOfstring  dsts(  v_dsts );
  class VArrayOflong    sizes( v_size );
  class VArrayOfboolean permanent( v_perm );
  //---

  if ( cfg->srmDebug )
    cout << " call "<< funcName << " - wait for connection " << endl;

  switch ( toDo ) {
  case doGET:
    ret = cs->get( &srcs, &protocols, &prs );
    break;
  case doPUT:
    ret = cs->put( &srcs, &dsts, &sizes, &permanent, &protocols, &prs );
    break;
  default:
    cout << "srmcp(), internal error - toDo switch ... exiting" << endl;
    exit(1);
  }

  if ( cfg->srmDebug )
    cout << " Ret code for " << funcName << " = " << ret << endl;

  if( ret ) {
    cerr << " " << funcName << ": got error #" << ret << ", exiting" << endl;
    exit(1);
  }
  if( prs == NULL ) {
    cerr << " srmcp(): internal error - " << funcName << " return OK,"
      " but pointer to ret.stat. is NULL, exiting" << endl;
    exit(1);
  }


  //====================
  // Request sent. 
  // Do not exit until file status set to Done

  if ( cfg->srmDebug ) {
    cout << "requestId = " << prs->requestId << endl;
    cout << "type      = " << prs->type      << endl;
    cout << "state     = " << prs->state     << endl;
    if(  string(prs->state) == "Failed" )
          cout << "error     = " << prs->errorMessage << endl;
    for(i=0 ; i < prs->fileStatuses->__size; i++ ) {
      cout << "  FILE # " << i << " ID fileId = " << prs->fileStatuses->__ptr[i].fileId << endl;
      cout << "  FILE # " << i << " ID size   = " << prs->fileStatuses->__ptr[i].size << endl;
      cout << "  FILE # " << i << " ID SURL   = " << prs->fileStatuses->__ptr[i].SURL << endl;
    }
    cout.flush();
  }

  // prs already checked for nonzero 
  if (   string(prs->state) != "Failed" ) {
//     requestId = prs->requestId;
    all_done = 0;

    // Wait for SRM to get TURLs
    while( ! all_done ) {

      delay = prs->retryDeltaTime;
      if ( cfg->srmDebug ) 
	cout << " sleep " << delay << " sec ..." << endl << flush;
      sleep( delay );

      ret = cs->getRequestStatus( prs->requestId, &prs );
      if( ret ) {
	cerr << " getRequestStatus(): got error " << ret << ", exiting" << endl;
	errors++;
	goto Exit;
      }
      if( prs == NULL ) {
	cerr << " getRequestStatus(): internal error - getRequestStatus() return OK,"
	  " but pointer to ret.stat.==NULL, exiting" << endl;
	errors++;
	goto Exit;
      }

      // Scan request is done
      all_done = 1;
      if ( string(prs->state) != "Failed" ) {
	for( i=0; i < prs->fileStatuses->__size; i++ ) {
	  fileState = prs->fileStatuses->__ptr[i].state;
	  if(  fileState == "Pending" ) 
	    all_done = 0;
	}
      }

      //=============
      if ( cfg->srmDebug ) {
	cout << " Request ID= " << prs->requestId << endl;
	cout << " type      = " << prs->type      << endl;
	cout << " state     = " << prs->state     << endl;
	if(  string(prs->state) == "Failed" )
          cout << "error     = " << prs->errorMessage << endl;
	cout << " submit Time = "        << prs->submitTime << endl;
    
	for( i=0 ; i < prs->fileStatuses->__size; i++ ) {
	  cout << "  FILE # " << i << " ID fileId = " << prs->fileStatuses->__ptr[i].fileId << endl;
	  cout << "  FILE # " << i << " ID size   = " << prs->fileStatuses->__ptr[i].size << endl;
	  cout << "  FILE # " << i << " ID SURL   = " << prs->fileStatuses->__ptr[i].SURL << endl;
	  cout << "  FILE # " << i << " ID TURL   = " << prs->fileStatuses->__ptr[i].TURL << endl;
	  cout << "  FILE # " << i << " ID state  = [" << prs->fileStatuses->__ptr[i].state 
	       << "] " << endl;
	}

	if ( all_done )
	  cout << "All done =" << all_done << endl << flush;
      }
      //=============
    }

    // Scan if request done
    if ( string(prs->state) == "Failed" ) {
      cout << "Request Failed" << endl;      
      cout << "error     = " << prs->errorMessage << endl;
      errors++;
    }else{
      prsDone = 0;
      for( i=0; i < prs->fileStatuses->__size; i++ ) {
	fileState = prs->fileStatuses->__ptr[i].state;
	if( fileState != "Ready" ){
	  errors++;
	  if( fileState != "Done" ){  // That is for "Failed" or "Pending"
	    ret = cs->setFileStatus( prs->requestId, prs->fileStatuses->__ptr[i].fileId, 
				     (char *)"Done", &prsDone );
	    if ( cfg->srmDebug ) 
	      cout << " Set request.fileId=" 
		   << prs->requestId <<"."<< prs->fileStatuses->__ptr[i].fileId 
		   << " to Done "<< endl;
	  }
	}
      }
      if ( prsDone )
	prs = prsDone; // point request status to the place it was requested last time
    }
    
  }else
    errors++;

  //
  // Do transfer
  //
  if ( !errors ) {
    string sTurl;
    string sFile;
    class url *dstURL =0;
    class url *srcURL =0;

    switch ( toDo ) {
    case doGET:
      sTurl  = prs->fileStatuses->__ptr[0].TURL;
      srcURL = new class srmURL( sTurl );          // TURL

      sFile  = argf[1];
      dstURL = new class srmURL( sFile );          // args

      break;
    case doPUT:
      sFile  = argf[0];
      srcURL = new class srmURL( sFile );          // args

      sTurl  = prs->fileStatuses->__ptr[0].TURL;
      dstURL = new class srmURL( sTurl );          // TURL
      break;
    default:
      cout << "srmcp(), internal error - toDo switch ... exiting" << endl;
      errors++;
      goto Exit;
    }

    if ( cfg->srmDebug ) {
      cout << "DBG: from=" << srcURL->getURL() << endl << flush;
      cout << "DBG: to=  " << dstURL->getURL() << endl << flush;
    }

    char *exArgs[30];
    int   idx;
    int rc;
    const int sz = 21;
    static char cBfSize[sz]; 
    ostrstream os(cBfSize, sz, ios::out);

    os << cfg->bufferSize << ends; // Convert buffer size to ascii string and terminate by '0'

    string srcHP("");
    string dstHP("");
    //---
    srcHP = srcURL->host();
    if( srcHP == ""
	&& srcURL->scheme() == "file" )
      srcHP = "localhost";

    if( srcURL->port() != "" )
      srcHP += ":" + srcURL->port();
    //---
    dstHP = dstURL->host(); 
    if( dstURL->port() != "" )
      dstHP += ":" + dstURL->port();

    if( dstHP == ""
	&& dstURL->scheme() == "file" )
      dstHP = "localhost";
    //--- [ 0] -- LEN
    idx = 0;

//     cout << "DBG: orig ex=" << exArgs[0] << endl;
//     exArgs[ idx++] = "/bin/echo";

    exArgs[idx++] = (char *)cfg->urlCopyScript.c_str();

    exArgs[idx++] = "-debug";
    exArgs[idx++] = (char *)(( cfg->srmDebug ) ? "true" : "false" );
//     //----------- [  ]
//     exArgs[idx++] = "-x509_user_proxy";
//     exArgs[idx++] = (char *)cfg->x509_userProxy.c_str();

//     exArgs[idx++] = "-x509_user_key";
//     exArgs[idx++] = (char *)cfg->x509_userKey.c_str();

//     exArgs[idx++] = "-x509_user_cert";
//     exArgs[idx++] = (char *)cfg->x509_userCert.c_str();

//     exArgs[idx++] = "-x509_user_certs_dir";
//     exArgs[idx++] = (char *)cfg->x509_certDir.c_str();
    //-----------
    if( cfg->gsiFtpClient == "kftp" ){
      exArgs[idx++] = "-use-kftp";
      exArgs[idx++] = "true";
    }
    //-----------
    exArgs[idx++] = "-buffer_size";
    exArgs[idx++] = (char *)cBfSize;
    //-----------
    exArgs[idx++] = "-src-protocol";
    exArgs[idx++] = (char *)(srcURL->scheme().c_str());

    exArgs[idx++] = "-src-host-port";
    exArgs[idx++] = (char *)srcHP.c_str();

    exArgs[idx++] = "-src-path";
    exArgs[idx++] = (char *)srcURL->urlPath().c_str();
    //-----------
    exArgs[idx++] = "-dst-protocol";
    exArgs[idx++] = (char *)(dstURL->scheme().c_str());

    exArgs[idx++] = "-dst-host-port";
    exArgs[idx++] = (char *)dstHP.c_str();

    exArgs[idx++] = "-dst-path";
    exArgs[idx++] = (char *)dstURL->urlPath().c_str();
    //-----------
    exArgs[idx++] = NULL;

    //===============
    pid_t pid;

    pid = fork ();
    if( pid < 0 ){
      perror("srmcp()");
      errors++;
      goto Exit;
    }
    if ( pid ) { // got child pid, this is a parent
      int stat;
      int rc;

      rc = wait( &stat );
      if( rc < 0) { /* now wait for the child */
	if(errno != ECHILD) 
	  cerr << "srmcp(): An error occurred waiting for children" <<  endl;
      }else if ( rc > 0 ) {
	retCode = stat;	
	if ( cfg->srmDebug ) 
	  cout << "srmcp() Parent: Child process done, status =" << stat << endl << flush;
      }else{
	retCode = 0;
	if ( cfg->srmDebug ) 
	  cout << "srmcp() Parent: Child process done" << endl << flush;
      }

    }else{ //<---- Child process

      if ( cfg->srmDebug )
	cout << "Child: Started ... " << endl << flush;

      rc = execvp ( executable,  exArgs );

      // Will return only IF error
      if ( rc ) { 
	perror( "srmcp()" );
	cerr << "$PATH=" << getenv("PATH") << endl;
	cerr << "srmcp(): can not execvp() transfer process, exiting" << endl;
	exit(1);
      }
    } //<--- End of Child process

    delete srcURL;
    delete dstURL;
  }


 Exit:;
  // Scan Release files in the request if it was not done yet
  if ( prs && string(prs->state) != "Done" ) { 
    for( i=0; i < prs->fileStatuses->__size; i++ ) {
      fileState = prs->fileStatuses->__ptr[i].state;
      if( fileState != "Done" ) {
	if ( cfg->srmDebug ) 
	  cout << " Set request.fileId=" 
	       << prs->requestId <<"."<< prs->fileStatuses->__ptr[i].fileId 
	       << " to Done "<< endl;
	ret = cs->setFileStatus( prs->requestId, prs->fileStatuses->__ptr[i].fileId, 
				 (char *)"Done", &prsDone );
      }
    }
  }

  if ( cfg->srmDebug ) 
    cout << " Done "<< endl;

  delete u1;
  delete u2;
  delete cs;
  delete cfg;

  return (errors) ? 1:retCode;
}
