//
// srmput client for Storage Resource Manager, v1.0
// put file on SRM server
//
//
// Usage: 
//  srmput [options] <local-file> <remote-file>
//  srmput --help
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

// some include files in gsi.h conflict with #include <string>, so it must go after it
//   and "SrmSoapWsClient.hpp":
#include "gsi.h"

//---------------
using namespace srm;

  // Keep track of the file CVS version in the binary file
  static char cvsid[] = "$Id: srmput.cpp,v 1.3 2003-10-07 20:18:59 cvs Exp $";


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

  int delay   = 2;
  //---------------

  int      i;
  int      ret;
  int      all_done = 0;
  int      errors = 0;
  string   fileState;

  srmInt   requestId;
  RequestStatus *prs;

  int retCode = -1;
  //----------------
  int   ind;
  class SrmWsClientConfig *cfg = new SrmWsClientConfig();

  // DEBUG: srmDebug is not set yet, for default config debugging only.
//   cout << "After Initialization:" << endl;
//   cfg->printConfig();  // Print initial configuration:

  //--------------------
  if ( cfg-> srmDebug )
    cout << "Parse Command Line Options - prescan " << endl;
  ind = cfg->parseCLOptions0( argc, argv );
  if ( cfg-> srmDebug )
    cfg->printConfig();  // Print updated configuration:

  //--------------------
  if ( cfg-> srmDebug )
    cout << "Read Configuration file ..." << endl;
  cfg->readConfig();
  if ( cfg-> srmDebug )
    cfg->printConfig();  // Print updated configuration:
  //--------------------

  if ( cfg-> srmDebug )
    cout << "Parse Command Line Options ... " << endl;

  ind = cfg->parseCLOptions( argc, argv );

// DEBUG:
//   if ( ind < argc ){
//     cout << endl;
//     cout << "More Arguments:" << endl;
//     for( int j = ind; j < argc; j++ )
//       cout << "\t" << argv[j] << endl;
//     cout << endl;
//   }

  //--------------------
  if ( cfg-> srmDebug ){
    cout << "Finally: " << endl;
    cfg->printConfig();  // Print updated configuration:
  }

  //----------------
  int narg = argc -ind;
  if ( narg < 2 || narg > 2 ) {
    if ( narg < 2  ) 
      cerr << "Too few arguments. Must be src and dst at least. Exiting. " << endl << endl;
    if ( narg > 2  ) 
      cerr << "Too many arguments. Exiting. " << endl << endl;
    cfg->usage( "srmput" );
    exit(1);
  }
  char ** argf;
  argf = argv + ind;

  char ** p = argf;
// DEBUG:
//   cout << "Files:" << endl;
//   for ( int j=0; j < narg; j++ ){
//     cout << "file " << *p++ << endl;
//   }

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
    cout << "Better try srmget, exiting" << endl;
    exit(1);
    //    break;
  case doPUT:
    fileURL = u1;
    srmURL  = u2;
    break;

  case doLOCAL:
    cout << "Will not copy file: to file:, exiting" << endl;
    exit(1);
  case doCOPY:
    cout << "Will not copy srm: to srm:, exiting" << endl;
    exit(1);
  default:
    cout << "srmput(), internal error - can't decide to get() or put() ... exiting" << endl;
    exit(1);
  }

//  DEBUG:
//   cout << "SrmURL=" << srmURL->getURL() << endl;
//   cout << "webServiceProtocol=" << cfg->webServiceProtocol << endl;
//   cout << "webServicePath="     << cfg->webServicePath     << endl;

  //----------------

  string srmHName = srmURL->host();
  string srmPort  = srmURL->port();
  string srmPath  = cfg->webServicePath;
  class SrmHostInfo srmHostInfo = SrmHostInfo( srmHName, srmPort, srmPath );

  string srmHost = srmHostInfo.getInfo();

  if ( cfg-> srmDebug )
    cout << "SRM Host Info =" << srmHost << endl;

  //---------------------------------
  // Init the gsoap runtime environment

  class SrmSoapWsClient *cs = new SrmSoapWsClient();

  if ( cfg->useGsiSsl ) {
    if ( cfg-> srmDebug )
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

  //------------
//   vector< string > vsSURLs;

//   vsSURLs.push_back( argf[0] );

//   //-------------

//   class VArrayOfstring surls( vsSURLs );

  vector<string> v_srcs(1);
  vector<string> v_dsts(1);
  vector<long> v_size(1);      // or   vector<LONG64> v_size(2);
  vector<bool> v_perm(1);

  v_srcs[0] = argf[0]; // fix me
  v_dsts[0] = argf[1]; // fix me

  v_size[0] = 0; // File size (for space reservation) - not impl. - fix me
  v_perm[0] = 0; // Make file permanent - not impl.

  //---
  class VArrayOfstring  srcs(  v_srcs );
  class VArrayOfstring  dsts(  v_dsts );
  class VArrayOflong    sizes( v_size );
  class VArrayOfboolean permanent( v_perm );
  //---

  if ( cfg-> srmDebug )
    cout << " call put() - wait for connection " << endl;

  ret = cs->put( &srcs, &dsts, &sizes, &permanent, &protocols, &prs );
  if ( cfg-> srmDebug ) {
    cout << " Ret code for put() = " << ret << endl;
  }

  if( ret ) {
    cerr << " put(): got error " << ret << ", exiting" << endl;
    exit(1);
  }
  if( prs == NULL ) {
    cerr << " srmput(): internal error - put() return OK, but pointer to ret.stat.==NULL, exiting" << endl;
    exit(1);
  }

  if ( cfg-> srmDebug ) {
    cout << "requestId = " << prs->requestId << endl;
    cout << "type      = " << prs->type      << endl;
    cout << "state     = " << prs->state     << endl;
    for(i=0 ; i < prs->fileStatuses->__size; i++ ) {
      cout << "  FILE # " << i << " ID fileId = " << prs->fileStatuses->__ptr[i].fileId << endl;
      cout << "  FILE # " << i << " ID size   = " << prs->fileStatuses->__ptr[i].size << endl;
      cout << "  FILE # " << i << " ID SURL   = " << prs->fileStatuses->__ptr[i].SURL << endl;
    }
    cout.flush();
  }

  requestId =  0;

  // fix me - prs already checked for nonzero 
  if ( prs
       && ( requestId = prs->requestId )  > 0
       && prs->state != "Failed" ) {

    all_done = 0;

    // Wait for SRM to get TURLs
    while( ! all_done ) {

      if ( cfg-> srmDebug ) {
	cout << " sleep " << delay << " sec ..." << endl;
	cout.flush();
      }
      sleep( delay );

      ret = cs->getRequestStatus( requestId, &prs );
      if( ret ) {
	cerr << " getRequestStatus(): got error " << ret << ", exiting" << endl;
	exit(1);
      }
      if( prs == NULL ) {
	cerr << " getRequestStatus(): internal error - getRequestStatus() return OK,"
	  " but pointer to ret.stat.==NULL, exiting" << endl;
	exit(1);
      }

      // Scan request is done
      all_done = 1;
      if ( prs->state != "Failed" ) { 
	for( i=0; i < prs->fileStatuses->__size; i++ ) {
	  fileState = prs->fileStatuses->__ptr[i].state;
	  if(  fileState == "Pending" ) 
	    all_done = 0;
	}
      }

      //=============
      if ( cfg-> srmDebug ) {
	cout << " Request ID for put() " << prs->requestId << endl;
	cout << " type      = " << prs->type      << endl;
	cout << " state     = " << prs->state     << endl;
	cout << " submit Time = "        << prs->submitTime << endl;
    
	for( i=0 ; i < prs->fileStatuses->__size; i++ ) {
	  cout << "  FILE # " << i << " ID fileId = " << prs->fileStatuses->__ptr[i].fileId << endl;
	  cout << "  FILE # " << i << " ID size   = " << prs->fileStatuses->__ptr[i].size << endl;
	  cout << "  FILE # " << i << " ID SURL   = " << prs->fileStatuses->__ptr[i].SURL << endl;
	  cout << "  FILE # " << i << " ID TURL   = " << prs->fileStatuses->__ptr[i].TURL << endl;
	  cout << "  FILE # " << i << " ID state  = [" << prs->fileStatuses->__ptr[i].state 
	       << "] " << endl;

	  fileState = prs->fileStatuses->__ptr[i].state;

	  if( fileState == "Ready" ) 
	    cout << "   - file done" << endl; 
	  else if( fileState == "Failed" ) 
	    cout << "   - file failed" << endl; 
	  else if( fileState == "Pending" )
	    cout << "   - pending, not done " << endl;
	  else
	    cout << "   not done yet" << endl; 
	}

	if ( all_done ) {
	  cout << "All done =" << all_done << endl;
	  cout.flush();
	}
      }
      //=============
    }

    // Scan if request done
    if (prs->state == "Failed" ) { 
      cout << "Request Failed" << endl;
      errors++;
    }else{
      for( i=0; i < prs->fileStatuses->__size; i++ ) {
	fileState = prs->fileStatuses->__ptr[i].state;
	if( fileState != "Ready" )
	  errors++;
	else {
	  // Ready for transfer:
	  if ( cfg-> srmDebug ) {
	    cout << "Transfer from file: " << argf[0] << endl; // fix it for now
	    cout << "           to TURL: " << prs->fileStatuses->__ptr[i].TURL << endl;
	  }
	}
      }
    }

  }else
    errors++;

  if ( !errors ) {
    string sTurl = prs->fileStatuses->__ptr[0].TURL;
    string sFile = argf[0];

    class url *dstURL = new class srmURL( sTurl ); // TURL
    class url *srcURL = new class srmURL( sFile ); // args

    if ( cfg->srmDebug ) {
      cout << "DBG: from=" << srcURL->getURL() << endl; cout.flush();
      cout << "DBG: to=  " << dstURL->getURL() << endl; cout.flush();
    }

    if ( cfg->urlCopyScript == "" ){
      cerr << "srmput(): urlCopyScript is not defined, exiting" << endl;
      exit(1);
    }
    
    const char *executable = cfg->urlCopyScript.c_str();

//   DEBUG:
//     const char *executable = "/bin/echo";

    char *exArgs[26];
    int rc;

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
    //---
    exArgs[ 0] = (char *)cfg->urlCopyScript.c_str();

//     cout << "DBG: orig ex=" << exArgs[0] << endl;
//     exArgs[ 0] = "/bin/echo";

    exArgs[ 1] = "-debug";
    exArgs[ 2] = (char *)(( cfg->srmDebug ) ? "true" : "false" );

    exArgs[ 3] = "-x509_user_proxy";
    exArgs[ 4] = (char *)cfg->x509_userProxy.c_str();

    exArgs[ 5] = "-x509_user_key";
    exArgs[ 6] = (char *)cfg->x509_userKey.c_str();

    exArgs[ 7] = "-x509_user_cert";
    exArgs[ 8] = (char *)cfg->x509_userCert.c_str();

    exArgs[ 9] = "-x509_user_certs_dir";
    exArgs[10] = (char *)cfg->x509_certDir.c_str();

    exArgs[11] = "-buffer_size";
    exArgs[12] = (char *)"2048";             // fix me - is fixed for now  - cfg->bufferSize;
    //--------
    exArgs[13] = "-src-protocol";
    exArgs[14] = (char *)(srcURL->scheme().c_str());

    exArgs[15] = "-src-host-port";
    exArgs[16] = (char *)srcHP.c_str();

    exArgs[17] = "-src-path";
    exArgs[18] = (char *)srcURL->urlPath().c_str();
    //--------
    exArgs[19] = "-dst-protocol";
    exArgs[20] = (char *)(dstURL->scheme().c_str());

    exArgs[21] = "-dst-host-port";
    exArgs[22] = (char *)dstHP.c_str();

    exArgs[23] = "-dst-path";
    exArgs[24] = (char *)dstURL->urlPath().c_str();
    //--------
    exArgs[25] = NULL;

    //===============
    pid_t pid;

    pid = fork ();
    if( pid < 0 ){
      perror("srmput():");
      exit(1);
    }
    if ( pid ) { // got child pid, this is a parent
      int stat;
      int rc;

      rc = wait( &stat );
      if( rc < 0) { /* now wait for the child */
	if(errno != ECHILD) 
	  cerr << "srmput(): An error occurred waiting for children" <<  endl;
      }else if ( rc > 0 ) {
	retCode = stat;	
	if ( cfg-> srmDebug ) {
	  cout << "srmput() Parent: Child process done, status =" << stat << endl;
	  cout.flush();
	}
      }else{
	retCode = 0;
	if ( cfg-> srmDebug ) {
	  cout << "srmput() Parent: Child process done" << endl;
	  cout.flush();
	}
      }

    }else{ //<---- Child process

      if ( cfg->srmDebug ) {
	cout << "Child: Started ... " << endl;
	cout.flush();
      }

      rc = execvp ( executable,  exArgs );

      // Will return only IF error
      if ( rc ) { 
	perror( "srmput()" );
	cerr << "srmput(): can not execvp() transfer process, exiting" << endl;
	exit(1);
      }
    } //<--- End of Child process

  }

  if ( cfg-> srmDebug ) 
    cout << " put() done "<< endl;
  
  delete cfg;

  return (errors) ? 1:retCode;
}

