// srm-gsi-client2.cpp::
// derived from submit-gsi-client.c as example.
// Alex Kulyavtsev, FNAL
//

#include "config.h"
#include "gsi.h"
#include "srmWSH.h"
#include "stdio.h"
#include <sys/time.h>

#include <SrmSoapWsClient.hpp>

#include <vector>
#include <iterator>
#include <string>

//---------------
using namespace srm;

static char *USAGE = "USAGE:\n"
  "srmget [-g] [-p] [-d <delay>]\n"
  " -g -- get files\n"
  " -p -- put files\n"
  " -d <delay> -- set delay for check status\n";


int
main (int argc, char **argv)
{
  //------------------
  // srmcp client data:

  int      i;
  int      ret;
  int      all_done = 0;

//   // gSOAP data:
//   struct soap *soap;

  srmInt   requestId;
  RequestStatus *prs;

  //  string srmHost("http://stkendca3a.fnal.gov:24128/srm/managerv1");

  //  string srmHost("http://stkendca3a.fnal.gov:24129/srm/managerv1");

  string srmHName( "stkendca3a.fnal.gov" );
  string srmPort( "24129" );
  string srmPath( "srm/managerv1" );
  class SrmHostInfo srmHostInfo = SrmHostInfo( srmHName, srmPort, srmPath );

  string srmHost = srmHostInfo.getInfo();
  cout << "SRM Host Info =" << srmHost << endl;

  int callGet = 1;
  int callPut = 0;
  int delay   = 2;

  int c;

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
      case '?':
      default:
	fprintf (stderr, "%s", USAGE);
	return (1);
      }
  }

  //---------------------------------
  // Init the gsoap runtime environment

//   soap = soap_new ();
//   class SrmSoapWsClient *cs = new SrmSoapWsClient( soap );

  class SrmSoapWsClient *cs = new SrmSoapWsClient();

  if ( cs->setupGSI() ) {
     cerr << "Can not register GSI plugin, exiting" << endl;
     exit (1);
   }

  if ( cs->setSrmURL( srmHost ) ) {
     cerr << "Can not set SRM URL, exiting" << endl;
     exit (1);
  }
  //==============================================
  if( ! callGet && ! callPut )
    printf( "No get(), no put() - nothing to do ...\n" );


if( callGet ){

  string file0 = "srm://stkendca3a.fnal.gov:25129/"
    "/pnfs/fnal.gov/usr/test/NeST/real/srmtest/aik/ts01.file1";
  string file1 = "srm://stkendca3a.fnal.gov:25129/"
    "/pnfs/fnal.gov/usr/test/NeST/real/srmtest/aik/ts01.file6";

  vector< string > vsSURLs;
  vector< string > vsProtocols;

  vsSURLs.push_back( file0 );
  vsSURLs.push_back( file1 );

  vsProtocols.push_back( string("gsiftp") );

  
  class ArrayOfstring surls;
  class ArrayOfstring protocols;

  const char * a_sURL[2];
  a_sURL[0] = (vsSURLs[0]).c_str();
  a_sURL[1] = (vsSURLs[1]).c_str();
  surls.__ptr    = (char **) a_sURL;
  surls.__size   = 2;
  surls.__offset = 0;

  const char * a_prot[1];
  a_prot[0] = (vsProtocols[0]).c_str();
  protocols.__ptr    = (char **) a_prot;
  protocols.__size   = 1;
  protocols.__offset = 0;
  
  cout << "## call get() - wait for connection " << endl;

  ret = cs->get( &surls, &protocols, &prs );
  if( ret ) {
    printf(" get(): got error %d, exiting\n", ret );
    return 1;
  }

  printf("requestId = %d\n",prs->requestId );
  printf("type      = %s\n",prs->type );
  printf("state     = %s\n",prs->state );
  for(i=0 ; i < prs->fileStatuses->__size; i++ ) {
    printf("  FILE # %d ID fileId = %d\n",   i,  prs->fileStatuses->__ptr[i].fileId);
    printf("  FILE # %d ID size   = %lld\n", i,  prs->fileStatuses->__ptr[i].size); // inhereted from FileMetadata
    printf("  FILE # %d ID SURL   = %s\n",   i,  prs->fileStatuses->__ptr[i].SURL); // inhereted from FileMetadata
  }

  requestId =  prs->requestId;

  all_done = 0;
  while( ! all_done) {

    all_done = 1;
    printf(" sleep %d sec ...\n", delay);

    fflush(stdout);
    sleep( delay );

    ret = cs->getRequestStatus( requestId, &prs );
    if( ret ) {
      printf(" getRequestStatus: got error %d, exiting\n", ret );
      return 1;
    }

    printf("requestId = %d\n",  prs->requestId );
    printf("submitTime = %s\n", prs->submitTime );
    for(i=0 ; i < prs->fileStatuses->__size; i++ ) {
      printf("  FILE # %d ID fileId = %d\n",   i, prs->fileStatuses->__ptr[i].fileId);
      printf("  FILE # %d ID size   = %lld\n", i, prs->fileStatuses->__ptr[i].size);   // inhereted from FileMetadata
      printf("  FILE # %d ID SURL   = %s\n",   i, prs->fileStatuses->__ptr[i].SURL);   // inhereted from FileMetadata
      printf("  FILE # %d ID TURL   = %s\n",   i, prs->fileStatuses->__ptr[i].TURL);
      printf("  FILE # %d ID state  = %s\n",   i, prs->fileStatuses->__ptr[i].state);
      if( prs->fileStatuses->__ptr[i].TURL == NULL) {
        printf("   not done yet\n");
        all_done = 0;
      }else
	printf("   -- done\n");
    }
  }

  printf("## get() done\n\n");

} // end '-g' -- call get()

  //---------------------

  if( callPut ) {
    printf("## call put() - come later !!!\n");
  } // end '-p' -- put() option


  return 0;
}

