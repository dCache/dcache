// Test functionality of SrmSoapWsClient.cpp 
//
// Alex Kulyavtsev, FNAL
//

#include "config.h"
#include "srmWSH.h"
#include "stdio.h"
#include <sys/time.h>

#include <SrmSoapWsClient.hpp>

#include <vector>
#include <iterator>
#include <string>

// some include files in gsi.h conflict with #include <string>, so it must go after it
//   and "SrmSoapWsClient.hpp":
#include "gsi.h"

//---------------
using namespace srm;

static char *USAGE = "USAGE:\n"
  "srmget [-g] [-p] [-d <delay>]\n"
  " -g -- get files\n"
  " -p -- put files\n"
  " -d <delay> -- set delay for check status\n";

//--------------------------
// #include "srmWSStub.h"
//--------------------------

int
main (int argc, char **argv)
{
  //------------------
  // srmcp client data:

  int      i;
  int      ret;
  int      all_done = 0;

  srmInt   requestId;
  RequestStatus *prs;

  string srmProtocol = "http";
  string srmHName( "stkendca3a.fnal.gov" );
  string srmPort( "24129" );
  string srmPath( "srm/managerv1" );
  class SrmHostInfo srmHostInfo = SrmHostInfo( srmProtocol, srmHName, srmPort, srmPath );

  string srmHost = srmHostInfo.getInfo();
  cout << "SRM Host Info =" << srmHost << endl;

  //---------------
  int callGet = 0;
  int callPut = 0;
  int delay   = 2;

  //---------------
  // Parse options
  int c;

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

  //-------------
  vector< string > v_protocols;                 // create emty vector of strings
  v_protocols.push_back( string("gsiftp") );    //   add element to it

  class VArrayOfstring protocols( v_protocols );

if( callGet ){

  //------------
  string file0 = "srm://stkendca3a.fnal.gov:25129/"
    "/pnfs/fnal.gov/usr/test/NeST/real/srmtest/aik/ts01.file1";
  string file1 = "srm://stkendca3a.fnal.gov:25129/"
    "/pnfs/fnal.gov/usr/test/NeST/real/srmtest/aik/ts01.file6";

  vector< string > vsSURLs;

  vsSURLs.push_back( file0 );
  vsSURLs.push_back( file1 );
  //-------------

  class VArrayOfstring surls( vsSURLs );

  cout << "## call get() - wait for connection " << endl;

  ret = cs->get( &surls, &protocols, &prs );
  if( ret ) {
    cerr << " get(): got error " << ret << ", exiting" << endl;
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
      printf(" getRequestStatus(): got error %d, exiting\n", ret );
      exit(1);
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
    cout << "## test put() ###" << endl;
    cout.flush();

    vector<string> v_srcs(2);
    vector<string> v_dsts(2);
    vector<long> v_size(2);      // or   vector<LONG64> v_size(2);
    vector<bool> v_perm(2);

    // sources:: "file:///thishost/fullpath/file.ext"
    v_srcs[0] = "file:///home/aik/srm/cppClientDevel/run/file1.dat";
    v_srcs[1] = "file:///home/aik/srm/cppClientDevel/run/file2.dat";

    // destinations - SURL:: "srm:///targethost/fullpath/file.ext"
    v_dsts[0]="/pnfs/fnal.gov/usr/test/NeST/real/srmtest/aik/ts02.file1.dat";
    v_dsts[1]="/pnfs/fnal.gov/usr/test/NeST/real/srmtest/aik/ts02.file2.dat";

    // size of the file (space to reserve for transfer)
    v_size[0] = 615879L;
    v_size[1] = 615879L;

    // want to make file permanent ?
    v_perm[0] = 1;
    v_perm[1] = 1;

    //
    class VArrayOfstring  srcs(  v_srcs );
    class VArrayOfstring  dsts(  v_dsts );
    class VArrayOflong    sizes( v_size );
    class VArrayOfboolean permanent( v_perm );

    cout << " call put()" << endl;
    cout.flush();

    ret = cs->put( &srcs, &dsts, &sizes, &permanent, &protocols, &prs );
    if( ret ) {
      cerr << " put(): got error " << ret << ", exiting" << endl;
      exit(1);
    }
    requestId =  prs->requestId;

    cout << " Request ID for put() " << prs->requestId << endl;
    cout.flush();

    do{
      all_done = 1;
      for ( i=0; i < prs->fileStatuses->__size; i++ ) {
	cout << "  FILE # " << i << " ID fileId = " << prs->fileStatuses->__ptr[i].fileId << endl;
	cout << "  FILE # " << i << " ID size   = " << prs->fileStatuses->__ptr[i].size << endl;
	cout << "  FILE # " << i << " ID SURL   = " << prs->fileStatuses->__ptr[i].SURL << endl;
	cout << "  FILE # " << i << " ID TURL   = " << prs->fileStatuses->__ptr[i].TURL << endl;
	cout << "  FILE # " << i << " ID state  = " << prs->fileStatuses->__ptr[i].state << endl;
	if( prs->fileStatuses->__ptr[i].TURL == NULL) {
	  cout << "   not done yet" << endl; 
	  all_done = 0; 
	}else
	  cout << "   - done" << endl; 
      }

      if( ! all_done ){
	cout << " sleep " << delay << " sec ..." << endl;
	cout.flush();
	sleep(delay);

	cout << " call getRequestStatus() for RequestId " << requestId << endl;
	cout.flush();

	ret = cs->getRequestStatus( requestId, &prs );
	if( ret ) {
	  cerr << " getRequestStatus(): got error " << ret << ", exiting" << endl;
	  exit(1);
	}
      }
    }while( ! all_done );

  } // end '-p' -- put() option


  return 0;
}

