#include "srm.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#ifdef __cplusplus
extern "C" {
#endif
  
  static char *USAGE = "USAGE:\n"
  "srmget [-g] [-p] [-d <delay>]\n"
  " -g -- get files\n"
  " -p -- put files\n"
  " -d <delay> -- set delay for check status\n";


int main(int argc, char** argv)
{
  int      i;
  srm_int  requestId;
  int      all_done = 0;
  srm_array_of_strings surls     = new_srm_array_of_strings(2);
  srm_array_of_strings protocols = new_srm_array_of_strings(1);
  RequestStatus rs;

  struct _srm_host_info _srm_host = {"stkendca3a.fnal.gov", "24128", "srm/managerv1" };
  srm_host_info srm_host =  & _srm_host; 

  int callGet = 0;
  int callPut = 0;
  int delay   = 5;

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

	//      free (somestring);
	//	somestring = strdup (optarg);

      case '?':
      default:
	fprintf (stderr, "%s", USAGE);
	return (1);
      }
  }

// port 25129 - cdfendca3
// port 24128 - stkendca
// port 24129 - stkendca, ssl
// port 25129 - stkendca, gsiftp

  set_element_srm_array_of_string( surls,0,  
	 (srm_string)("srm://stkendca3a.fnal.gov:25129/"
		      "/pnfs/fnal.gov/usr/test/NeST/real/srmtest/aik/ts01.file1"));
  set_element_srm_array_of_string( surls,1,  
	 (srm_string)("srm://stkendca3a.fnal.gov:25129/"
		      "/pnfs/fnal.gov/usr/test/NeST/real/srmtest/aik/ts01.file6"));
  set_element_srm_array_of_string( protocols, 0,  (srm_string)(&"gsiftp"));

  //------------------------
  if( ! callGet && ! callPut )
    printf("Nothing to do ...\n" );

if( callGet ){
  printf("## call get()\n");

  rs = get( surls, protocols, srm_host );
  if(rs == NULL) {
    printf(" get(): received NULL Request Status\n");
    return 1;
  }

  printf("requestId = %d\n",rs->requestId );
  for(i=0 ; i < rs->fileStatuses->length; i++ ) {
    printf("  FILE # %d ID IS    = %d\n",   i, rs->fileStatuses->array[i]->fileId);
    printf("  FILE # %d ID size  = %lld\n", i, rs->fileStatuses->array[i]->size);
    printf("  FILE # %d ID SURL  = %s\n",   i, rs->fileStatuses->array[i]->SURL);
  }
  requestId =  rs->requestId;

  all_done = 0;
  while( ! all_done) {
    free_RequestStatus(rs);
    all_done = 1;
    printf(" sleep %d sec ...\n", delay);
    fflush(stdout);
    sleep( delay );

    rs = getRequestStatus( requestId, srm_host );
    if(rs == NULL) {
      printf(" get(): received NULL Request Status\n");
      return 1;
    }

    printf("requestId = %d\n",  rs->requestId );
    printf("submitTime = %s\n", rs->submitTime );
    for(i=0 ; i < rs->fileStatuses->length; i++ ) {
      printf("  FILE # %d ID IS    = %d\n",   i, rs->fileStatuses->array[i]->fileId);
      printf("  FILE # %d ID size  = %lld\n", i, rs->fileStatuses->array[i]->size);
      printf("  FILE # %d ID SURL  = %s\n",   i, rs->fileStatuses->array[i]->SURL);
      printf("  FILE # %d ID TURL  = %s\n",   i, rs->fileStatuses->array[i]->TURL);
      printf("  FILE # %d ID state = %s\n",   i, rs->fileStatuses->array[i]->state);
      if( rs->fileStatuses->array[i]->TURL == NULL) {
        printf("   not done yet\n");
        all_done = 0;
      }else
	printf("   -- done\n");
    }
  }

  free_RequestStatus( rs );
  printf("## get() done\n\n");
}
  //---------------------

  if( callPut ) {
    printf("## call put()\n");

    {  
    srm_long 	size[2] 	= {615879L,615879L};
    srm_boolean dontwant[2] 	= {1,1};

    srm_array_of_strings sources   = new_srm_array_of_strings(2);
    srm_array_of_strings dests     = new_srm_array_of_strings(2);
    srm_array_of_longs sizes       = new_srm_array_of_longs  (2,size);
    srm_array_of_booleans wantPerm = new_srm_array_of_booleans(2,dontwant);


    // sources:: "file:///thishost/fullpath/file.ext"
    set_element_srm_array_of_string( sources, 0,  
				     (srm_string)("file:///home/aik/srm/c_cpp_client/file1.dat"));
    set_element_srm_array_of_string( sources, 1,  
				     (srm_string)("file:///home/aik/srm/c_cpp_client/file2.dat"));

    // destinations - SURL:: "srm:///targethost/fullpath/file.ext"
    set_element_srm_array_of_string( dests,   0,  
				     (srm_string)("/pnfs/fnal.gov/usr/test/NeST/real/srmtest/aik/ts02.file1.dat"));
    set_element_srm_array_of_string( dests,   1,  
				     (srm_string)("/pnfs/fnal.gov/usr/test/NeST/real/srmtest/aik/ts02.file2.dat"));

    rs = put( sources, dests, sizes, wantPerm, protocols, srm_host );

    do{

      if(rs == NULL) {
	printf(" put(): received NULL Request Status\n");
	return 1;
      }

      requestId =  rs->requestId;
      printf("requestId = %d\n", rs->requestId);

      all_done = 1;
      for ( i=0; i < rs->fileStatuses->length; i++ ) {
	printf("  FILE # %d ID IS    = %d\n",   i, rs->fileStatuses->array[i]->fileId);
	printf("  FILE # %d ID size  = %lld\n", i, rs->fileStatuses->array[i]->size);
	printf("  FILE # %d ID SURL  = %s\n",   i, rs->fileStatuses->array[i]->SURL);
	printf("  FILE # %d ID TURL  = %s\n",   i, rs->fileStatuses->array[i]->TURL);
	printf("  FILE # %d ID state = %s\n",   i, rs->fileStatuses->array[i]->state);
	if( rs->fileStatuses->array[i]->TURL == NULL) {
	  printf("   not done yet\n"); 
	  all_done = 0; 
	}else
	  printf("   - done\n"); 
      }

      free_RequestStatus(rs);

      if( ! all_done ){
	printf(" sleep %d sec ...\n", delay);
	fflush(stdout);
	sleep(delay);

	rs = getRequestStatus( requestId, srm_host );
      }
    }while( ! all_done );

  }

  free_RequestStatus(rs);

}
  return 0;
}

#ifdef __cplusplus
} /* extern "C" */
#endif  /* __cplusplus */
