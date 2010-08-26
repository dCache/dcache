#include "srm.h"
#include <stdio.h>
#include <unistd.h>

#ifdef __cplusplus
extern "C" {
#endif

int main(int argc, char** argv)
{
  int i;
  srm_int requestId;
  int all_done = 0;
  srm_array_of_strings surls = new_srm_array_of_strings(2);
  srm_array_of_strings protocols = new_srm_array_of_strings(1);
  RequestStatus rs;
  set_element_srm_array_of_string( surls,0,  (srm_string)("srm://fnisd1.fnal.gov:24129//pnfs/fnal.gov/usr/cdfen/jon/timur/README5"));
  set_element_srm_array_of_string( surls,1,  (srm_string)("srm://fnisd1.fnal.gov:24129//pnfs/fnal.gov/usr/cdfen/jon/timur/file2.txt"));
  set_element_srm_array_of_string( protocols,0,  (srm_string)(&"http"));
  rs = get( surls, protocols,NULL);
  if(rs == NULL)
  {
    printf(" received NULL:\n");
    return 1;
  }
  printf("requestId = %d\n",rs->  requestId);
  for(i=0 ; i<rs->fileStatuses->length;++i)
  {
    printf("  FILE # %d ID IS = %d\n",i,rs->fileStatuses->array[i]->fileId);
    printf("  FILE # %d ID size = %lld\n",i,rs->fileStatuses->array[i]->size);
    printf("  FILE # %d ID SURL = %s\n",i,rs->fileStatuses->array[i]->SURL);
  }
  requestId =  rs->  requestId;

  while(!all_done)
  {
    free_RequestStatus(rs);
    all_done = 1;
    sleep(2);
    rs = getRequestStatus( requestId,NULL );
    if(rs == NULL)
    {
      printf(" received NULL:\n");
      return 1;
    }
    printf("requestId = %d\n",rs->  requestId);
    printf("submitTime = %d\n",rs->  submitTime);
    for(i=0 ; i<rs->fileStatuses->length;++i)
    {
      printf("  FILE # %d ID IS = %d\n",i,rs->fileStatuses->array[i]->fileId);
      printf("  FILE # %d ID size = %lld\n",i,rs->fileStatuses->array[i]->size);
      printf("  FILE # %d ID SURL = %s\n",i,rs->fileStatuses->array[i]->SURL);
      printf("  FILE # %d ID TURL = %s\n",i,rs->fileStatuses->array[i]->TURL);
      printf("  FILE # %d ID state = %s\n",i,rs->fileStatuses->array[i]->state);
      if( rs->fileStatuses->array[i]->TURL == NULL)
      {
        printf("   not done yet\n");
        all_done = 0;
      }
    }
  }

  free_RequestStatus(rs);

  printf("now call put\n");

  {
    srm_long size[3] = {124l,1024l,10240000l};
    srm_boolean dontwant[3] = {0,1,1};
    srm_array_of_strings sources = new_srm_array_of_strings(1);
		srm_array_of_strings dests = new_srm_array_of_strings(1);
	  srm_array_of_longs sizes =  new_srm_array_of_longs(3,size);
		srm_array_of_booleans wantPerm =new_srm_array_of_booleans(3,dontwant);
    set_element_srm_array_of_string( sources,0,  (srm_string)("./file1.txt"));
    set_element_srm_array_of_string( dests,0,  (srm_string)("pnfs/fnal.gov/usr/test/timur/file1.txt"));
    rs = put( sources,
			      dests,
			      sizes,
			      wantPerm,
			      protocols,
            NULL );
     if(rs == NULL)
     {
       printf(" received NULL:\n");
       return 1;
     }
     printf("requestId = %d\n",rs->  requestId);
     for(i=0 ; i<rs->fileStatuses->length;++i)
    {
      printf("  FILE # %d ID IS = %d\n",i,rs->fileStatuses->array[i]->fileId);
      printf("  FILE # %d ID size = %lld\n",i,rs->fileStatuses->array[i]->size);
      printf("  FILE # %d ID SURL = %s\n",i,rs->fileStatuses->array[i]->SURL);
      printf("  FILE # %d ID TURL = %s\n",i,rs->fileStatuses->array[i]->TURL);
      printf("  FILE # %d ID state = %s\n",i,rs->fileStatuses->array[i]->state);
      if( rs->fileStatuses->array[i]->TURL == NULL)
      {
        printf("   not done yet\n");
        all_done = 0;
      }
    }

  }
  free_RequestStatus(rs);
  return 0;

}

#ifdef __cplusplus
} /* extern "C" */
#endif  /* __cplusplus */
