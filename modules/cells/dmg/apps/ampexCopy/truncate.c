#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>

main( int argc , char * argv[] ){
  int newSize = 0 ;
  int size = 0 ;
  int rc = 0 ;
  char * filename ;
  struct stat info ;
  if( argc < 3 ){
    fprintf(stderr,"Usage : %s <filename> <length>\n",argv[0]);
    exit(4);
  }
  filename = argv[1] ;
  sscanf( argv[2] , "%d" , &newSize ) ;

  rc = lstat( filename , &info ) ;
  if( rc < 0 ){
    fprintf( stderr , "Can't stat '%s' -> %d\n",filename,rc) ;
    exit(5);
  }  
  size = info.st_size ;
  if( size < newSize ){
    fprintf(stderr,"Panic : file (%s) too small %d <-> %d\n" ,filename,size,newSize) ;
    exit(6) ;
  }
  printf( "File %s cutting from %d to %d\n",filename,size,newSize) ;
  rc = truncate( filename , newSize ) ;
  if( rc < 0 ){
    fprintf( stderr , "Can't truncate '%s' to %d -> %d\n",filename,newSize,rc) ;
    exit(7);
  }  
  exit(0);

}
