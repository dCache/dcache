#include <stdio.h>
char * crypt( char * in , char * cc ) ;
int main( int argc , char * argv[] ){
   char * result ;
   if( argc < 3 ){
      printf( "USAGE : ... <salt> <string>\n" ) ;
      exit(4) ; 
   }
   
   result = crypt( argv[2] , argv[1] ) ;
   
   printf( "result : %s\n" , result ) ;

}
