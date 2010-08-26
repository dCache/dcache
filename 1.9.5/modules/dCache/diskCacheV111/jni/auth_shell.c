#include <stdio.h>

#define IDLE    (0) 
#define COMMAND (1)
#define DUMMY   (2)
#define MAX_LINE (1024)
#define MAX_POS  (32)

static void execute( unsigned char * array[]  , int pos ) ;

int main( int argc , char * argv [] ){

   int pos = 0 ;
   int i ;
   int in ;
   int n ;
   unsigned char c ;
   int state = IDLE ;
   unsigned char * array[MAX_POS] ;
   unsigned char * line ;
   
   while( ( in = fgetc(stdin) ) != EOF ){
       c = (unsigned char) in ;
       switch( state ){
       
          case IDLE :
             if( c == ' ' ){
             
             }else if( c == '\n' ){
                if( pos > 0 ){
                    execute( array , pos ) ;
                    for( i = 0 ; i < pos ; i++ )free( (void *)array[i] ) ;
                }
                pos = 0 ;
             }else{
                n = 0 ;
                line = (unsigned char *)malloc(MAX_LINE) ;
                line[n++] = (unsigned char) c ;
                state = COMMAND ;
             }
          break ;
          
          case DUMMY :
             if( c == '\n' ){
                if( pos > 0 ){
                    execute( array , pos ) ;
                    for( i = 0 ; i < pos ; i++ )free( (void *)array[i] ) ;
                }
                pos = 0 ;
                state = IDLE ;
             }          
          break ;
          case COMMAND :
             if( c == ' ' ){
             
                line[n++] = '\0' ;
                array[pos++] = line ;
                if( pos >= ( MAX_POS -1 ) ){
                   state = DUMMY ;
                }else{
                   state = IDLE ;
                }
                
             }else if( c == '\n' ){
             
                line[n++] = '\0' ;
                array[pos++] = line ;
                state = IDLE ;
                if( pos > 0 ){
                    execute( array , pos ) ;
                    for( i = 0 ; i < pos ; i++ )free( (void *)array[i] ) ;
                }
                pos = 0 ;
                
             }else{
                line[n++] = (unsigned char) c ;
                if( n >= ( MAX_LINE - 1 ) ){
                   line[n] = '\0' ;
                   array[pos++] = line ;
                   state = DUMMY ;
                }
             }
          break ;
       
       }
   
   }


}
#ifdef DUMMYX
static void execute( unsigned char * array[] , int pos ){
   int i = 0 ;
   for( i = 0 ; i < pos ; i++ ){
      printf(" %d %s\n",i,array[i]);
   }
}
#else
static void execute( unsigned char * array[] , int pos ){

   int i ;
   if( ( pos < 4 ) || strcmp(array[0],"check" ) ){
   
      fprintf(stdout,"error\n") ;
   
   }else{
   
      if( checkUser( array[1] , array[2] , array[3] ) ){
         fprintf(stdout,"true\n") ;
      }else{
         fprintf(stdout,"false\n");
      }
   }
   
   fflush(stdout) ;
}
#endif
