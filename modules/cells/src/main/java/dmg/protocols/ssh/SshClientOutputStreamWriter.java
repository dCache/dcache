package dmg.protocols.ssh ;

import java.io.FilterWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

public class SshClientOutputStreamWriter extends FilterWriter {

   OutputStream _outputStream;
   public SshClientOutputStreamWriter( Writer output ){
     super( output ) ;
   }
   public SshClientOutputStreamWriter( OutputStream output ){
     super( new OutputStreamWriter( output ) ) ;
 //    System.out.println( "OutputStreamReader created" ) ;
   }
   @Override
   public void write( char [] c )
      throws IOException  {
      printout( "SshOutputStreamWriter : write( char [] c )" ) ;
      write( c , 0 , c.length ) ;
   }
   @Override
   public void write( char [] c , int off , int len )
      throws IOException  {
     printout( "SshOutputStreamWriter : write( char [] c , int off , int "+len+" )" ) ;
     for( int i = off ; i < (off+len) ; i++ ) {
         write((int) c[i]);
     }
   }
   @Override
   public void write( int c )
      throws IOException  {
       printout( "SshOutputStreamWriter : write( int "+c+" )" ) ;
       if( c == '\n' ){   out.write( 0xa ) ; }
       else {
           out.write(c);
       }
   }
   @Override
   public void write( String str ) throws IOException {
       printout( "SshOutputStreamWriter : write( String "+str+" )" ) ;
       for( int i= 0 ; i < str.length() ; i++ ) {
           write(str.charAt(i));
       }
   }
   @Override
   public void write( String str , int off , int len )throws IOException{
       printout( "SshOutputStreamWriter : write( String str , int off , int len )" ) ;
       for( int i = off ; i < (off+len) ; i++ ) {
           write(str.charAt(i));
       }
   }
   void printout( String str ){
//     System.out.println( str ) ;
   }


}
