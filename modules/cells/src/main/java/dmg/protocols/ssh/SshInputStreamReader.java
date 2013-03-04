package dmg.protocols.ssh ;

import java.io.FilterReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;

public class SshInputStreamReader extends FilterReader {

  private boolean               _eof;
  private boolean               _echo = true ;
  private char                  _echoChar = (char)0 ;
  private boolean               _tempEchoOff;
  private SshOutputStreamWriter _output ;

  public SshInputStreamReader( InputStream input ){
     super( new InputStreamReader( input ) ) ;
     _output  = null ;
  }
  public SshInputStreamReader( InputStream input , OutputStream output ){
     super( new InputStreamReader( input ) ) ;
     _output = new SshOutputStreamWriter( output ) ;
  }

  public void setEcho( boolean e ){ _echo = e ; }
  public void setEchoChar( char c ){ _echoChar = c ; }
  public int readx( char [] cbuf , int off , int len )
         throws IOException {

    System.out.println( "Requesting "+len );
    if( _eof ) {
        return -1;
    }
    int rc = super.read( cbuf , off , len ) ;
    System.out.println( "rc : "+rc ) ;
//    for( int i = 0 ; i < rc ; i++ )
//        System.out.print( ""+cbuf[i]+"("+((int)cbuf[i])+") " ) ;
//    System.out.println("");

    if( rc <= 0 ) {
        return rc;
    }
    for( int i = off ; i < (off+rc) ; i++ ){
       int ccc = cbuf[i] ;
       System.out.println( "xx = "+ccc ) ;
       if( cbuf[i] == 13 ){
          cbuf[i] = '\n' ;
          _tempEchoOff = false ;
       }else if( cbuf[i] == 4 ){
          _eof = true ;
          rc = i - off - 1 ;
          break ;
       }else if( cbuf[i] == 9 ){
          _tempEchoOff = true ;
       }

       if( _output != null ){
          if( (cbuf[i] == '\n') || ( _echo && ! _tempEchoOff ) ) {
              _output.write(cbuf[i]);
          } else if( _echoChar != (char)0 ) {
              _output.write(_echoChar);
          }
          _output.flush();
       }
    }
    return rc ;
  }
  private final static char  CONTROL_H  =  (char)8 ;
  @Override
  public int read( char [] cbuf , int off , int len )
         throws IOException {

    if( _eof ) {
        return -1;
    }
//    System.out.println( "Requesting "+len );
    int i;
    for( i = off ; i < (off+1) ;  ){
       int n = super.read( cbuf , i , 1 ) ;
       if( n < 0 ){ _eof = true ; return -1 ; }
//       System.out.println( "got "+((int)cbuf[i])) ;
       if( cbuf[i] == 13 ){
          cbuf[i] = '\n' ;
          push( cbuf[i] ) ;
          _tempEchoOff = false ;
          i++ ;
          break ;
       }else if( cbuf[i] == 4 ){
          _eof = true ;
          break ;
       }else if( ( cbuf[i] == CONTROL_H ) ||
                 ( cbuf[i] == 127       )    ){
          cbuf[i] = CONTROL_H ;
          push( CONTROL_H ) ;
          push(' ') ;
          push( CONTROL_H ) ;
          i++ ;
       }else if( cbuf[i] == 9 ){
          _tempEchoOff = ! _tempEchoOff ;
       }else{
          push( cbuf[i] ) ;
          i++ ;
       }

    }
//    System.out.println("returned "+(i-off) ) ;
    return i-off ;
  }
  public void push( char c ) throws IOException {
       if( _output != null ){
          if( ( c == '\n') ||
              ( _echo && ! _tempEchoOff ) ) {
              _output.write(c);
          } else if( _echoChar != (char)0 ) {
              _output.write(_echoChar);
          }
          _output.flush();
       }
  }

}
