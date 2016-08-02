 package dmg.protocols.telnet ;

 import java.io.IOException;
 import java.io.InputStream;

 /**
   * The Telnet input stream builds the layer between
   * a telnet input stream and the application layer.
   * If it is used as Filter for higher level Streams,
   * is simply extracts the telnet controls out of the stream.
   * Otherwise the 'getNext' method returns a byte array which
   * represents the control characters and an CharacterObject
   * otherwise.
  *
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  *
   */
 public class TelnetInputStream extends InputStream {
     //
     //   the telnet constants
     //
     private static final byte telnetSE   =   (-16);
     private static final byte telnetNOP  =   (-15);
     private static final byte telnetDM   =   (-14);
     private static final byte telnetBRK  =   (-13);
     private static final byte telnetIP   =   (-12);
     private static final byte telnetAO   =   (-11);
     private static final byte telnetAYT  =   (-10);
     private static final byte telnetEC   =   (-9);
     private static final byte telnetEL   =   (-8);
     private static final byte telnetGA   =   (-7);
     private static final byte telnetSB   =   (-6);
     private static final byte telnetWILL =   (-5);
     private static final byte telnetWONT =   (-4);
     private static final byte telnetDO   =   (-3);
     private static final byte telnetDONT =   (-2);
     private static final byte telnetIAC  =   (-1);
     private static final byte telnetCR   =   (0xd);
     private static final byte telnetLF   =   (0xa);
     private static final byte telnetNUL  =   (0);

     private static final byte telnetOptionEcho  = (1);

     private static final int telnetStateControl     =  0 ;
     private static final int telnetStateIntro       =  1 ;
     private static final int telnetStateAuthUser    =  2 ;
     private static final int telnetStateAuthPasswd  =  3 ;
     private static final int telnetStateMOTD        =  4 ;
     private static final int telnetStateData        =  5 ;
     private static final int telnetStateNoAccess    =  6 ;

     private static final int cctData = 1 ;
     private static final int cctCR   = 2 ;
     private static final int cctCR2  = 3 ;
     private static final int cctCT1  = 4 ;
     private static final int cctCT2  = 5 ;
     private static final int cctSUB  = 6 ;
     private static final int cctESC  = 7 ;
    //
    // class variables
    //
    int         _engineState ;
    int         _controlDataPos ;
    byte []     _controlData ;
    InputStream _inputStream ;

   public TelnetInputStream( InputStream in ){
//       super( in )  ;
       _inputStream    = in ;
       _engineState    = 0 ;
       _controlData    = null ;
       _controlDataPos = 0 ;
   }
   private void _engineControlAdd( byte c ){
      //
      if( _controlDataPos >= _controlData.length ){
      //
      //  somethin' wrong with telnet engine
      //
           _controlDataPos = 0 ;
      }
      _controlData[_controlDataPos++] = c ;
   }
   private void _engineControlClear(){
       if( _controlData == null ) {
           _controlData = new byte[32];
       }
       _controlDataPos = 0 ;
   }
   private byte [] _engineControlGet(){
      if( _controlDataPos == 0 ) {
          return null;
      }

      byte [] rc = new byte[ _controlDataPos ] ;

      System.arraycopy( _controlData , 0 , rc , 0 , _controlDataPos ) ;

      _engineControlClear() ;
      return rc ;
   }
   public Object readNext() throws IOException {
      int    rc ;
      Object obj ;
      while( true ){

         if( ( rc = _inputStream.read() ) < 0 ) {
             return null;
         }
         if( ( obj = _next( (byte)rc ) ) != null ) {
             return obj;
         }
      }

   }
   @Override
   public int read() throws IOException {
      Object obj ;
      while(true){
         obj = this.readNext() ;
         if(  obj == null ){
            return -1 ;
         }else if( obj instanceof Byte ){
            return ((Byte)obj).intValue() ;
         }else{
            System.out.println("Got telnet control : " + obj) ;
         }
      }
   }
   private Object _next( byte c ){

     if( _engineState == 0 ){
       _engineControlClear() ;
       _engineState   = cctData ;
     }
     switch( _engineState ){
       case cctData :
          if( c == telnetIAC ){
             _engineState = cctCT1 ;
          }else if( c == telnetCR ){
             _engineState = cctCR ;
          }else{
             return _charOfByte( c )  ;
          }
       break ;
       case cctCT1 :
          if( c == telnetIAC ){
             _engineState = cctData ;
             return _charOfByte( c )   ;
          }else if( c == telnetSB ){
             _engineState = cctSUB ;
             _engineControlAdd( telnetIAC ) ;
             _engineControlAdd( c ) ;
          }else{
             _engineState = cctCT2 ;
             _engineControlAdd( telnetIAC ) ;
             _engineControlAdd( c ) ;
          }
       break ;
       case cctCT2 :
             _engineState = cctData ;
             _engineControlAdd( c ) ;
             return  _engineControlGet() ;
       case cctSUB :
          if( c == telnetIAC ){
             _engineState = cctESC ;
          }else{
             _engineControlAdd( c ) ;
          }
       break ;
       case cctESC :
          if( c == telnetSE ){
             _engineState = cctData ;
             _engineControlAdd( telnetIAC ) ;
             _engineControlAdd( c ) ;
             return _engineControlGet() ;
          }else{
             _engineState = cctSUB ;
             _engineControlAdd( telnetIAC ) ;
             _engineControlAdd( c ) ;
          }
       break ;
       case cctCR :
             _engineState = cctCR2 ;
             return '\n';
       case cctCR2 :
          if( c == telnetIAC ){
             _engineState = cctCT1 ;
          }else if( c == telnetCR ){
             _engineState = cctCR ;
          }else{
             _engineState = cctData ;
             return _charOfByte( c ) ;
          }
       break ;
     }
     return null ;
   }
//   private Byte _charOfByte( byte c ){ return new Byte( c ) ; }
   private Character _charOfByte( byte c ){
      byte [] rc = new byte[1] ;
      rc[0] = c ;
      return new String(rc).charAt(0);
   }


 }
