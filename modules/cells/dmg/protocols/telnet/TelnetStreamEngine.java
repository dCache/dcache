package dmg.protocols.telnet ;

import java.net.* ;
import java.io.* ;
import java.util.* ;
import java.lang.reflect.* ;

import dmg.security.CellUser;
import dmg.util.* ;


public class  TelnetStreamEngine implements StreamEngine {


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
     private static final byte telnetOptionLine  = (3);

     private static final int cctData = 1 ;
     private static final int cctCR   = 2 ;
     private static final int cctCR2  = 3 ;
     private static final int cctCT1  = 4 ;
     private static final int cctCT2  = 5 ;
     private static final int cctSUB  = 6 ;
     private static final int cctESC  = 7 ;

     public  static final byte [] telnetBN = { telnetCR , telnetLF } ;

     private final byte willEcho[] = {
          telnetIAC , telnetWILL , telnetOptionLine ,
          telnetIAC , telnetWILL , telnetOptionEcho
     } ;
     private final byte wontEcho[] = {
          telnetIAC , telnetWONT , telnetOptionLine ,
          telnetIAC , telnetWONT , telnetOptionEcho
     } ;
    //
    // class variables
    //
    private int         _engineState ;
    private int         _controlDataPos ;
    private byte []     _controlData ;

   boolean     _lineMode = true ;
   boolean     _echoMode = true ;
   boolean _passwordMode = false ;

   private Socket                     _socket     = null ;
   private TelnetServerAuthentication _serverAuth = null ;
   private InetAddress                _host       = null ;
   private CellUser                   _userName   = new CellUser("unknow", null, null);
   private InputStream   _inputStream ;
   private OutputStream  _outputStream ;

   private TelnetInputStream2  _telnetInputStream ;
   private TelnetOutputStream2 _telnetOutputStream ;

   private TelnetInputStreamReader _reader ;
   private TelnetOutputStreamWriter _writer ;


   public TelnetStreamEngine( Socket socket ,
                              TelnetServerAuthentication auth )
          throws IOException, TelnetAuthenticationException, IllegalArgumentException, IllegalAccessException, InvocationTargetException {

      _socket     = socket ;
      _serverAuth = auth ;

      _engineState    = 0 ;
      _controlData    = null ;
      _controlDataPos = 0 ;

      _host               = _socket.getInetAddress() ;

      _inputStream        = _socket.getInputStream() ;
      _outputStream       = _socket.getOutputStream() ;

      _telnetInputStream  = new TelnetInputStream2( this )  ;
      _telnetOutputStream = new TelnetOutputStream2( this ) ;

      _writer = new TelnetOutputStreamWriter( _telnetOutputStream ) ;
      _reader = new TelnetInputStreamReader( _telnetInputStream , _writer ) ;

      try{
    	       Method meth = _socket.getClass().getMethod( "verify" , (Class [])null) ;
    	       Boolean verified = (Boolean)meth.invoke( _socket , new Object[0] );
    	       if (verified.booleanValue()) {
    	           meth = _socket.getClass().getMethod( "getUserPrincipal" , new Class[0] ) ;
    	           String user = (String)meth.invoke( _socket , new Object[0] );

    	           meth = _socket.getClass().getMethod( "getRole" , new Class[0] ) ;
    	           String role = (String)meth.invoke( _socket , new Object[0] );

    	           meth = _socket.getClass().getMethod( "getGroup" , new Class[0] ) ;
    	           String group = (String)meth.invoke( _socket , new Object[0] );

    	           _userName = new CellUser( user, group, role);
    	       } else {
                   String hostAddress = (_socket.getInetAddress()).getHostAddress();
                   _socket.close();
                   throw new TelnetAuthenticationException("Host "+hostAddress+": Tunnel verification failed!" ) ;
    	       }

      }catch(NoSuchMethodException nsm){
    	  // nsm.printStackTrace();
    	  // it's not a tunnel...still OK
      }

      if( _serverAuth != null )doAuthentication() ;

   }
   //
   // the stream engine interface
   //
   public Reader getReader(){ return _reader ; }
   public Writer getWriter(){ return _writer ; }

   public InputStream  getInputStream(){  return _telnetInputStream  ; }
   public OutputStream getOutputStream(){ return _telnetOutputStream ; }

   public CellUser getUserName() {         return _userName ; }

   public InetAddress getInetAddress(){  return _host ;  }
   public InetAddress getLocalAddress() { return _socket.getLocalAddress(); }

   public Socket getSocket() { return _socket; }


   //
   // the public and package part
   //
   public void setEcho( boolean echo ) throws IOException {
      _outputStream.write( echo ? wontEcho : willEcho ) ;
      _outputStream.flush();
   }
   public void setPasswordMode(boolean p ) throws IOException {
      _passwordMode = p ;
      setEcho( ! p ) ;
   }
   void close() throws IOException {
       _outputStream.close() ;
       _inputStream.close() ;
   }
   int read() throws IOException {
      Object obj ;
      while(true){
         if(  ( obj = readNext() ) == null ){
            return -1 ;
         }else if( obj instanceof Byte ){
            int b = ((Byte)obj).intValue() ;
            b = b == 13 ? '\n' : b ;
            if( ( ! _echoMode ) && ( ! _passwordMode ) )write( b ) ;
            return b ;
         }else if( obj instanceof byte [] ){
            _handleControl(  (byte [])obj ) ;
         }
      }
   }
   void write( int c ) throws IOException {

       if( c == '\n' ){
         _outputStream.write( 0xd ) ;
         _outputStream.write( 0xa ) ;
       }else
         _outputStream.write( c ) ;

   }
   //
   // now the private parts
   //
   private void doAuthentication()
           throws TelnetAuthenticationException,
                  IOException  {

      if( _serverAuth.isHostOk( _host ) )return ;
      setEcho( true ) ;
      _writer.write( "\n      User : " ) ;
      _writer.flush() ;
      BufferedReader r  = new BufferedReader( _reader ) ;
      String user = r.readLine() ;
      if( _serverAuth.isUserOk( _host , user ) ){
         _userName = new CellUser( user , null, null) ;
         return ;
      }
      setPasswordMode( true ) ;
      _writer.write( "  Password : " ) ;
      _writer.flush() ;
      String password = r.readLine() ;
      if( _serverAuth.isPasswordOk( _host , user , password ) ){
         setPasswordMode( false ) ;
         _userName = new CellUser( user , null, null) ;
         _writer.write("\n\n") ;
         _writer.flush();
         return ;
      }
      _writer.write( "\n\n !!! Access Denied !!! \n" ) ;
      _writer.flush() ;
      _writer.close() ;
      throw new
      TelnetAuthenticationException(
          "Not authenticated (host="+_host+";user="+user+")" ) ;


   }
   private void _handleControl( byte [] cntr )throws IOException{
//      System.out.print("Control arrived : ");
//      for( int i = 0 ; i < cntr.length ; i++ )
//        System.out.print( ""+cntr[i]+" " ) ;
//      System.out.println("");
      if(  cntr.length == 3 ){
         switch( cntr[1] ){

            case telnetDONT :
              switch( cntr[2] ) {
                 case telnetOptionLine : _lineMode = true ; break ;
                 case telnetOptionEcho : _echoMode = true ; break ;
              }
              _writer.flush() ;
              cntr[1] = telnetWONT ;
             _outputStream.write( cntr ) ;
             _outputStream.flush() ;
            break ;
            case telnetDO :
              switch( cntr[2] ) {
                 case telnetOptionLine : _lineMode = false ; break ;
                 case telnetOptionEcho : _echoMode = false ; break ;
              }
              _writer.flush() ;
              cntr[1] = telnetWILL ;
             _outputStream.write( cntr ) ;
             _outputStream.flush() ;
            break ;

         }
      }
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
       if( _controlData == null )_controlData    = new byte[32] ;
       _controlDataPos = 0 ;
   }
   private byte [] _engineControlGet(){
      if( _controlDataPos == 0 )return null ;

      byte [] rc = new byte[ _controlDataPos ] ;

      System.arraycopy( _controlData , 0 , rc , 0 , _controlDataPos ) ;

      _engineControlClear() ;
      return rc ;
   }
   private Object readNext() throws IOException {
      int    rc ;
      Object obj ;
      while( true ){

         if( ( rc = _inputStream.read() ) < 0 )return null ;
         if( ( obj = _next( (byte)rc ) ) != null )return obj ;
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
             return Byte.valueOf ( c )  ;
          }
       break ;
       case cctCT1 :
          if( c == telnetIAC ){
             _engineState = cctData ;
             return Byte.valueOf ( c )   ;
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
             return Byte.valueOf( (byte)'\n' ) ;
       case cctCR2 :
          if( c == telnetIAC ){
             _engineState = cctCT1 ;
          }else if( c == telnetCR ){
             _engineState = cctCR ;
          }else{
             _engineState = cctData ;
             return Byte.valueOf ( c ) ;
          }
       break ;
     }
     return null ;
   }

}
