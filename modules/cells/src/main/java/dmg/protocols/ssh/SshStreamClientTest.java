package dmg.protocols.ssh ;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;

public class SshStreamClientTest
       implements SshClientAuthentication {

   SshRsaKey          _userKey , _hostKey ;
   SshRsaKeyContainer _objectKeys ;
   SshStreamEngine    _engine ;
   Socket             _socket ;

   private final static int ST_NULL     = 0 ;
   private final static int ST_PASSWORD = 1 ;
   private final static int ST_RSA      = 2 ;
   private final static int ST_RHOSTS_RSA      = 3 ;


   public SshStreamClientTest( String address ,
                               int port ,
                         String objectsKeysFile ,
                         String userKeyFile  ,
                         String hostKeyFile      ) throws Exception {

      _socket    = new Socket( address , port ) ;


      _objectKeys  = new SshRsaKeyContainer(
                              new FileInputStream( objectsKeysFile ) ) ;

      _userKey   = userKeyFile.equals("NONE") ? null :
                   new SshRsaKey( new FileInputStream( userKeyFile ) ) ;

      _hostKey   = hostKeyFile.equals("NONE") ? null :
                   new SshRsaKey( new FileInputStream( hostKeyFile ) ) ;

      _engine    = new SshStreamEngine( _socket , this ) ;

      System.out.println( "Creating reader" ) ;
      BufferedReader br = new BufferedReader(_engine.getReader() ) ;

      System.out.println( "Creating writer" ) ;
      PrintWriter pw = new PrintWriter(  _engine.getWriter() ) ;
      String str;
      System.out.println( "Sending command" ) ;
      pw.println( "ps -a" ) ;
      pw.println( "exit" ) ;
      pw.println( "exit" ) ;
      pw.flush() ;
      while( ( str = br.readLine() ) != null ){
         System.out.println( " line received : "+str ) ;
      }
   }
   /*
   public SshStreamTest( Socket socket  ,
                         SshRsaKey host , SshRsaKey server ,
                         SshRsaKeyContainer users             )
          throws Exception {

      _hostKey    = host ;
      _serverKey  = server ;
      _userKeys   = users ;
      _socket     = socket ;

      Thread runner = new Thread( this ) ;
      runner.start() ;
   }

   public void run()  {
    try{
      _engine = new SshStreamEngine( _socket , this ) ;


      BufferedReader br = new BufferedReader(
                          new SshInputStreamReader(
                                    _engine.getInputStream() ,
                                    _engine.getOutputStream() ) ) ;

      PrintWriter pw = new PrintWriter(
                       new SshOutputStreamWriter(
                          _engine.getOutputStream() )  ) ;

      String str ;

      while( ( str = br.readLine() ) != null ){
         System.out.println( " line received : "+str ) ;
         if( str.equals( "exit" ) ){
            _engine.close(3);
         }else{
           pw.println( str ) ;
           pw.flush();
         }
      }

      System.out.println( " Finished ... " ) ;
    }catch( Exception exc ){
       System.out.println( " Exception in run loop : "+exc ) ;
    }
   }
   */

   //
   //   Client Authentication interface
   //
   @Override
   public boolean  isHostKey( InetAddress host , SshRsaKey keyModulus ) {
       SshRsaKey key = _objectKeys.findByModulus( keyModulus ) ;
       System.out.println( "ServerKey : "+keyModulus.getFingerPrint() ) ;
       if( key == null ){
          System.out.println( " Couldn't find key for host "+host ) ;
          return true ;
       }else{
          System.out.println( " Key found for host "+host+
                              " -> "+key.getComment() ) ;
          return true ;
       }
   }
   @Override
   public String getUser( ){
      System.out.println( " User requested" ) ;
      return "manfred" ;
   }
   @Override
   public SshSharedKey  getSharedKey( InetAddress host ){ return null ; }
   private int _state = ST_RSA ;

   @Override
   public SshAuthMethod getAuthMethod(){

      SshAuthMethod rc = null ;

      switch( _state ){
         case ST_NULL :
            rc = null ;
         break ;
         case ST_PASSWORD :
            System.out.println( "Trying ST_PASSWORD" ) ;
            rc = new SshAuthPassword( "manfred" ) ;
            _state = ST_RHOSTS_RSA ;
            break ;
         case ST_RSA :
            System.out.println( "Trying ST_RSA" ) ;
            System.out.println( " User Key : \n"+_userKey ) ;
            rc = new SshAuthRsa( _userKey ) ;
            _state = ST_NULL ;
            break ;
         case ST_RHOSTS_RSA :
            System.out.println( "Trying ST_RHOSTS_RSA" ) ;
            rc = new SshAuthRhostsRsa( "patrick" , _hostKey ) ;
            _state = ST_NULL ;
         break ;

      }
      return rc ;
   }

   public static void main( String [] args ){

      if( args.length < 5 ){
        System.out.println(
        " USAGE : SshStreamClientTest <host> <port> "+
        "<knownObjects.pub> <userKey.priv>|NONE  <hostKey.priv>|NONE");
        System.exit(4);
      }
      try{
         String address = args[0] ;
         int port  = new Integer(args[1]);

         String knownObjects = args[2] ;
         String userKey      = args[3] ;
         String hostKey      = args[4] ;

         new SshStreamClientTest( address , port ,
                                  knownObjects , userKey , hostKey ) ;

      }catch( Exception e ){
         System.out.println( "Exception : "+e ) ;
         e.printStackTrace() ;
      }
   }


}
