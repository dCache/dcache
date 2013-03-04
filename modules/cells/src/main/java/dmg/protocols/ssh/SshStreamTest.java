package dmg.protocols.ssh ;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

public class SshStreamTest
       implements SshServerAuthentication , Runnable {

   SshRsaKey          _hostKey ;
   SshRsaKey          _serverKey ;
   SshRsaKeyContainer _userKeys ;
   SshStreamEngine    _engine ;
   Socket             _socket ;

   public SshStreamTest( int port ,
                         String hostKeyFile ,
                         String serverKeyFile ,
                         String userKeysFile        ) throws Exception {

      ServerSocket listen = new ServerSocket( port ) ;
      SshRsaKey  hostKey   = new SshRsaKey(
                              new FileInputStream( hostKeyFile ) ) ;
      SshRsaKey  serverKey = new SshRsaKey(
                              new FileInputStream( serverKeyFile ) ) ;
      SshRsaKeyContainer userKeys  = new SshRsaKeyContainer(
                              new FileInputStream( userKeysFile ) ) ;
      while( true ){
        try{
           Socket    socket = listen.accept() ;

           new SshStreamTest( socket , hostKey , serverKey , userKeys ) ;
        }catch( Exception e ){
           System.err.println( " Exception : "+e ) ;
           e.printStackTrace() ;
        }

      }

   }
   public SshStreamTest( Socket socket  ,
                         SshRsaKey host , SshRsaKey server ,
                         SshRsaKeyContainer users             )
   {

      _hostKey    = host ;
      _serverKey  = server ;
      _userKeys   = users ;
      _socket     = socket ;

      Thread runner = new Thread( this ) ;
      runner.start() ;
   }
   @Override
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
            pw.close();
         }else{
           pw.println( str ) ;
           pw.flush();
         }
      }

      System.out.println( " Finished ... " ) ;
    }catch( Exception exc ){
       System.out.println( " Exception in run loop : "+exc ) ;
       exc.printStackTrace() ;
       try{_socket.close() ;}catch(IOException e){}
    }
   }

   @Override
   public boolean authUser( InetAddress host, String user ){
      System.out.println( "authUser : Host="+host+" User "+user+" requested and denied" ) ;
      return user.equals("elchy") ? true : false ;
   }
   @Override
   public boolean authPassword( InetAddress host, String user , String password ){
      System.out.println( "authPassword : Host="+host+" User="+user+" Password="+password+" requested" ) ;
      if( user.equals("patrick") && password.equals( "hallo" ) ) {
          return true;
      }
      return false ;
   }
   @Override
   public boolean authRhosts(    InetAddress host, String user ){
      System.out.println( "authRhosts : Host="+host+" User "+user+" requested and denied" ) ;
      return false ;
   }

   @Override
   public SshRsaKey  authRsa( InetAddress host, String user , SshRsaKey userKey ){

      System.out.println( "authRsa : host="+host+" key=" ) ;
      System.out.println( ""+userKey ) ;


      SshRsaKey key = _userKeys.findByModulus( userKey ) ;

      if( key == null ){
         System.out.println( " Request modulus not found" ) ;
         return null ;
      }
      System.out.println( " Request modulus found : "+key.getComment() ) ;
      return key ;

   }
   @Override
   public SshRsaKey  authRhostsRsa( InetAddress host, String userName ,
                                    String reqUser , SshRsaKey hostKey ){
      System.out.println( "authRhostsRsa : host="+host+" user="+userName+" key=" ) ;
      System.out.println( ""+hostKey ) ;
      SshRsaKey key = _userKeys.findByModulus( hostKey ) ;

      if( key == null ){
         System.out.println( " Request modulus not found" ) ;
         return null ;
      }
      System.out.println( " Request modulus found : "+key.getComment() ) ;
      return key ;
   }

   @Override
   public SshRsaKey  getHostRsaKey() { return _hostKey ; }


   @Override
   public SshRsaKey  getServerRsaKey()  {  return _serverKey ; }

   @Override
   public SshSharedKey  getSharedKey( InetAddress host , String keyName ){
     return null ;
   }

   public static void main( String [] args ){

      if( args.length < 4 ){
        System.out.println(
        " USAGE : SshServerTest <port> <hostKeyFile> <serverKeyFile> <user>");
        System.exit(4);
      }
      try{
         int port  = new Integer(args[0]);
         new SshStreamTest( port , args[1] , args[2], args[3] ) ;
      }catch( Exception e ){
         System.out.println( "Exception : "+e ) ;
         e.printStackTrace() ;
      }
   }


}
