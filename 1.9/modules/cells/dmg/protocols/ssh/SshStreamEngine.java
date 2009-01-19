package dmg.protocols.ssh ;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Date;
import java.util.Random;

import dmg.security.CellUser;
import dmg.security.digest.Md5;
import dmg.util.StreamEngine;


public class      SshStreamEngine
       extends    SshCoreEngine 
       implements StreamEngine      {

   private final Socket        _socket ;
   private SshServerAuthentication _serverAuth = null ;
   private SshClientAuthentication _clientAuth = null ;
   private Thread        _listenerThread = null ;
   private SshRsaKey     _serverIdentity = null ;
   private SshRsaKey     _hostIdentity   = null ;
   private byte       [] _sessionId      = null ;
   private final int           _mode  ;
   private boolean       _closing = false ;
   private boolean       _closed  = false ;
   private final Object        _closeLock = new Object() ;
   
   private InetAddress   _remoteAddress = null ;
   private CellUser      _remoteUser    = null ;
   
   public final static int   SERVER_MODE = 1 ;
   public final static int   CLIENT_MODE = 2 ;
   
   private boolean _isActive = true ;
   
   private static final String __version = "SSH-1.5-3333\n" ;
   
   public SshStreamEngine( Socket socket , SshServerAuthentication auth )
          throws IOException ,
                 SshAuthenticationException         {
          
       super( socket ) ;
       _socket       = socket ;
       _serverAuth   = auth ;
       _mode         = SERVER_MODE ;
       
       runServerProtocol()  ;
   }
   public SshStreamEngine( Socket socket , SshClientAuthentication auth )
          throws IOException ,
                 SshAuthenticationException         {
          
       super( socket ) ;
       _socket       = socket ;
       _clientAuth   = auth ;
       _mode         = CLIENT_MODE ;
       
       runClientProtocol()  ;
   }
   public int getMode(){ return _mode ; }
   
   public Socket       getSocket(){ return _socket ; }
   public InputStream  getInputStream() { return new SshInputStream( this ) ; }
   public OutputStream getOutputStream(){ return new SshOutputStream( this ) ; }
   public CellUser       getUser(){ return _remoteUser ; }
   public CellUser       getUserName(){ return _remoteUser ; }
   public InetAddress  getInetAddress(){ return _remoteAddress ; }
   public InetAddress getLocalAddress() { return _socket.getLocalAddress();}

   public Reader getReader(){ 
      if( _mode == SERVER_MODE ){
         return  new SshInputStreamReader( 
                         getInputStream() ,
                         getOutputStream()   ) ;
      }else{
         return  new SshClientInputStreamReader( 
                         getInputStream() ,
                         getOutputStream()   ) ;
      }
           
   }
   public Writer getWriter(){
      if( _mode == SERVER_MODE ){
         return new SshOutputStreamWriter( getOutputStream() ) ;
      }else{
         return new SshClientOutputStreamWriter( getOutputStream() ) ;
      }
   }
   private void runClientProtocol() 
           throws IOException ,
                  SshAuthenticationException {
        printout( "SshStreamEngine : Connected" ) ;
        //
        // wait for the version string and check consistence
        //
        String version = readVersionString() ;
        printout( "SshStreamEngine : Received version : "+version ) ;
        //
        //  write the version string
        //
        printout( "SshStreamEngine : Sending our verstion : "+__version ) ;
        writeString( __version ) ;

        
        client_loop() ;
        printout( "SshStreamEngine : Preparation Loop finished " ) ;
        return ;
           
   }
   private void runServerProtocol() 
           throws IOException ,
                  SshAuthenticationException {
           
        printout( "SshStreamEngine : Connected" ) ;
        //
        _remoteAddress = _socket.getInetAddress() ;
        //         
        //
        //  write the version string
        //
        writeString( __version ) ;
        //
        // wait for the version string and check consistence
        //
        String version = readVersionString() ;
        //
        // generate and send the public key message
        //
        _serverIdentity = _serverAuth.getServerRsaKey() ;
        _hostIdentity   = _serverAuth.getHostRsaKey() ;

        if( ( _serverIdentity == null ) || ( _hostIdentity == null ) )
            throw new
            SshAuthenticationException("Either server or host Identity not found");

        SshSmsgPublicKey key = 
           new SshSmsgPublicKey( 
              _serverIdentity , _hostIdentity , 
              SshCoreEngine.SSH_CIPHER_MASK_IDEA | 
              SshCoreEngine.SSH_CIPHER_MASK_DES  | 
              SshCoreEngine.SSH_CIPHER_MASK_BLOWFISH , 
              SshCoreEngine.SSH_AUTH_RHOSTS_RSA |
              SshCoreEngine.SSH_AUTH_PASSWORD |
              SshCoreEngine.SSH_AUTH_RSA ) ;

        _sessionId = key.getSessionId() ;

        writePacket( key ) ;

        
        server_loop() ;
        printout( "SshStreamEngine : Preparation Loop finished " ) ;
        return ;



      
   }
   public void close() throws IOException { close(0) ; }
   
   public void close(int val ) throws IOException {
      printout( "SshStreamEngine : close( closing="+_closing+";closed="+_closed+")" ) ;
      if( _mode == SERVER_MODE ){
          synchronized( _closeLock ){
             if( ( ! _closing ) && ( ! _closed ) ){
                writePacket( new SshSmsgExitStatus(val) ) ;
                _closing = true ;
                _isActive = false ;
             }
          }
      }else{
          synchronized( _closeLock ){
              if( ! _closed )
                 throw new IOException("Client not allowed to close connection first" ) ;
          }
      }
   
   }
   
   public boolean isActive() {
	   boolean isActive;
	   synchronized( _closeLock ){
              isActive = _isActive ;
        }
	   
	   return isActive;
   }
   void confirmed() throws IOException {
      printout( "SshStreamEngine : confirmed( closing="+_closing+";closed="+_closed+")" ) ;
      if( _mode == SERVER_MODE ){
         synchronized( _closeLock ){
            if( _closing ){
               _closed  = true ;
               _closing = false ;
               _socket.close() ;
            }
         }
      }else{
         synchronized( _closeLock ){
            if( ! _closed ){
               _closed  = true ;
               _socket.close() ;
            }
         }
      }
   
   }
   private static final int ST_ERROR        = -1 ;
   private static final int ST_INIT         = 0 ;
   private static final int ST_USER         = 1 ;
   private static final int ST_AUTH         = 2 ;
   private static final int ST_PREPARE      = 3 ;
   private static final int ST_INTERACTIVE  = 4 ;
   private static final int ST_SESSION_SEND = 5 ;
   private static final int ST_USER_SEND    = 6 ;
   private static final int ST_TRY_RSA_AUTH = 7 ;
   private static final int ST_RSA_RESPONSE = 8 ;
   private static final int ST_EXEC_SEND    = 9 ;
   private static final int ST_TRY_PASSWORD_AUTH   = 10 ;
   private static final int ST_TRY_RHOSTS_RSA_AUTH = 11 ;

   private void   client_loop() 
   
           throws IOException , 
                  SshAuthenticationException {
                  
      int       state    = ST_INIT ;
      SshPacket packet  ;
      byte []   sessionId  = null ;
      SshRsaKey identity   = null ;
       
      while( ( packet = readPacket() ) != null  ){
         int packetType = packet.getType() ;
         printout( " Packet arrived : "+packetType ) ;
         switch(  packet.getType() ){
            case SshPacket.SSH_MSG_DISCONNECT :
            
               printout( "SshStreamEngine : SSH_MSG_DISCONNECT : not implemented" ) ;
               
            return  ;
            case SshPacket.SSH_MSG_DEBUG : {
               SshMsgDebug debug = new SshMsgDebug( packet ) ;
               
               printout( "SshStreamEngine : SSH_MSG_DEBUG : " + debug.getMessage()  ) ;
               
            }
            break ;
            case SshPacket.SSH_SMSG_PUBLIC_KEY :  {
                printout( "SshStreamEngine : SSH_SMSG_PUBLIC_KEY received" ) ;
                
                SshSmsgPublicKey publicKey = new SshSmsgPublicKey( packet ) ;
                
                if( ! _clientAuth.isHostKey( _socket.getInetAddress() ,
                                              publicKey.getHostKey() ) )
                   throw new SshAuthenticationException( "Unknown Host key" ) ;
                //
                // get the session id from the packet
                //
                sessionId  = publicKey.getSessionId() ;
                //
                //  get a random session Key
                //
                byte [] sessionKey = new byte[32] ;
                Random r   = new Random( new Date().getTime() ) ;
                r.nextBytes( sessionKey ) ;
                byte [] remSessionKey = new byte[32] ;
                System.arraycopy( sessionKey    , 0 , 
                                  remSessionKey , 0 ,
                                  sessionKey.length ) ;
                for(int i = 0 ; i < 16 ; i++ )remSessionKey[i] ^= sessionId[i] ;
                
                byte [] encrypted = 
                   publicKey.getHostKey().encrypt( 
                     publicKey.getServerKey().encrypt( remSessionKey ) ) ;
                     
                printout( "SshStreamEngine : Sending SshCmsgSessionKey" ) ;
                
                writePacket(
                   new SshCmsgSessionKey( SSH_CIPHER_IDEA ,
                                          publicKey.getCookie() ,
                                          encrypted ,
                                          0 ) 
                            ) ;
                //
                // make it idea
                //
                byte [] ideakey = new byte[16] ;
                
                System.arraycopy( sessionKey, 0 ,ideakey , 0 , 16 ) ;
                setEncryption( SSH_CIPHER_IDEA , ideakey ) ;
                
                state = ST_SESSION_SEND ;
                break ;

            }
            case SshPacket.SSH_SMSG_AUTH_RSA_CHALLENGE : {
               if( ( state != ST_TRY_RSA_AUTH ) &&
                   ( state != ST_TRY_RHOSTS_RSA_AUTH )     )
                  throw new 
                      SshProtocolException( 
                        "PANIC : SSH_SMSG_AUTH_RSA_CHALLENGE in state "+state ) ;
               
               SshSmsgAuthRsaChallenge challenge = 
                          new SshSmsgAuthRsaChallenge( packet ) ;
                          
               byte [] cBytes = challenge.getMpInt() ;
               
               cBytes = identity.decrypt( cBytes ) ;
               printout( "SshStreamEngine : SSH_SMSG_AUTH_RSA_CHALLENGE received" );
               try{
                  Md5 md5 = new Md5() ;
                  md5.update( cBytes ) ;
                  md5.update( sessionId ) ;
                  cBytes = md5.digest() ;               
               }catch( Exception eee ){}
               writePacket( new  SshCmsgAuthRsaResponse( cBytes ) ) ;
               state  = ST_RSA_RESPONSE ;
            }
            break ;
            case SshPacket.SSH_SMSG_SUCCESS :   {
              switch( state ){
                 case ST_SESSION_SEND :
                   printout( "SshStreamEngine : SSH_SMSG_SUCCESS on ST_SESSION_SEND" ) ;
                   writePacket( new SshCmsgUser( _clientAuth.getUser() ) ) ;
                   state = ST_USER_SEND ;
                 break ;
                 case ST_RSA_RESPONSE :
                   printout( "SshStreamEngine : SSH_SMSG_SUCCESS on ST_RSA_RESPONSE" ) ;
                   writePacket( new SshCmsgExecShell(  ) ) ;
                   state = ST_INTERACTIVE ;
                   return ;
                 case ST_TRY_PASSWORD_AUTH :
                   printout( "SshStreamEngine : SSH_SMSG_SUCCESS on ST_TRY_PASSWORD_AUTH" ) ;
                   writePacket( new SshCmsgExecShell(  ) ) ;
                   state = ST_INTERACTIVE ;
                   return ;
                 default :
                    throw new 
                      SshProtocolException( 
                        "PANIC : SSH_SMSG_SUCCESS in state "+state ) ;
              }
            }
            break ;
            case SshPacket.SSH_SMSG_FAILURE :  {
              switch( state ){
                 case ST_RSA_RESPONSE :
                 case ST_TRY_RSA_AUTH : 
                 case ST_TRY_RHOSTS_RSA_AUTH : 
                 case ST_TRY_PASSWORD_AUTH : 
                 case ST_USER_SEND : 
                 {
                   printout( "SshStreamEngine : SSH_SMSG_FAILURE on "+state ) ;
                   //
                   // start rsa authentication
                   // 
                   SshAuthMethod method = _clientAuth.getAuthMethod() ;
                   if( method == null )
                       throw new SshAuthenticationException("No more methods from Client");
                   
                   if( method instanceof SshAuthRsa ){
                       identity = method.getKey() ;
                       byte []   modulus  = identity.getModulusBytes() ;
                       int       bits     = identity.getModulusBits() ;
                       writePacket( 
                              new SshCmsgAuthRsa( modulus , 0 , bits ) 
                                  ) ;
                       state  = ST_TRY_RSA_AUTH ;
                       printout( "SshStreamEngine : Sending ST_TRY_RSA_AUTH" ) ;
                   }else if( method instanceof SshAuthRhostsRsa ){
                       identity = method.getKey() ;
                       writePacket( 
                              new SshCmsgAuthRhostsRsa( method.getUser() , identity ) 
                                  ) ;
                       state  = ST_TRY_RHOSTS_RSA_AUTH ;
                       printout( "SshStreamEngine :  Sending ST_TRY_RSA_RHOSTS_AUTH" ) ;
                   }else if( method instanceof SshAuthPassword ){
                       writePacket( 
                              new SshCmsgAuthPassword( method.getUser() ) 
                                  ) ;
                       printout( "SshStreamEngine : Sending ST_TRY_PASSWORD_AUTH" ) ;
                       state  = ST_TRY_PASSWORD_AUTH ;
                   }else
                       throw new SshProtocolException("Illegal class from Client");
                 }
                 break ;
                 default :
                    throw new 
                      SshProtocolException( 
                        "PANIC : SSH_SMSG_FAILURE in state "+state ) ;
              }
            } 
            break ;
            default : 
                printerr( "SshStreamEngine :  Unknown denied : "+packet.getType() ) ;
         }

      }
      return  ;
   
   }
   private void server_loop() 
                throws   IOException, 
                         SshAuthenticationException {
   
      SshSmsgFailure bad = new SshSmsgFailure() ;
      SshSmsgSuccess ok  = new SshSmsgSuccess() ;
      SshPacket      packet ;
      byte       []  challenge = null  ;
      
      int       state    = ST_INIT ;
      
      while( ( packet = readPacket() ) != null  ){

         switch(  packet.getType() ){
            case SshPacket.SSH_MSG_DISCONNECT :
            
               printout( "SshStreamEngine : SSH_MSG_DISCONNECT" ) ;
               
            return  ;
            case SshPacket.SSH_MSG_DEBUG : {
               SshMsgDebug debug = new SshMsgDebug( packet ) ;
               
               printout( "SshStreamEngine : SSH_MSG_DEBUG : " + debug.getMessage()  ) ;
               
            }
            break ;
            case SshPacket.SSH_CMSG_AUTH_RSA : {
            
               printout( "SshStreamEngine : SSH_CMSG_AUTH_RSA" ) ;
               
               SshCmsgAuthRsa rsa = new SshCmsgAuthRsa( packet ) ;
               //
               // we have to ask the listener for the full key
               // of the modulus bytes.
               //
               SshRsaKey userKey = _serverAuth.authRsa( _remoteAddress , 
                                                        _remoteUser.getName() ,
                                                        rsa.getKey() ) ;
               if( userKey == null ){
                  printerr( "SshStreamEngine : SSH_CMSG_AUTH_RSA : Key not found" ) ;
                  writePacket( bad ) ;
                  break ;
               }
               printout( "SshStreamEngine : SSH_CMSG_AUTH_RSA : Key found " ) ;
               //
               // we need to create the callenge,
               // encrypt it with the user key and send it back
               //
               Random r  = new Random( System.currentTimeMillis() ) ;
               challenge = new byte[32] ;
               r.nextBytes( challenge ) ;
               BigInteger bi = userKey.encryptBigInteger(challenge , 
                                                         0 , 
                                                         challenge.length)  ;
               writePacket(
                  new SshSmsgAuthRsaChallenge( bi.toByteArray() , 
                                               0 , 
                                               bi.bitLength() ) ) ;
                                               
            }
            break ;
            case SshPacket.SSH_CMSG_AUTH_RHOSTS_RSA : {
            
               printout( "SshStreamEngine : SSH_CMSG_AUTH_RHOSTS_RSA" ) ;
               
               SshCmsgAuthRhostsRsa rsa = new SshCmsgAuthRhostsRsa( packet ) ;
               //
               // we have to ask the listener for the full key
               // of the modulus bytes.
               //
               SshRsaKey hostKey = _serverAuth.authRhostsRsa( _remoteAddress ,
                                                              _remoteUser.getName() ,
                                                              rsa.getUserName() ,
                                                              rsa.getKey()         ) ;
               if( hostKey == null ){
                  printerr( "SshStreamEngine : SSH_CMSG_AUTH_RSA : Key not found" ) ;
                  writePacket( bad ) ;
                  break ;
               }
               printout( "SshStreamEngine : SSH_CMSG_AUTH_RSA : Key found "+hostKey ) ;
               //
               // we need to create the callenge,
               // encrypt it with the user key and send it back
               //
               Random r  = new Random( System.currentTimeMillis() ) ;
               challenge = new byte[32] ;
               r.nextBytes( challenge ) ;

               writePacket(
                  new SshSmsgAuthRsaChallenge( 
                         hostKey.encrypt( challenge , 0 , challenge.length ) ,
                         0 , 
                         hostKey.getKeySize() )  ) ;
                                               
            }
            break ;
            case SshPacket.SSH_CMSG_AUTH_RSA_RESPONSE : {
            
               printout( "SshStreamEngine : SSH_CMSG_AUTH_RSA_RESPONSE" ) ;
               
               SshCmsgAuthRsaResponse rsaresp = new SshCmsgAuthRsaResponse( packet ) ;
               
               if( challenge == null ) 
                 throw new 
                 SshProtocolException( 
                 "SSH_CMSG_AUTH_RSA_RESPONSE challenge not found" ) ;
               
               byte [] response = rsaresp.getResponse() ;
               //
               // we get the md5 sum of the challenge || sessionId
               //               
               if( response.length != 16 ){
                  printerr( "SshStreamEngine :  SSH_CMSG_AUTH_RSA_RESPONSE : "+
                            " response != 16 ("+response.length+" bytes)" ) ;
                  writePacket( bad ) ;
                  break ;
               }
               Md5 digest ;
               try{ digest = new Md5() ; }catch(Exception ne){ digest=null;}
               digest.update( challenge ) ;
               digest.update( _sessionId ) ;
               byte [] res = digest.digest() ;

               int k ;
               for( k = 0 ; 
                    ( k < res.length ) && ( res[k] == response[k] ) ; k++ ) ;
               if( k == 16 ){
                  printout( "SshStreamEngine : SSH_CMSG_AUTH_RSA_RESPONSE : O.K." ) ;
                  writePacket( ok ) ;
                  state  = ST_PREPARE ;
               }else{
                  printout( "SshStreamEngine : SSH_CMSG_AUTH_RSA_RESPONSE : failed" ) ;
                  writePacket( bad ) ;
               }
            }
            break ;
            case SshPacket.SSH_CMSG_SESSION_KEY :{
            
               printout( "SshStreamEngine : SSH_CMSG_SESSION_KEY" ) ;
               
               if( state != ST_INIT ) 
                 throw new 
                 SshProtocolException( "SSH_CMSG_SESSION_KEY in not INIT state" ) ;
               
               SshCmsgSessionKey session = 
                  new SshCmsgSessionKey( _serverIdentity , _hostIdentity , packet ) ;
                     
               byte [] sessionKey = session.getSessionKey() ;        
               for( int i = 0 ; i < 16 ; i++ ) sessionKey[i] ^= _sessionId[i] ;
           
               if( ! setEncryption( session.getCipher() , sessionKey ) ){
                  writePacket( bad ) ;
                  throw new 
                  SshProtocolException( "Cipher : can't use "+session.getCipher() ) ;
               
               }
               writePacket( ok ) ;
               state = ST_USER ;
            }  
            break ;
            case SshPacket.SSH_CMSG_USER : {
            
               printout( "SshStreamEngine : SSH_CMSG_USER : arrived" ) ;
               if( state != ST_USER )
                 throw new 
                 SshProtocolException( "SSH_CMSG_USER in not USER state" ) ;
                 
               SshCmsgUser user = new SshCmsgUser( packet ) ;
               
               printout( "SshStreamEngine : SSH_CMSG_USER : User = "+user.getUser() ) ;
               _remoteUser = new CellUser( user.getUser() , null, null) ;
               if( _serverAuth.authUser( _remoteAddress , _remoteUser.getName() ) ){
                   state  = ST_PREPARE ;
                   writePacket( ok ) ;
               }else{
                   state  = ST_AUTH ;
                   writePacket( bad ) ;
               }
            }
            break ;
            case SshPacket.SSH_CMSG_AUTH_PASSWORD : {
            
               printout( "SshStreamEngine : SSH_CMSG_AUTH_PASSWORD : arrived" ) ;
               if( state != ST_AUTH )
                 throw new 
                 SshProtocolException( "SSH_CMSG_AUTH_PASSWORD in not ST_AUTH state" ) ;
                
                SshCmsgAuthPassword psw = 
                     new SshCmsgAuthPassword( packet) ;

                if( _serverAuth.authPassword( _remoteAddress ,
                                              _remoteUser.getName() ,
                                              psw.getPassword() ) ){
                   writePacket( ok ) ;
                   state    = ST_PREPARE ;
                }else{
                   writePacket( bad ) ;
                }
            }
            break ;
            case SshPacket.SSH_CMSG_REQUEST_PTY :
               if( state != ST_PREPARE ) 
                 throw new 
                 SshProtocolException( "SSH_CMSG_REQUEST_PTY in not ST_PREPARE state" ) ;
               writePacket( ok ) ;
               printout( "SshStreamEngine : SSH_CMSG_REQUEST_PTY : o.k." ) ;
            break ;
            case SshPacket.SSH_CMSG_X11_REQUEST_FORWARDING :
               if( state != ST_PREPARE )
                 throw new 
                 SshProtocolException( "SSH_CMSG_X11_REQUEST_FORWARDING in not ST_PREPARE state" ) ;
               writePacket( ok ) ;
               printout( "SshStreamEngine : SSH_CMSG_REQUEST_PTY : o.k." ) ;
            break ;
            case SshPacket.SSH_CMSG_EXEC_SHELL :
               if( state != ST_PREPARE ) 
                 throw new 
                 SshProtocolException( "SSH_CMSG_EXEC_SHELL in not ST_PREPARE state" ) ;
               printout( "SshStreamEngine : SSH_CMSG_EXEC_SHELL : o.k." ) ;
               state = ST_INTERACTIVE ;
               return ;
            case SshPacket.SSH_CMSG_EXEC_CMD :
               if( state != ST_PREPARE ) 
                 throw new 
                 SshProtocolException( "SSH_CMSG_EXEC_CMD in not ST_PREPARE state" ) ;
               printout(" SSH_CMSG_EXEC_CMD : "+( new SshCmsgExecCmd(packet)));
               state = ST_INTERACTIVE ;
               return ;
            case SshPacket.SSH_CMSG_EXIT_CONFORMATION :
                printout( "SshStreamEngine : SSH_CMSG_EXIT_CONFORMATION : o.k." ) ;
                
            break ;
            case SshPacket.SSH_CMSG_STDIN_DATA :{
            
              throw new 
              SshProtocolException( "SSH_CMSG_STDIN_DATA while preparing (FATAL)" ) ;
               
                       
            }   
            default : 
                printerr( "SshStreamEngine : Unknown denied : "+packet.getType() ) ;
                if( state != ST_INTERACTIVE )writePacket( bad ) ;
         }

      }
      return  ;
   
   }

}
