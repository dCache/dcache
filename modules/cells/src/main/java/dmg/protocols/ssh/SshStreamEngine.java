package dmg.protocols.ssh ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.Socket;
import java.security.Principal;
import java.util.Date;
import java.util.Random;

import dmg.security.digest.Md5;
import dmg.util.StreamEngine;

import org.dcache.auth.Subjects;
import org.dcache.auth.UserNamePrincipal;

public class      SshStreamEngine
       extends    SshCoreEngine
       implements StreamEngine      {

   private static final Logger _log = LoggerFactory.getLogger(SshStreamEngine.class);
   private final Socket        _socket ;
   private SshServerAuthentication _serverAuth;
   private SshClientAuthentication _clientAuth;
   private Thread        _listenerThread;
   private SshRsaKey     _serverIdentity;
   private SshRsaKey     _hostIdentity;
   private byte       [] _sessionId;
   private final int           _mode  ;
   private boolean       _closing;
   private boolean       _closed;
   private final Object        _closeLock = new Object() ;

   private InetAddress   _remoteAddress;
   private Subject      _remoteUser    = new Subject();

   private String _terminal;
   private int _width;
   private int _height;

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
       _remoteUser.setReadOnly();
       runServerProtocol()  ;
   }
   public SshStreamEngine( Socket socket , SshClientAuthentication auth )
          throws IOException ,
                 SshAuthenticationException         {

       super( socket ) ;
       _socket       = socket ;
       _clientAuth   = auth ;
       _mode         = CLIENT_MODE ;
       _remoteUser.setReadOnly();
       runClientProtocol()  ;
   }
   public int getMode(){ return _mode ; }

   @Override
   public Socket       getSocket(){ return _socket ; }
   @Override
   public InputStream  getInputStream() { return new SshInputStream( this ) ; }
   @Override
   public OutputStream getOutputStream(){ return new SshOutputStream( this ) ; }
   public String getName() { return Subjects.getUserName(_remoteUser); }
   @Override
   public Subject      getSubject(){ return _remoteUser ; }
   @Override
   public InetAddress  getInetAddress(){ return _remoteAddress ; }
   @Override
   public InetAddress getLocalAddress() { return _socket.getLocalAddress();}

    @Override
    public String getTerminalType()
    {
        return _terminal;
    }

    @Override
    public int getTerminalWidth()
    {
        return _width;
    }

    @Override
    public int getTerminalHeight()
    {
        return _height;
    }

   @Override
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
   @Override
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
        _log.debug("Connected (client)");
        //
        // wait for the version string and check consistence
        //
        String version = readVersionString() ;
        _log.debug("Received version: {}", version);
        //
        //  write the version string
        //
        _log.debug("Sending our version: {}", __version);
        writeString( __version ) ;


        client_loop() ;
        _log.debug("Preparation Loop finished");

   }
   private void runServerProtocol()
           throws IOException ,
                  SshAuthenticationException {

        _log.debug("Connected (server)");
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
        _log.debug("Client has version: {}", version);
        //
        // generate and send the public key message
        //
        _serverIdentity = _serverAuth.getServerRsaKey() ;
        _hostIdentity   = _serverAuth.getHostRsaKey() ;

        if( ( _serverIdentity == null ) || ( _hostIdentity == null ) ) {
            throw new
                    SshAuthenticationException("Either server or host Identity not found");
        }

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
        _log.debug("Preparation Loop finished");
   }

   public void close() throws IOException { close(0) ; }

   public void close(int val ) throws IOException {
      _log.debug("close(closing={}; closed={})", _closing, _closed);
      if( _mode == SERVER_MODE ){
          synchronized( _closeLock ){
             if( ( ! _closing ) && ( ! _closed ) ){
                 _closing = true ;
                 _isActive = false ;
                 shutdown(val);
             }
          }
      }else{
          synchronized( _closeLock ){
              if( ! _closed ) {
                  throw new IOException("Client not allowed to close connection first");
              }
          }
      }
   }

   private void shutdown(int value) throws IOException {
       writePacket(new SshSmsgExitStatus(value));
       _socket.shutdownOutput();
       waitForClientConfirmation();
       waitForClientClose();
       confirmed();
   }

   private void waitForClientConfirmation() throws IOException {
       SshPacket packet = readPacket();

       /* It can happen that a client closes their end of the connection just
        * as they ran the logoff command.  This triggers a race between them
        * sending us the EOF packet and our closing the session.  We silently
        * ignore such EOF messages since we're closing the connection anyway.
        */
       if(packet != null && packet.getType() == SshPacket.SSH_CMSG_EOF) {
           packet = readPacket();
       }

       if(packet != null &&
               packet.getType() != SshPacket.SSH_CMSG_EXIT_CONFORMATION) {
           _log.warn("received unexpected message (type={}) after server " +
                     "send exit message; discarding any subsequent data.",
                     packet.getType());
           do {
               packet = readPacket();
           } while(packet != null &&
                   packet.getType() != SshPacket.SSH_CMSG_EXIT_CONFORMATION);
       }
   }

   private void waitForClientClose() throws IOException {
       SshPacket packet = readPacket();

       if( packet != null) {
           _log.warn("unexpected client packet (type={}) after server " +
                     "finished the session; discarding incoming data until " +
                     "client closes.",
                     packet.getType());

           do {
               packet = readPacket();
           } while(packet != null);
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
      _log.debug("confirmed(closing={}; closed={})", _closing, _closed);
      if( _mode == SERVER_MODE ){
         synchronized( _closeLock ){
            if( !_closed && _closing ){
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
         _log.debug("Packet arrived: "+packetType ) ;
         switch(  packet.getType() ){
            case SshPacket.SSH_MSG_DISCONNECT :
                _log.debug("SSH_MSG_DISCONNECT: not implemented");
                return;
            case SshPacket.SSH_MSG_DEBUG : {
                    SshMsgDebug debug = new SshMsgDebug( packet ) ;
                    _log.debug("SSH_MSG_DEBUG: {}",  debug.getMessage());
                }
                break;
            case SshPacket.SSH_SMSG_PUBLIC_KEY :  {
                _log.debug("SSH_SMSG_PUBLIC_KEY received");

                SshSmsgPublicKey publicKey = new SshSmsgPublicKey( packet ) ;

                if( ! _clientAuth.isHostKey( _socket.getInetAddress() ,
                                              publicKey.getHostKey() ) ) {
                    throw new SshAuthenticationException("Unknown Host key");
                }
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
                for(int i = 0 ; i < 16 ; i++ ) {
                    remSessionKey[i] ^= sessionId[i];
                }

                byte [] encrypted =
                   publicKey.getHostKey().encrypt(
                     publicKey.getServerKey().encrypt( remSessionKey ) ) ;

                _log.debug("Sending SshCmsgSessionKey");

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
                   ( state != ST_TRY_RHOSTS_RSA_AUTH )     ) {
                   throw new
                           SshProtocolException(
                           "PANIC : SSH_SMSG_AUTH_RSA_CHALLENGE in state " + state);
               }

               SshSmsgAuthRsaChallenge challenge =
                          new SshSmsgAuthRsaChallenge( packet ) ;

               byte [] cBytes = challenge.getMpInt() ;

               cBytes = identity.decrypt( cBytes ) ;
               _log.debug("SSH_SMSG_AUTH_RSA_CHALLENGE received");
               try{
                  Md5 md5 = new Md5() ;
                  md5.update( cBytes ) ;
                  md5.update( sessionId ) ;
                  cBytes = md5.digest() ;
               }catch( Exception eee ){
                   _log.warn("Ignored exception: {}", eee.toString());
               }
               writePacket( new  SshCmsgAuthRsaResponse( cBytes ) ) ;
               state  = ST_RSA_RESPONSE ;
            }
            break;
            case SshPacket.SSH_SMSG_SUCCESS :   {
              switch( state ){
                 case ST_SESSION_SEND :
                   _log.debug("SSH_SMSG_SUCCESS on ST_SESSION_SEND");
                   writePacket( new SshCmsgUser( _clientAuth.getUser() ) ) ;
                   state = ST_USER_SEND ;
                   break;
                 case ST_RSA_RESPONSE :
                   _log.debug("SSH_SMSG_SUCCESS on ST_RSA_RESPONSE");
                   writePacket( new SshCmsgExecShell(  ) ) ;
                   state = ST_INTERACTIVE ;
                   return;
                 case ST_TRY_PASSWORD_AUTH :
                   _log.debug("SSH_SMSG_SUCCESS on ST_TRY_PASSWORD_AUTH");
                   writePacket( new SshCmsgExecShell(  ) ) ;
                   state = ST_INTERACTIVE ;
                   return;
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
                   _log.debug("SSH_SMSG_FAILURE on {}", state);
                   //
                   // start rsa authentication
                   //
                   SshAuthMethod method = _clientAuth.getAuthMethod() ;
                   if( method == null ) {
                       throw new SshAuthenticationException("No more methods from Client");
                   }

                   if( method instanceof SshAuthRsa ){
                       identity = method.getKey() ;
                       byte []   modulus  = identity.getModulusBytes() ;
                       int       bits     = identity.getModulusBits() ;
                       writePacket(
                              new SshCmsgAuthRsa( modulus , 0 , bits )
                                  ) ;
                       state  = ST_TRY_RSA_AUTH ;
                       _log.debug("Sending ST_TRY_RSA_AUTH");
                   }else if( method instanceof SshAuthRhostsRsa ){
                       identity = method.getKey() ;
                       writePacket(
                              new SshCmsgAuthRhostsRsa( method.getUser() , identity )
                                  ) ;
                       state  = ST_TRY_RHOSTS_RSA_AUTH ;
                       _log.debug("Sending ST_TRY_RSA_RHOSTS_AUTH");
                   }else if( method instanceof SshAuthPassword ){
                       writePacket(
                              new SshCmsgAuthPassword( method.getUser() )
                                  ) ;
                       _log.debug("Sending ST_TRY_PASSWORD_AUTH");
                       state  = ST_TRY_PASSWORD_AUTH ;
                   }else {
                       throw new SshProtocolException("Illegal class from Client");
                   }
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
                _log.warn("Unknown denied: "+packet.getType());
         }

      }
   }

   private void server_loop()
                throws   IOException
   {

      SshSmsgFailure bad = new SshSmsgFailure() ;
      SshSmsgSuccess ok  = new SshSmsgSuccess() ;
      SshPacket      packet ;
      byte       []  challenge = null  ;

      int       state    = ST_INIT ;

      while( ( packet = readPacket() ) != null  ){

         switch(  packet.getType() ){
            case SshPacket.SSH_MSG_DISCONNECT :
               _log.debug("SSH_MSG_DISCONNECT");
               return;
            case SshPacket.SSH_MSG_DEBUG : {
               SshMsgDebug debug = new SshMsgDebug( packet ) ;
               _log.debug("SSH_MSG_DEBUG: {}", debug.getMessage());
               }
               break;
            case SshPacket.SSH_CMSG_AUTH_RSA : {

               _log.debug("SSH_CMSG_AUTH_RSA");

               SshCmsgAuthRsa rsa = new SshCmsgAuthRsa( packet ) ;
               //
               // we have to ask the listener for the full key
               // of the modulus bytes.
               //
               SshRsaKey userKey = _serverAuth.authRsa( _remoteAddress ,
                                                        getName() ,
                                                        rsa.getKey() ) ;
               if( userKey == null ){
                  _log.warn("SSH_CMSG_AUTH_RSA: Key not found");
                  writePacket( bad ) ;
                  break ;
               }
               _log.debug("SSH_CMSG_AUTH_RSA: Key found");
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

               _log.debug("SSH_CMSG_AUTH_RHOSTS_RSA");

               SshCmsgAuthRhostsRsa rsa = new SshCmsgAuthRhostsRsa( packet ) ;
               //
               // we have to ask the listener for the full key
               // of the modulus bytes.
               //
               SshRsaKey hostKey = _serverAuth.authRhostsRsa( _remoteAddress ,
                                                              getName() ,
                                                              rsa.getUserName() ,
                                                              rsa.getKey()         ) ;
               if( hostKey == null ){
                  _log.warn("SSH_CMSG_AUTH_RSA: Key not found");
                  writePacket( bad ) ;
                  break ;
               }
               _log.debug("SSH_CMSG_AUTH_RSA: Key found {}", hostKey);
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

               _log.debug("SSH_CMSG_AUTH_RSA_RESPONSE");

               SshCmsgAuthRsaResponse rsaresp = new SshCmsgAuthRsaResponse( packet ) ;

               if( challenge == null ) {
                   throw new
                           SshProtocolException(
                           "SSH_CMSG_AUTH_RSA_RESPONSE challenge not found");
               }

               byte [] response = rsaresp.getResponse() ;
               //
               // we get the md5 sum of the challenge || sessionId
               //
               if( response.length != 16 ){
                  _log.warn("SSH_CMSG_AUTH_RSA_RESPONSE: response != 16 ({} bytes)", response.length);
                  writePacket( bad ) ;
                  break ;
               }
               Md5 digest = new Md5();
               digest.update(challenge);
               digest.update(_sessionId);
               byte [] res = digest.digest() ;

               int k ;
               for( k = 0 ;
                    ( k < res.length ) && ( res[k] == response[k] ) ; k++ ) {
               }
               if( k == 16 ){
                  _log.debug("SSH_CMSG_AUTH_RSA_RESPONSE: O.K." );
                  writePacket( ok ) ;
                  state  = ST_PREPARE ;
               }else{
                  _log.debug("SSH_CMSG_AUTH_RSA_RESPONSE: failed" );
                  writePacket( bad ) ;
               }
            }
            break ;
            case SshPacket.SSH_CMSG_SESSION_KEY :{

                _log.debug("SSH_CMSG_SESSION_KEY");

               if( state != ST_INIT ) {
                   throw new
                           SshProtocolException("SSH_CMSG_SESSION_KEY in not INIT state");
               }

               SshCmsgSessionKey session =
                  new SshCmsgSessionKey( _serverIdentity , _hostIdentity , packet ) ;

               byte [] sessionKey = session.getSessionKey() ;
               for( int i = 0 ; i < 16 ; i++ ) {
                   sessionKey[i] ^= _sessionId[i];
               }

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

               _log.debug("SSH_CMSG_USER: arrived");
               if( state != ST_USER ) {
                   throw new
                           SshProtocolException("SSH_CMSG_USER in not USER state");
               }

               SshCmsgUser user = new SshCmsgUser( packet ) ;

                _log.debug("SSH_CMSG_USER: User = {}", user.getUser());
                Principal principal = new UserNamePrincipal(user.getUser());
                _remoteUser = new Subject();
                _remoteUser.getPrincipals().add(principal);
               if( _serverAuth.authUser( _remoteAddress , getName() ) ){
                   state  = ST_PREPARE ;
                   writePacket( ok ) ;
               }else{
                   state  = ST_AUTH ;
                   writePacket( bad ) ;
               }
            }
            break ;
            case SshPacket.SSH_CMSG_AUTH_PASSWORD : {

               _log.debug("SSH_CMSG_AUTH_PASSWORD: arrived");
               if( state != ST_AUTH ) {
                   throw new
                           SshProtocolException("SSH_CMSG_AUTH_PASSWORD in not ST_AUTH state");
               }

                SshCmsgAuthPassword psw =
                     new SshCmsgAuthPassword( packet) ;

                if( _serverAuth.authPassword( _remoteAddress ,
                                              getName() ,
                                              psw.getPassword() ) ){
                   writePacket( ok ) ;
                   state    = ST_PREPARE ;
                }else{
                   writePacket( bad ) ;
                }
            }
            break ;
            case SshPacket.SSH_CMSG_REQUEST_PTY :
               if( state != ST_PREPARE ) {
                   throw new
                           SshProtocolException("SSH_CMSG_REQUEST_PTY in not ST_PREPARE state");
               }
               SshCmsgRequestPty requestPty = new SshCmsgRequestPty(packet);
               _terminal = requestPty.getTerminal();
               _height = requestPty.getHeight();
               _width = requestPty.getWidth();
               writePacket( ok ) ;
               _log.debug("SSH_CMSG_REQUEST_PTY: o.k.");
               break;
            case SshPacket.SSH_CMSG_WINDOW_SIZE :
               SshCmsgWindowSize requestWindowSize =
                   new SshCmsgWindowSize(packet);
               _height = requestWindowSize.getHeight();
               _width = requestWindowSize.getWidth();
               writePacket( ok ) ;
               _log.debug("SSH_CMSG_WINDOW_SIZE: o.k.");
               break;
            case SshPacket.SSH_CMSG_X11_REQUEST_FORWARDING :
               if( state != ST_PREPARE ) {
                   throw new
                           SshProtocolException("SSH_CMSG_X11_REQUEST_FORWARDING in not ST_PREPARE state");
               }
               writePacket( ok ) ;
               _log.debug("SSH_CMSG_REQUEST_FORWARDING: o.k.");
               break;
            case SshPacket.SSH_CMSG_EXEC_SHELL :
               if( state != ST_PREPARE ) {
                   throw new
                           SshProtocolException("SSH_CMSG_EXEC_SHELL in not ST_PREPARE state");
               }
               _log.debug("SSH_CMSG_EXEC_SHELL: o.k.");
               state = ST_INTERACTIVE ;
               return ;
            case SshPacket.SSH_CMSG_EXEC_CMD :
               if( state != ST_PREPARE ) {
                   throw new
                           SshProtocolException("SSH_CMSG_EXEC_CMD in not ST_PREPARE state");
               }
               _log.debug("SSH_CMSG_EXEC_CMD: {}", new SshCmsgExecCmd(packet));
               state = ST_INTERACTIVE ;
               return ;
            case SshPacket.SSH_CMSG_EXIT_CONFORMATION :
               _log.debug("SSH_CMSG_EXIT_CONFORMATION: o.k." );
               break ;
            case SshPacket.SSH_CMSG_STDIN_DATA :{

              throw new
              SshProtocolException( "SSH_CMSG_STDIN_DATA while preparing (FATAL)" ) ;


            }
            case SshPacket.SSH_CMSG_KEX_DH_GEX_REQUEST_OLD:
                writePacket(bad);
                break;
            default :
                _log.warn("Unknown denied : {}", packet.getType());
                if( state != ST_INTERACTIVE ) {
                    writePacket(bad);
                }
         }

      }

   }

}
