// $Id: Ssh1DomainConnection.java,v 1.2 2006-11-19 09:14:19 patrick Exp $
//
package org.pcells.services.connection ;
//

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.Socket;

import dmg.protocols.ssh.SshAuthMethod;
import dmg.protocols.ssh.SshAuthPassword;
import dmg.protocols.ssh.SshAuthRsa;
import dmg.protocols.ssh.SshClientAuthentication;
import dmg.protocols.ssh.SshRsaKey;
import dmg.protocols.ssh.SshSharedKey;
import dmg.protocols.ssh.SshStreamEngine;
/**
 */
public class Ssh1DomainConnection
       extends DomainConnectionAdapter
       implements SshClientAuthentication {

   private String _hostname;
   private int    _portnumber;
   private Socket _socket;
   public SshAuthRsa _rsaAuth;
   public String     _password;
   public String     _loginName = "Unknown" ;

   public Ssh1DomainConnection( String hostname , int portnumber ){
      _hostname   = hostname ;
      _portnumber = portnumber ;
   }
   @Override
   public void go() throws Exception {

      _socket = new Socket( _hostname , _portnumber ) ;
      SshStreamEngine engine  = new SshStreamEngine( _socket , this ) ;

      setIoStreams( engine.getInputStream() ,
                    engine.getOutputStream(),
                    engine.getReader() ,
                    engine.getWriter() );

      try{
         super.go() ;
      }finally{
         try{ _socket.close() ; }catch(Exception ee ){}
      }

   }
   public void setLoginName( String name ){
      _loginName = name ;
   }
   public void setPassword( String password ){
      _password = password ;
   }
   public void setIdentityFile( File identityFile ) throws Exception {

       InputStream in  = new FileInputStream(identityFile) ;
       SshRsaKey   key = new SshRsaKey( in ) ;
       try{ in.close() ; }catch(Exception ee ){}

       _rsaAuth = new SshAuthRsa( key ) ;

   }
   ////////////////////////////////////////////////////////////////////////////////////////
   //
   //   Client Authentication interface
   //
   private int _requestCounter;
   @Override
   public boolean isHostKey( InetAddress host , SshRsaKey keyModulus ) {


 //      System.out.println( "Host key Fingerprint\n   -->"+
 //                      keyModulus.getFingerPrint()+"<--\n"   ) ;

 //     NOTE : this is correctly done in : import dmg.cells.applets.login.SshLoginPanel

       return true ;
   }
   @Override
   public String getUser( ){
      _requestCounter = 0 ;
      return _loginName ;
   }
   @Override
   public SshSharedKey  getSharedKey( InetAddress host ){
      return null ;
   }
   @Override
   public SshAuthMethod getAuthMethod(){

       SshAuthMethod result;
       if( _requestCounter++ == 0 ){
          if( _rsaAuth == null ) {
              result = new SshAuthPassword(_password);
          } else {
              result = _rsaAuth;
          }
       }else if( _requestCounter++ <= 2 ){
          result = new SshAuthPassword( _password ) ;
       }else{
          result = null;
       }
//       System.out.println("getAuthMethod("+_requestCounter+") "+result) ;
       return result ;
    }
   public static void main( String [] args )throws Exception {
      if( args.length < 2 ){

          System.err.println("Usage : <hostname> <portNumber>");
          System.exit(4);
      }
      String hostname = args[0] ;
      int portnumber  = Integer.parseInt( args[1] ) ;
      System.out.println("Creating new Raw...");
      Ssh1DomainConnection connection = new Ssh1DomainConnection( hostname , portnumber ) ;
      System.out.println("Starting Test");
      connection.test() ;


   }
   private class RunConnection
           implements Runnable, DomainConnectionListener, DomainEventListener {


      public RunConnection(  ) throws Exception {
         System.out.println("class runConnection init");
         addDomainEventListener(this);
         setLoginName("admin");
         setIdentityFile( new File(System.getProperty("user.home") + "/.ssh/identity" ) );
         setPassword("dickerelch");
         new Thread(this).start() ;
      }
      @Override
      public void run(){
         try{
            go() ;
         }catch(Exception ee ){
            System.out.println("RunConnection got : "+ee);
            ee.printStackTrace();
         }
      }
      @Override
      public void domainAnswerArrived( Object obj , int id ){
          System.out.println("Answer : "+obj);
          if( id == 54 ){
             try{
                sendObject(  "logoff" , this , 55 ) ;
             }catch(Exception ee ){
                System.out.println("Exception in sendObject"+ee);
             }
          }
      }
      @Override
      public void connectionOpened( DomainConnection connection ){
         System.out.println("DomainConnection : connectionOpened");
         try{
            sendObject( "System" , "ps -f" , this , 54 ) ;
         }catch(Exception ee ){
            System.out.println("Exception in sendObject"+ee);
         }
      }
      @Override
      public void connectionClosed( DomainConnection connection ){
         System.out.println("DomainConnection : connectionClosed");
      }
      @Override
      public void connectionOutOfBand( DomainConnection connection ,
                                       Object subject                ){
         System.out.println("DomainConnection : connectionOutOfBand");
      }
   }
   public void test() throws Exception {
      System.out.println("Starting test");
      new RunConnection() ;
   }

}
