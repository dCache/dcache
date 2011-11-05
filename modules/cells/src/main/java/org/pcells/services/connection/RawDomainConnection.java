// $Id: RawDomainConnection.java,v 1.2 2006-11-19 09:14:19 patrick Exp $
//
package org.pcells.services.connection ;
//
import java.util.*;
import java.io.* ;
import java.net.* ;
import org.pcells.services.connection.DomainConnection ;
import org.pcells.services.connection.DomainConnectionListener ;
import org.pcells.services.connection.DomainEventListener ;
import dmg.cells.applets.login.DomainObjectFrame ;
/**
 */
public class RawDomainConnection extends DomainConnectionAdapter {

   private String _hostname   = null ;
   private int    _portnumber = 0 ;
   private Socket _socket     = null ;

   public RawDomainConnection( String hostname , int portnumber ){
      _hostname   = hostname ;
      _portnumber = portnumber ;
   }
   public void go() throws Exception {
   
      _socket = new Socket( _hostname , _portnumber ) ;
      setIoStreams( _socket.getInputStream() , _socket.getOutputStream() ) ;
      
      try{
         super.go() ;
      }finally{
         try{ _socket.close() ; }catch(Exception ee ){}
      }
      
   }
   private class RunConnection 
           implements Runnable, DomainConnectionListener, DomainEventListener {
      
        
      public RunConnection(  ) throws Exception {
         System.out.println("class runConnection init");
         addDomainEventListener(this);
         new Thread(this).start() ;
      }
      public void run(){
         try{
            go() ;
         }catch(Exception ee ){
            System.out.println("RunConnection got : "+ee);
            ee.printStackTrace();
         }
      }
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
      public void connectionOpened( DomainConnection connection ){
         System.out.println("DomainConnection : connectionOpened");
         try{
            sendObject( "System" , "ps -f" , this , 54 ) ;
         }catch(Exception ee ){
            System.out.println("Exception in sendObject"+ee);
         }
      }
      public void connectionClosed( DomainConnection connection ){
         System.out.println("DomainConnection : connectionClosed");
      }
      public void connectionOutOfBand( DomainConnection connection ,
                                       Object subject                ){
         System.out.println("DomainConnection : connectionOutOfBand");
      }
   }
   public void test() throws Exception {
      System.out.println("Starting test");
      new RunConnection() ;
   }
   public static void main( String [] args )throws Exception {
      if( args.length < 2 ){
      
          System.err.println("Usage : <hostname> <portNumber>");
          System.exit(4);
      }
      String hostname = args[0] ;
      int portnumber  = Integer.parseInt( args[1] ) ;
      System.out.println("Creating new Raw...");
      RawDomainConnection connection = new RawDomainConnection( hostname , portnumber ) ;
      System.out.println("Starting Test");
      connection.test() ;


   }
}
