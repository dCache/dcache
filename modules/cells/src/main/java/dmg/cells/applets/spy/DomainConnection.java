package dmg.cells.applets.spy ;

import java.util.* ;
import java.io.* ;
import java.net.* ;

import dmg.util.* ;
import dmg.cells.services.* ;
import dmg.cells.nucleus.* ;

public class DomainConnection implements Runnable {
   private Socket    _socket;
   private Hashtable _hash;
   private int       _counter;
   private ObjectOutputStream _out;
   private ObjectInputStream  _in;
   private Thread             _listen ;
   private String             _host;
   private int                _port;
   private DomainConnectionListener _listener;
   private boolean  _inUse, _ok;

   public DomainConnection( String host , int port ) {
       _host = host ;
       _port = port ;
   }
   public void addConnectionListener( DomainConnectionListener listener ){
       _listener = listener ;
   }
   public void connect() throws Exception {

      synchronized( this ){
          if( _inUse ) {
              throw new Exception("Still In Use");
          }
          _inUse = true ;
      }

      _listen  = new Thread( this ) ;
      _listen.start() ;
   }
   @Override
   public void run(){
      try{
         _socket  = new Socket( _host , _port ) ;
         _out     = new ObjectOutputStream( _socket.getOutputStream() ) ;
         _in      = new ObjectInputStream( _socket.getInputStream() ) ;
      }catch( Exception e ){
         if( _listener != null ) {
             _listener.connectionFailed(
                     new DomainConnectionEvent(this, "Problem : " + e
                             .toString()));
         }
         synchronized( this ){
           _inUse = false ;
           return ;
         }
      }
      _hash = new Hashtable() ;
      _counter = 0 ;
      synchronized( this ){ _ok = true ; }
      if( _listener != null ) {
          _listener.connectionActivated(new DomainConnectionEvent(this));
      }
      try{
         while( true ){
             Object o = _in.readObject() ;
             if( ( o == null ) ||
                ! ( o instanceof MessageObjectFrame ) ) {
                 throw new Exception("Protocol violation");
             }

             MessageObjectFrame frame = (MessageObjectFrame)o ;
             FrameArrivable client = (FrameArrivable)_hash.get( frame ) ;
             if( client == null ){
                System.err.println( "Client not found" ) ;
                continue ;
             }
             client.frameArrived( frame ) ;
         }
      }catch( Exception e ){
//         System.out.println( "Exception in run "+e ) ;
         synchronized( this ){ _ok = false ; }
//         System.out.println( "Calling listener") ;
         if( _listener != null ) {
             _listener.connectionDeactivated(
                     new DomainConnectionEvent(this, "Connection Closed : " + e
                             .toString()));
         }
//         System.out.println( "Connection closed by foreign host "+e ) ;
         try{ _out.close() ; }catch(IOException ee){}
          try{ _in.close() ; }catch(IOException ee){}
          try{ _socket.close() ; }catch(IOException ee){}
          synchronized( this ){ _inUse = false ; }
     }

   }
   public void send( String cell , Serializable obj , FrameArrivable client ){

      synchronized( this ){ if( ! _ok ) {
          return;
      }
      }

      try{
         int next = _counter++ ;
         MessageObjectFrame frame =
            new MessageObjectFrame( next , new CellPath( cell ) , obj ) ;
         _hash.put( frame , client ) ;
         _out.writeObject( frame ) ;
         _out.flush() ;
         _out.reset() ;
      }catch( Exception e ){
         System.err.println( "Send Problem : "+e ) ;
      }
   }
   public void close(){
//      System.out.println( "Close called" ) ;
      synchronized( this ){ if( ! _ok ) {
          return;
      }
      }
//      System.out.println( "Close starting" ) ;
//      try{ _socket.close() ; }catch(Exception e){} ;
      try{
         _out.writeObject( "BYE BYE" ) ;
         _out.flush() ;
         _out.reset() ;
      }catch( Exception e ){
      }
//      System.out.println( "Close Done" ) ;
   }

}

