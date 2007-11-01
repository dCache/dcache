package dmg.apps.psdl.clients ;

import dmg.apps.psdl.vehicles.* ;
import dmg.apps.psdl.pnfs.* ;

import java.io.* ;
import java.util.* ;
import java.net.* ;

public class PsdlClient implements Runnable {

   private Hashtable    _threadHash      = new Hashtable() ;
   private Hashtable    _requestHash     = new Hashtable() ;
   private Hashtable    _finishedHash    = new Hashtable() ;
   private ServerSocket _listenSocket    = null ;
   private int          _listenPort      = 0 ;
   private Thread       _listenThread    = null ;
   private String       _hostName        = null ;
   private Socket       _commandSocket   = null ;
   private Object       _finishedLock    = new Object() ;
   private boolean      _sendingFinished = false ;
   private ObjectInputStream  _commandIn     = null ;
   private ObjectOutputStream _commandOut    = null ;
   private Thread             _commandThread = null ;
   //
   //   internal classes ( this class keeps track of the requests ) 
   //
   private class PutRequest extends IORequest {
      public PutRequest( PnfsFile fromFile ,
                         PnfsFile toDirectory ,
                         PnfsFile toFile ,
                         PsdlPutRequest psdlRequest ){
         super( fromFile , toDirectory , toFile , psdlRequest ) ;
      }
      public PsdlPutRequest  getRequest(){ return (PsdlPutRequest)getCoreRequest() ; }
   }
   /*
   private class GetRequest extends IORequest {
      public GetRequest( PnfsFile fromFile ,
                         PnfsFile toDirectory ,
                         PnfsFile toFile ,
                         PsdlRequest psdlRequest ){
         super( fromFile , toDirectory , toFile , psdlRequest ) ;
      }
   }
   */
   private class IORequest {
   
      private PnfsFile         _fromFile , _toDirectory , _toFile ;
      private PsdlCoreRequest  _psdlRequest ;
      private DataInputStream  _in ;
      private DataOutputStream _out ;
      private Socket           _socket ;
      private boolean          _ready ;
      
      public IORequest(  PnfsFile fromFile ,
                         PnfsFile toDirectory ,
                         PnfsFile toFile ,
                         PsdlCoreRequest psdlRequest ){
                         
         _fromFile    = fromFile ;
         _toDirectory = toDirectory ;
         _toFile      = toFile ;
         _psdlRequest = psdlRequest ;
      }
      public PnfsFile         getSourceFile(){   return _fromFile ; }
      public PnfsFile         getDestFile(){     return _toFile ; }
      public PsdlCoreRequest  getCoreRequest(){  return _psdlRequest ; }
      public DataInputStream  getInputStream(){  return _in ; }
      public DataOutputStream getOutputStream(){ return _out ; }
      public Socket           getSocket(){       return _socket ; }
      public boolean          isReady(){         return _ready ; }
      
      public void setIo( Socket s , DataInputStream in , DataOutputStream out ){
         _socket = s ;_in = in ; _out = out ;
      }
      public void setRequest( PsdlCoreRequest psdl ){
         _ready       = true ;
         _psdlRequest = psdl ;
      }
      public String toString(){
         return  _psdlRequest.toString() ;
      }
   }

   public synchronized void waitForFinished( ){
      noMoreRequests( true ) ;
   }
   public synchronized void noMoreRequests( boolean doWait ){
      synchronized( _finishedLock ){
          _sendingFinished = true ;
          try{
             _finishedLock.wait() ;
          }catch( InterruptedException ie ){}
      }
//      System.err.println( "All Requests are answered; sending exit" ) ;
      try{
           _commandOut.writeObject( "exit" ) ;
      }catch( IOException ioe ){
         System.err.println( "Problems closing control path : "+ioe ) ;
      } 
      Enumeration e = _finishedHash.elements() ;
      for( ; e.hasMoreElements() ; ){
         IORequest req = (IORequest) e.nextElement() ;
         System.out.println( req.toString() ) ;
      }
         
   }

   public PsdlClient( String serverHost , int serverPort ) throws IOException {
   
      try{
         _commandSocket = new Socket( serverHost , serverPort ) ;
         _commandOut    = new ObjectOutputStream( 
                                _commandSocket.getOutputStream() ) ;
         _commandIn     = new ObjectInputStream( 
                                _commandSocket.getInputStream() ) ;
         _commandThread = new Thread( this ) ;
         _commandThread.start();
                            
      }catch( IOException e ){
         System.err.println( "Problem in contacting "+serverHost+":"+serverPort ) ;
         throw e ;
      }
      startListener() ;
      _listenThread = new Thread( this ) ;
      _listenThread.setDaemon( true ) ;
      _listenThread.start() ;
   }
   public synchronized 
       void putRequest(  PnfsFile fromFile ,
                         PnfsFile toDirectory ,
                         String toName             ) throws IOException {
   
      long size = 0 ;
      try{
          if( fromFile.canRead() )size = fromFile.length() ;
      }catch( Exception e ){
          size = 0 ;
      }
      //
      // create the pnfs entry
      //
      PnfsFile pnfsFile = new PnfsFile( toDirectory , toName ) ;
      try{
         FileOutputStream out =  new FileOutputStream( pnfsFile ) ;
         out.close() ;
      }catch( IOException ioe ){
         System.err.println( "Sorry, can't create pnfs entry : "+
                             pnfsFile+" "+ioe );
         throw ioe ;
      }
      PnfsId id = pnfsFile.getPnfsId() ;
      if( id == null ){
          pnfsFile.delete() ;
          System.err.println( "Problem extracting pfns Id of : "+pnfsFile);
          throw new IOException ( "Pnfs : can't get id of : "+pnfsFile ) ;
      }
           
      PsdlPutRequest r = new PsdlPutRequest( _hostName , 
                                             _listenPort ,
                                             id ,  
                                             toDirectory.getPnfsId() ,  
                                             size         ) ;
                                       
      _requestHash.put( r.getId() , 
                        new PutRequest( fromFile ,
                                        toDirectory ,
                                        pnfsFile  ,     
                                        r              ) ) ;
      
      try{
          _commandOut.writeObject( r ) ;
      }catch( Exception ee ){
          _requestHash.remove( r.getId() ) ;
          pnfsFile.delete() ;
      }
      
     
   }
   private boolean sendRequest( PsdlCoreRequest req ){
      try{
          _commandOut.writeObject( req ) ;
          return true ;
      }catch( Exception ioe ){
          System.out.println( "Couldn't send request "+req+
                              "; due to exc : "+ioe ) ;
          return false ;                
      }
   }
   private void startListener() throws IOException {
      _listenSocket = new ServerSocket(_listenPort) ;
      _listenPort   = _listenSocket.getLocalPort() ;
      _hostName     = InetAddress.getLocalHost().getHostName() ;
   }
   private void runPsdlRequest( PsdlCoreRequest req ){
      System.out.println( "Request arrived : "+req ) ;
      IORequest ior = (IORequest)_requestHash.remove( req.getId() ) ;
      if( ior == null ){
        System.err.println( "Request not found in table : "+req ) ;
        return ;
      }
      ior.setRequest( req ) ;
      if( req.getReturnCode() != 0 ){
         System.err.println( "Request failed , deleting destination file" ) ;
         ior.getDestFile().delete() ;
      }else{
         System.err.println( "Request ok , size = "+ior.getDestFile().length() ) ;
      }
      _finishedHash.put( req.getId() , ior ) ;
      synchronized( _finishedLock ){
         if( _sendingFinished && ( _requestHash.size() == 0 ) )
             _finishedLock.notifyAll() ;
      }
   }
   public void run(){
    
      IORequest req = null ;
      
      if( Thread.currentThread() == _commandThread ){
          Object obj ;
          try{
             while( ( obj = _commandIn.readObject() ) != null ){
                if( obj instanceof PsdlCoreRequest )
                    runPsdlRequest( (PsdlCoreRequest) obj ) ;
                else
                   System.err.println( "Unknown object received : "+
                                       obj.getClass().getName() ) ;
             }
          }catch( EOFException ioe ){
             System.out.println( "Command channel closed" ) ;
          }catch( Exception ioe ){
             ioe.printStackTrace() ;
          }
          try{ _commandSocket.close() ; }catch(Exception eee ){}
          
      }else if( Thread.currentThread() == _listenThread ){
          while( true ){
            Socket s = null ;
            try{
            
               s = _listenSocket.accept() ;
               System.err.println( "Connection from "+s ) ;
               DataOutputStream out = 
                 new DataOutputStream( s.getOutputStream() ) ;
               DataInputStream in = 
                 new DataInputStream( s.getInputStream() ) ;
               Long id = new Long( in.readLong() ) ;
               System.err.println( "Receive id "+id ) ;
               req = (IORequest)_requestHash.get( id ) ;
               if( req == null ){
                   System.out.println( "requested id not found in table" ) ;
                   continue ;
               }
               req.setIo( s , in , out ) ;
               Thread t = new Thread( this ) ;
               _threadHash.put( t , req ) ;
               t.start() ;
            }catch( IOException ioe ){
               System.out.println( "except loop : "+ioe ) ;
               try{ s.close() ; }catch(Exception ee ){}
            }
          }
      }else if( ( req = (IORequest)_threadHash.remove( 
                         Thread.currentThread() ) 
                ) != null ){
                
          if( req instanceof PutRequest ){
             runPutMover( (PutRequest) req ) ;
//          }else if( req instanceof GetRequest ){
//             runGetMover( (GetRequest) req ) ;
          }
         
      }
      
   }
   private void runPutMover( PutRequest req ){
      DataInputStream  netIn   = req.getInputStream() ;
      DataOutputStream netOut  = req.getOutputStream() ;
      Socket socket = req.getSocket() ;
      
      System.err.println( "Connected by server (put)" ) ;
      
      File f = req.getSourceFile()  ;
      if( ! f.canRead() ){
         try{
            System.err.println( "Can't read "+f ) ;
            socket.close() ;
         }catch(Exception e ){}
         return ;
      }
      byte [] buffer = new byte[32*1024] ;
      //
      //   protocol version 0.1
      //    long | size of data following  |
      //    byte | .... data ... |
      //    long | zero                    |
      FileInputStream fileIn = null ;
      try{
         fileIn = new FileInputStream( f ) ;
         long total = f.length() ;
         netOut.writeLong( total ) ; netOut.flush() ;
         int n ;
         System.err.println( "Starting data transfer" ) ;
         long start = System.currentTimeMillis() ;
         for( long sum = 0 ; true; ){
             n = fileIn.read( buffer , 0 , buffer.length ) ;
             if( n < 0 )break ;
             sum += n ;
             netOut.write( buffer , 0 , n ) ;
         }
         netOut.writeLong( 0L ) ; netOut.flush() ;
         long finished = System.currentTimeMillis() ;
         double res = (( double) total) /( ( double ) (finished-start) ) ;
         System.err.println( "Tranfer finished : "+( res / 1024.0 )+" MB/sec" ) ;
      }catch( IOException ioe ){
         ioe.printStackTrace() ;
      } 
      try{ fileIn.close() ;}catch(Exception e ){}
      try{ socket.close() ;}catch(Exception e ){}
   }
   public void putRequestx( PnfsFile from , PnfsFile toDir , String toFileName){
      System.out.println( " Direction : Put" ) ;
      System.out.println( " FromFile  : "+from.toString() ) ;
      System.out.println( " ToDir     : "+toDir.toString() ) ;
      System.out.println( " ToFile    : "+toFileName ) ;
   }
   public void getRequest( PnfsFile from , PnfsFile toDir , String toFileName){
      System.out.println( " Direction : Get" ) ;
      System.out.println( " FromFile  : "+from.toString() ) ;
      System.out.println( " ToDir     : "+toDir.toString() ) ;
      System.out.println( " ToFile    : "+toFileName ) ;
   
   }

}
 
