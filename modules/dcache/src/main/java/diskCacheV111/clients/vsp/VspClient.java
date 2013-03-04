// $Id: VspClient.java,v 1.6 2007-05-24 13:51:05 tigran Exp $
//
package diskCacheV111.clients.vsp ;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Hashtable;
import java.util.Vector;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.VspArgs;


public class      VspClient
       implements Runnable {

   private final Hashtable<Integer,VspBaseRequest> _requestHash = new Hashtable<>() ;
   private ServerSocket _listen;
   private Socket       _door;
   private BufferedReader _in;
   private PrintWriter    _out;
   private Thread         _commandThread;
   private Thread         _serviceThread;
   private boolean        _debug = true ;
   private String         _host;
   private int            _port;
   private int            _sessionId = 1 ;
   private boolean        _online;
   private synchronized int nextSessionId(){ return _sessionId++ ; }

   public VspClient( String host , int port )
          throws CacheException {

      try{
          _door = new Socket( host , port ) ;
          _in   = new BufferedReader(
                     new InputStreamReader(
                            _door.getInputStream() ) );
          _out  = new PrintWriter(
                     new OutputStreamWriter(
                            _door.getOutputStream() ) ) ;

           prepareService() ;
          _serviceThread = new Thread( this ) ;
          _serviceThread.start() ;
          _commandThread = new Thread( this ) ;
          _commandThread.start() ;
      }catch( CacheException ce ){
          throw ce ;
      }catch( Exception e ){
          throw new CacheException( 1 , e.toString() ) ;
      }

   }
   private void prepareService() throws Exception {
       _listen = new ServerSocket(0) ;
       _host   = _listen.getInetAddress().getHostAddress() ;
       _port   = _listen.getLocalPort() ;

   }
   @Override
   public void run(){
      if( Thread.currentThread() == _commandThread ){
         try{
            _out.println( "0 0 client hello 0 0 0 0" ) ;
            _out.flush() ;
            commandRun() ;
         }catch( Exception e ){

         }

      }else if( Thread.currentThread() == _serviceThread ){
         serviceRun() ;
      }
   }
   private void serviceRun(){
      try{

         Socket s;
         int sessionId = 0 ;
         DataInputStream data;
         while( ( s = _listen.accept() ) != null ){
            try{
                data = new DataInputStream( s.getInputStream() ) ;
                sessionId = data.readInt() ;
                VspBaseRequest request =
                        _requestHash.get(
                               Integer.valueOf( sessionId ) );

                if( request == null ){
                   System.err.println(
                      "Unexpected data connection for "+sessionId ) ;
                   try{ s.close() ; }catch(Exception xx){}

                }
                if(_debug) {
                    System.err.println("DataConnection for : " + sessionId);
                }
                request.dataConnectionArrived(s) ;
            }catch(Exception ie ){
                System.err.println(
                    "Exception for data : "+sessionId+
                    " : "+ie ) ;
                try{ s.close() ; }catch(Exception xx){}
            }

         }
      }catch(Exception e ){

      }
   }
   private void commandRun() throws Exception {
      String line;
      VspArgs args;
      VspBaseRequest request;
      int        sessionId;
      while( ( line = _in.readLine() ) != null ){
         args      = new VspArgs( line ) ;
         System.err.println( "Door : "+args ) ;
         sessionId = args.getSessionId() ;

         if( sessionId == 0 ){
            //
            //  master session
            //
            masterSession( args ) ;
         }else{
            request =
                    _requestHash.get(
                       Integer.valueOf(sessionId));
            if( request == null ){
               System.err.println(
                  "Unexpected session id : "+sessionId) ;
               continue ;
            }
            request.infoArrived( args ) ;
         }

      }
   }
   private synchronized void masterSession( VspArgs args )
   {
      if( args.getCommand().equals("welcome") ){
         _online =true ;
         notifyAll() ;
      }
   }
   private class VspPutRequest extends VspBaseRequest {
      private VspPutRequest( String pnfsId , File file ){
          super( pnfsId , file )  ;
          _out.println( ""+ getSessionId() +" 0 client put "+
                        pnfsId+" 0 "+_host+" "+_port ) ;
          _out.flush() ;
      }
      @Override
      public void runIo( DataInputStream x , DataOutputStream out )
             throws Exception {
         FileInputStream in = new FileInputStream(_file) ;
         try{
            byte [] data = new byte[8*1024] ;
            int size ;
            while(true){
               size =  in.read(data) ;
//               if(_debug)System.err.println("Transfer : "+size);
               if( size <= 0 ) {
                   break;
               }
               _dataOut.write(data,0,size) ;
            }
         }finally{
             try{ in.close() ; }catch(Exception eee ){}
         }
      }
   }
   private class VspGetRequest extends VspBaseRequest {
      private VspGetRequest( String pnfsId , File file ){
          super( pnfsId , file )  ;
          _out.println( ""+ getSessionId() +" 0 client get "+
                        pnfsId+" "+_host+" "+_port ) ;
          _out.flush() ;
      }
      @Override
      public void runIo( DataInputStream in , DataOutputStream controlOut )
             throws Exception {
         //
         // skip 0 byte
         //
         controlOut.writeLong(0) ;
         //
         // tell them we need it all
         //
         controlOut.writeLong(-1) ;
         //
         // and take it
         //
         FileOutputStream out = new FileOutputStream(_file) ;
         long total   = 0 ;
         long started = System.currentTimeMillis() ;
         try{
            byte [] data = new byte[8*1024] ;
            int size ;
            while(true){
               size =  in.read(data) ;
//               if(_debug)System.err.println("Transfer : "+size);
               if( size <= 0 ) {
                   break;
               }
               total += size ;
               out.write(data,0,size) ;
            }
         }finally{
             try{ out.close() ; }catch(Exception eee ){}
             long diff = System.currentTimeMillis() - started ;
             if( diff > 0 ){
                System.out.println( "Rate : "+
                  (   (((double)total)/((double)diff))/(1024.*1.024) ) ) ;
             }
         }
      }

   }
   private class VspBaseRequest implements VspRequest,Runnable {
      private int    _rc  = VspRequest.IN_PROCESS ;
      private String _msg = "in process" ;
      protected String _pnfsId;
      protected File   _file;
      protected DataOutputStream _dataOut;
      protected DataInputStream  _dataIn;
      private Socket             _socket;
      private boolean _ioFinished;
      private boolean _infoArrived;
      private int     _sessionId ;
      protected VspBaseRequest( String pnfsId , File file ){
          _pnfsId = pnfsId ;
          _file   = file ;
          _sessionId = nextSessionId() ;
      }
      int getSessionId(){ return _sessionId ; }
      public String toString(){
         return "["+_sessionId+"]("+_rc+") -> "+_msg ;
      }
      @Override
      public void run(){
         try{
            //
            // skip the challange
            //
            int skip = _dataIn.readInt() ;
            if(_debug) {
                System.err.println("Skipping " + skip);
            }
            _dataIn.skipBytes(skip);
            //
            // run the io
            //
            runIo( _dataIn , _dataOut ) ;
//            setResult(0,"o.k.");
         }catch(Exception e ){
           System.err.println("Exception in run : "+e ) ;
//           setResult(44,e.getMessage());
         }finally{
           try{ _dataOut.close() ; }catch(IOException ee ){}
           try{ _dataIn.close() ; }catch(IOException ee){}
           try{ _socket.close() ; }catch(IOException ee){}
         }
         synchronized( _requestHash ){
             _ioFinished = true ;
            whatNext() ;
         }
         if(_debug) {
             System.err.println("Session thread : " + _sessionId + " finished");
         }
      }
      private void whatNext(){
         if( _ioFinished && _infoArrived ) {
             _requestHash.remove(Integer.valueOf(_sessionId));
         }
        _requestHash.notifyAll() ;
      }
      public void runIo( DataInputStream in , DataOutputStream out )
             throws Exception {

      }
      private void infoArrived( VspArgs args )
      {
         int    rc  = 3 ;
         if( args.getCommand().equals("ok") ){
            setResult( rc = 0,"o.k.") ;
         }else{
            String msg = "Failed" ;
            if( args.argc() > 0 ){
               try{ rc = Integer.parseInt(args.argv(0)) ;
               }catch(Exception e){ rc = 44 ; }
               if( args.argc() > 1 ){
                   msg = args.argv(1) ;
               }
            }
            setResult( rc , msg ) ;
         }
         synchronized( _requestHash ){
            if( rc != 0 ) {
                _ioFinished = true;
            }
             _infoArrived = true ;
            whatNext() ;
         }
      }
      private void dataConnectionArrived( Socket s ) throws Exception {
         _socket  = s ;
         _dataIn  = new DataInputStream( s.getInputStream() ) ;
         _dataOut = new DataOutputStream( s.getOutputStream() ) ;
         if(_debug) {
             System.err.println("Starting io thread");
         }
         new Thread(this).start() ;
      }
      @Override
      public int getResultCode(){ return _rc ; }
      @Override
      public String getResultMessage(){ return _msg ; }
      private void setResult( int rc , String msg ){
        _rc = rc ;
        _msg = msg ;
      }
   }
   private void waitForOnline() throws CacheException {
       if(_debug) {
           System.err.println("Waiting for 'online'");
       }
       while( ! _online ){
          try{
             synchronized(this){wait() ;}
          }catch( InterruptedException ie ){
             throw new CacheException( "Wait for online interrupted" ) ;
          }
       }
       if( _debug) {
           System.err.println("Online .. ");
       }
   }
   public VspRequest putPnfs( String pnfsId , File localFile )
          throws CacheException {

       waitForOnline() ;

       if( ! localFile.exists() ) {
           throw new
                   CacheException(6, "Local File doesn't exists : " + localFile);
       }
        /*
        FileOutputStream s = null ;
        try{
           s = new FileOutputStream( localFile ) ;
        }catch(Exception ie ){
           throw new
           CacheException(5,"Can't create localfile : "+localFile ) ;
        }finally{
           try{ s.close() ; }catch(Exception ee){}
        }
        */
        VspBaseRequest vsp;
        synchronized( _requestHash ){
           vsp = new VspPutRequest( pnfsId , localFile ) ;
           _requestHash.put(vsp.getSessionId(), vsp ) ;
           _requestHash.notifyAll() ;
        }
        return vsp ;
   }
   public VspRequest getPnfs( String pnfsId , File localFile )
          throws CacheException {

       waitForOnline() ;

        VspBaseRequest vsp;
        synchronized( _requestHash ){
           vsp = new VspGetRequest( pnfsId , localFile ) ;
           _requestHash.put(vsp.getSessionId(), vsp ) ;
           _requestHash.notifyAll() ;
        }
        return vsp ;
   }
   public void waitForAll(){
       try{
          waitForOnline() ;
       }catch(Exception ee ){
          return ;
       }
       synchronized(_requestHash ){
          int count;
          while((count=_requestHash.size())>0){
             if(_debug) {
                 System.err.println("Still " + count + " requests");
             }
             try{
                _requestHash.wait() ;
             }catch(InterruptedException ie){
                break ;
             }

          }
       }
   }
   public static void main( String [] args ){

       if( ( args.length < 3 ) ||
           ! ( args[0].equals("put") ||
               args[0].equals("get") ||
               args[0].equals("none")     ) ){
          System.err.println( "Usage : ... put|get <pnfsId> <fileName>" ) ;
          System.exit(4);
       }
     try{
       VspClient vsp = new VspClient( "localhost" , 22125 ) ;

       int m = args.length;
       int count = 0 ;
       while(true){
       int n =  0 ;
       Vector<VspRequest> v = new Vector<>() ;
           label:
           while (true) {
               switch (args[n]) {
               case "put":
                   if ((m - n) < 3) {
                       System.err.println("Not enough arguments for 'put' ");
                       break label;
                   }
                   try {
                       v.addElement(vsp
                               .putPnfs(args[n + 1], new File(args[n + 2])));
                   } catch (Exception e) {
                       System.err
                               .println("Problem for " + args[n + 1] + " " + e);
                   }
                   n += 3;
                   break;
               case "get":
                   if ((m - n) < 3) {
                       System.err.println("Not enough arguments for 'get' ");
                       break label;
                   }
                   try {
                       v.addElement(vsp
                               .getPnfs(args[n + 1], new File(args[n + 2])));
                   } catch (Exception e) {
                       System.err
                               .println("Problem for " + args[n + 1] + " " + e);
                   }
                   n += 3;
                   break;
               case "none":
                   n += 1;
                   break;
               default:
                   System.err.println("Scanning stopped at : " + args[n]);
                   break label;
               }
               if (n >= m) {
                   break;
               }
           }

       vsp.waitForAll() ;
           for (Object o : v) {
               System.out.println(o.toString());
           }
       System.out.println("-------------> Count : "+(count++));
       }
//       System.exit(0);
     }catch(Exception e ){
        e.printStackTrace()  ;
        System.exit(4);
     }
   }
}
