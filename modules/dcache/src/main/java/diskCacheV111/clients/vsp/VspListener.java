package diskCacheV111.clients.vsp ;


import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Enumeration;
import java.util.Hashtable;

import org.dcache.util.Args;

public class VspListener implements Runnable {
   public static final int IOCMD_WRITE     = 1 ;
   public static final int IOCMD_READ      = 2 ;
   public static final int IOCMD_SEEK      = 3 ;
   public static final int IOCMD_CLOSE     = 4 ;
   public static final int IOCMD_INTERRUPT = 5 ;
   public static final int IOCMD_ACK       = 6 ;
   public static final int IOCMD_FIN       = 7 ;
   public static final int IOCMD_DATA      = 8 ;
   public static final int IOCMD_LOCATE    = 9 ;
   private static final int IOCMD_SEEK_SET      = 0 ;
   private static final int IOCMD_SEEK_CURRENT  = 1 ;
   private static final int IOCMD_SEEK_END      = 2 ;
   private ServerSocket _listen;
   private Thread _acceptThread;
   private int    _counter      = 1 ;
   private Hashtable<Integer,IoChannel> _hash = new Hashtable<>();

   private String [] _commands = {
      "Unkown" ,
      "IOCMD_WRITE" ,
      "IOCMD_READ" ,
      "IOCMD_SEEK" ,
      "IOCMD_CLOSE" ,
      "IOCMD_INTERRUPT" ,
      "IOCMD_ACK" ,
      "IOCMD_FIN" ,
      "IOCMD_DATA" ,
      "IOCMD_LOCATE"
   } ;
   private String iocmdToString(int iocmd ){
      if( ( iocmd <= 0 ) || ( iocmd > 9 ) ) {
          return "Unkown";
      }
      return _commands[iocmd] ;
   }
   private class IoChannel implements Runnable {
       private int _session;
       private Socket _socket;
       private DataInputStream _dataIn;
       private DataOutputStream _dataOut;
       private Thread _worker;
       private IoChannel( Socket socket , int session ) throws IOException {
          _session = session ;
          _socket = socket ;
          try{
             _dataIn = new DataInputStream( _socket.getInputStream() ) ;
             _dataOut = new DataOutputStream( _socket.getOutputStream() ) ;
             say("");
             int  remoteSession = _dataIn.readInt() ;
             say( "Remote session is "+remoteSession ) ;
             int chSize = _dataIn.readInt() ;
             say( "Challenge Size is "+chSize ) ;
             _dataIn.skipBytes(chSize) ;
          }catch(IOException e){
             System.err.println( "["+_session+"] : exc : "+e ) ;
             try{ _socket.close() ; }catch(IOException ii ){}
             throw e ;
          }
          _worker = new Thread( this ) ;
          _worker.start() ;
       }
       @Override
       public void run(){
        try{
          while(true){
             int len;
             try{
                 len = _dataIn.readInt() ;
             }catch(EOFException eofe){
                 say( "connection closed" ) ;
                 throw eofe ;
             }
             if( len < 4 ) {
                 throw new Exception("Protocol violation (len)");
             }
             int command = _dataIn.readInt() ;
             int rc , iocmd ;
             switch( command ){
                case IOCMD_DATA :   // DATA
                   say( "" ) ;
                   say( "{datatransfermode}" ) ;
                   while( true ) {
                      int datalen = _dataIn.readInt() ;
                      if( datalen < 0 ){
                         say( "Data transfer delimiter" ) ;
                         break ;
                      }
                      say( "Reading block of "+datalen ) ;
                      if( datalen == 0 ) {
                          continue;
                      }
                      _dataIn.skipBytes(datalen) ;
                   }
                   say( "{commandmode}" ) ;
                break ;
                case IOCMD_ACK :   // ACK
                case IOCMD_FIN :   // FIN
                   if( len < 12 ) {
                       throw new Exception("Protocol violation (len2)");
                   }
                   say( "" ) ;

                   iocmd = _dataIn.readInt() ;
                   rc    = _dataIn.readInt() ;

                   StringBuilder sb = new StringBuilder() ;
                   sb.append( "{REQUEST_")
                     .append(command==IOCMD_ACK?"ACK":"FIN")
                     .append(" iocmd=") ;
                   sb.append(iocmdToString(iocmd))
                     .append(" rc=")
                     .append(rc)
                     .append("} ") ;
                   len -= 12 ;
                   if( rc == 0 ){
                      for( int i = 0 ; len >= 8 ; i++ , len -= 8 ){
                        long arg = _dataIn.readLong() ;
                        sb.append(";a[").append(i).append("]=").append(arg);
                      }
                      sb.append( ";\n" ) ;
                   }else{
                      //
                      // the following code doesn't support
                      // character encoding above 0x7f.
                      // ( it will run out of sync.
                      //
                      String str = _dataIn.readUTF() ;
                      len -= ( str.length() + 2 ) ;
                      sb.append(";ErrorMessage='").append(str).append("'");
                   }
//                   System.out.println( "Skipping : "+len ) ;
                   say( sb.toString() ) ;
                   _dataIn.skipBytes(len) ;
                break ;
                default :
                   say( "Unknown command : "+command ) ;

             }
          }
        }catch(Exception e ){
          try{ _socket.close() ; }catch(IOException ii ){}
          _hash.remove( Integer.valueOf(_session) ) ;
        }
       }
       private void say( String s ){
          System.out.println( "["+_session+"] > "+s ) ;
       }
       public String toString(){
          return " session="+_session ;
       }
       public void seek( long offset , int whence ) throws IOException {
          _dataOut.writeInt(16) ;
          _dataOut.writeInt( IOCMD_SEEK ) ;
          _dataOut.writeLong( offset ) ;
          _dataOut.writeInt(whence) ;
          _dataOut.flush() ;

       }
       public void close() throws Exception {
          _dataOut.writeInt(4) ;
          _dataOut.writeInt(IOCMD_CLOSE) ;
          _dataOut.flush() ;
       }
       public void write() throws Exception {
          _dataOut.writeInt(4) ;
          _dataOut.writeInt(IOCMD_WRITE) ;
          _dataOut.flush() ;
       }
       public void locate() throws Exception {
          _dataOut.writeInt(4) ;
          _dataOut.writeInt(IOCMD_LOCATE) ;
          _dataOut.flush() ;
       }
       public void read( long size ) throws Exception {
          _dataOut.writeInt(12) ;
          _dataOut.writeInt(IOCMD_READ) ;
          _dataOut.writeLong(size) ;
          _dataOut.flush() ;
       }
       public void data( int size ) throws Exception {
          int full = size/1024 ;
          int res  = size%1024 ;
          _dataOut.writeInt(4);
          _dataOut.writeInt(IOCMD_DATA) ;
          byte [] data = new byte[1024] ;
          for( int i = 0 ; i < full ; i++ ){
             _dataOut.writeInt(1024) ;
             _dataOut.write( data ) ;
          }
          if( res > 0 ){
             _dataOut.writeInt( res ) ;
             _dataOut.write( data , 0 , res ) ;
          }
          _dataOut.writeInt(-1);
          _dataOut.flush() ;
       }
   }
   public VspListener() throws Exception {
      _listen = new ServerSocket(0) ;
      _acceptThread = new Thread( this ) ;
      _acceptThread.start() ;
   }
   public void seek( int session , long offset , int whence ) throws Exception {

      IoChannel io = _hash.get( Integer.valueOf(session) );
      if( io == null ) {
          throw new
                  Exception("Session not found : " + session);
      }

      io.seek( offset , whence ) ;

   }
   public void close( int session ) throws Exception {

      IoChannel io = _hash.get( Integer.valueOf(session) );
      if( io == null ) {
          throw new
                  Exception("Session not found : " + session);
      }

      io.close() ;

   }
   public void locate( int session ) throws Exception {

      IoChannel io = _hash.get( Integer.valueOf(session) );
      if( io == null ) {
          throw new
                  Exception("Session not found : " + session);
      }

      io.locate() ;

   }
   public void write( int session ) throws Exception {

      IoChannel io = _hash.get( Integer.valueOf(session) );
      if( io == null ) {
          throw new
                  Exception("Session not found : " + session);
      }

      io.write() ;

   }
   public void read( int session , long size ) throws Exception {

      IoChannel io = _hash.get( Integer.valueOf(session) );
      if( io == null ) {
          throw new
                  Exception("Session not found : " + session);
      }

      io.read( size ) ;

   }
   public void data( int session , int size ) throws Exception {

      IoChannel io = _hash.get( Integer.valueOf(session) );
      if( io == null ) {
          throw new
                  Exception("Session not found : " + session);
      }

      io.data( size ) ;

   }
   public Enumeration<IoChannel> elements(){ return _hash.elements() ; }
   @Override
   public void run(){
      if( _acceptThread == Thread.currentThread() ){
       try{
         while(true){
           synchronized( this ){
               Socket socket  = _listen.accept() ;
               _counter ++ ;
               System.out.println( "[m] Connection accepted : ["+_counter+"]" ) ;
               try {
                   _hash.put(_counter, new IoChannel(socket, _counter));
               } catch (IOException e) {
               }
           }
         }
        }catch(IOException ioe ){
            System.err.println( "Error in accept ... " ) ;
            System.exit(4);
        }
      }
   }
   public synchronized int getCounter(){ return _counter ; }
   public int getListenPort(){ return _listen.getLocalPort() ; }
   public static void main( String [] xargs ){
      try{
         VspListener vsp = new VspListener() ;
         System.out.println( "Listen Port : "+vsp.getListenPort() ) ;

         BufferedReader br = new BufferedReader(
                              new InputStreamReader( System.in) ) ;

         String line;
         int session = 2 ;
         while( true ){
             System.out.print( "["+session+"] < " ) ;
             line = br.readLine() ;
             if( line == null ) {
                 break;
             }
             Args args = new Args( line ) ;
             if( args.argc() > 0 ){
                 String command = args.argv(0) ;
                 args.shift() ;
                 switch (command) {
                 case "cd":
                     if (args.argc() < 1) {
                         System.err.println("Syntax Error");
                         continue;
                     }
                     try {
                         session = Integer.parseInt(args.argv(0));
                     } catch (Exception e) {
                         System.err.println("Syntax Error");
                     }
                     break;
                 case "ls":
                     Enumeration<IoChannel> e = vsp.elements();
                     while (e.hasMoreElements()) {
                         System.out.println(e.nextElement().toString());
                     }
                     break;
                 case "read":
                     if (args.argc() < 1) {
                         System.err.println("Syntax Error");
                         continue;
                     }
                     try {
                         long offset = Long.parseLong(args.argv(0));
                         vsp.read(session, offset);
                     } catch (Exception ee) {
                         System.err.println("Error " + ee);
                     }
                     break;
                 case "data":
                     if (args.argc() < 1) {
                         System.err.println("Syntax Error");
                         continue;
                     }
                     try {
                         int offset = Integer.parseInt(args.argv(0));
                         vsp.data(session, offset);
                     } catch (Exception ee) {
                         System.err.println("Error " + ee);
                     }
                     break;
                 case "write":
                     if (args.argc() < 0) {
                         System.err.println("Syntax Error");
                         continue;
                     }
                     try {
                         vsp.write(session);
                     } catch (Exception ee) {
                         System.err.println("Error " + ee);
                     }
                     break;
                 case "locate":
                     if (args.argc() < 0) {
                         System.err.println("Syntax Error");
                         continue;
                     }
                     try {
                         vsp.locate(session);
                     } catch (Exception ee) {
                         System.err.println("Error " + ee);
                     }
                     break;
                 case "close":
                     if (args.argc() < 0) {
                         System.err.println("Syntax Error");
                         continue;
                     }
                     try {
                         vsp.close(session);
                     } catch (Exception ee) {
                         System.err.println("Error " + ee);
                     }
                     break;
                 case "seek":
                     if (args.argc() < 2) {
                         System.err.println("Syntax Error");
                         continue;
                     }
                     try {
                         long offset = Long.parseLong(args.argv(0));
                         int whence = Integer.parseInt(args.argv(1));
                         vsp.seek(session, offset, whence);
                     } catch (Exception ee) {
                         System.err.println("Error " + ee);
                     }
                     break;
                 case "help":
                     System.out.println("[0] > ls");
                     System.out.println("[0] > cd <session>");
                     System.out.println("[0] > exit");
                     System.out.println("[0] > close");
                     System.out.println("[0] > write");
                     System.out.println("[0] > data <bytes>");
                     System.out.println("[0] > read <bytes>");
                     System.out.println("[0] > seek <offset> <whence=0,1,2>");
                     break;
                 case "exit":
                     System.exit(4);
                 default:
                     System.out.println("What ? ");
                     break;
                 }
             }
         }
         System.out.println("");
      }catch(Exception ee ){
         ee.printStackTrace() ;
      }
      System.exit(0);

   }

}
