package diskCacheV111.util ;

import java.io.* ;
import java.util.* ;
import java.net.* ;

import dmg.util.* ;

public class CheckIo {
   private ServerSocket _server  = null ;
   private static int  __counter = 0 ;
   private static synchronized int next(){ return __counter++ ; }
   private InputStream      _streamIn  = null ;
   private DataOutputStream _dataOut   = null ;
   private Vector           _hash      = new Vector() ;
   private class IoServer implements Runnable {
      private Socket _ioSocket = null ;
      private int    _counter  = 0 ;
      private DataInputStream _dataIn = null ;
      private OutputStream    _streamOut = null ;
      private long   _time    = 0 ;
      private int    _bufferSize = 0 ;
      private boolean _continue  = true ;
      private boolean _doDisplay = false ;
      
      private IoServer( Socket s , DataInputStream dataIn , OutputStream streamOut ) 
              throws Exception {
         _ioSocket   = s ;
         _counter    = next() ;
         _dataIn     = dataIn ;
         _streamOut  = streamOut ;
         _bufferSize = _dataIn.readInt() ;
         _time       = (long)_dataIn.readInt() ;

         new Thread(this).start() ;
         
         new Thread( 
            new Runnable(){
               public void run(){
                  try{ Thread.currentThread().sleep(_time) ;
                  }catch(InterruptedException ii){}
                  _continue = false ;
               }
            }
         ).start() ;
      }
      public void run(){
         say( "Starting BufferSize="+_bufferSize+" Time="+_time ) ;
         try{
            long start = System.currentTimeMillis() ;
            long part  = start ;
            long now = 0 ;
            long sum = 0 ;
            long total = 0 ;
            byte [] data = new byte[_bufferSize] ;
            while( _continue ){
               if( _doDisplay ){
                  now = System.currentTimeMillis() ;
                  if( ( now - start ) > _time )break ;
                  if( ( now - part ) > 5000 ){
                     say( "Rate : "+(((double)sum)/(double)(now-part)) ) ;
                     part = now ;
                     sum  = 0 ;
                  }
               }
               _streamOut.write( data , 0 , data.length ) ;
               sum   += data.length ;
               total += data.length ;
            }
            now = System.currentTimeMillis() ;
            say( "TRate : "+(((double)total)/(double)(now-start)) ) ;
            _hash.addElement( new IoStat( total , now -start ) ) ;
            _streamOut.close() ;
            _ioSocket.close() ;
         }catch(Exception ee ){
            say( "Problem : "+ee ) ;
         }
         say( "Finished" ) ;
      }
      private void say(String s ){
         System.out.println( "["+_counter+"] "+s ) ; 
      }
   
   }
   private class IoStat {
      private long _bytes = 0 ;
      private long _time  = 0 ;
      private double _rate = 0.0 ;
      private IoStat( long bytes , long time ){
         _bytes = bytes ;
         _time  = time ;
         _rate  = (((double)bytes)/((double)time))*1000  ;
      }
      public String toString(){
         return "Bytes="+_bytes+";time="+_time+";rate="+_rate ;
      }
      public double getRate(){ return _rate ; } 
      
   }
   public static final int CLEAR     = 0 ;
   public static final int SHUTDOWN  = 1 ;
   public static final int IO_SERVER = 2 ;
   public static final int DISPLAY   = 3 ;
   public CheckIo( int port )throws Exception {
       final ServerSocket _server = new ServerSocket( port ) ;
       new Thread( 
          new Runnable(){
            public void run(){
              System.out.println("Starting Listener" ) ;
              try{
                 while(true){
                    final Socket s = _server.accept() ;
                    System.out.println("Connected from : "+s ) ;
                    new Thread( 
                       new Runnable(){
                          public void run(){
                             try{
                                InputStream      is = s.getInputStream() ;
                                OutputStream     os = s.getOutputStream() ;
                                DataInputStream dis = new DataInputStream(is) ;
                                int command = dis.readInt() ;
                                switch( command ){
                                   case DISPLAY : 
                                      try{
                                         PrintWriter pw = 
                                           new PrintWriter(
                                              new OutputStreamWriter( os ) ) ;
                                         Enumeration e = _hash.elements() ;
                                         double sum  = 0 ;
                                         double sum2 = 0 ;
                                         int    n    = 0 ;
                                         while( e.hasMoreElements() ){
                                            IoStat stat = (IoStat)(e.nextElement()) ;
//                                            System.out.println( " sending : "+xx ) ;
                                            pw.println( stat.toString() ) ;
                                            sum  += stat.getRate() ;
                                            sum2 += stat.getRate() * stat.getRate() ;
                                            n    += 1 ;
                                         }
                                         double av = sum / (double) n ;
                                         double dev = ( sum2 - av * av * (double)n ) / (double)(n-1) ;
                                         dev = Math.sqrt( dev ) ;
                                         pw.println( "Av="+av+";dev="+dev ) ; 
                                         pw.flush() ;
                                      }catch(Exception eee ){
                                         eee.printStackTrace() ;
                                      }finally{
                                         try{
                                            s.close() ;
                                         }catch(Exception oe ){
                                         
                                         }
                                      }
                                   
                                   break ;
                                   case CLEAR : 
                                      _hash.removeAllElements() ;
                                      System.out.println( "Vector cleared" ) ;
                                          try{
                                            s.close() ;
                                         }catch(Exception oe ){
                                         
                                         }
                                  break ;
                                   case SHUTDOWN : System.exit(0) ; break ;
                                   case IO_SERVER :
                                      new IoServer( s , dis , os ) ;
                                   break ;
                                
                                }
                             }catch(Exception ee ){
                                try{ s.close() ; }catch(Exception ie ){}
                             }
                          }
                       }
                    ).start() ;
                 }
               }catch(Exception ee ){
//                  ee.printStackTrace() ;
                  System.out.println( "Exception in accept : "+ee ) ;
                  return ;
               }
            }
          }
       ).start() ;       
   
   }
   public CheckIo( String host , int port , int bufferSize , int time )
          throws Exception {
          
        Socket s  = new Socket( host , port ) ;
        _dataOut = new DataOutputStream( s.getOutputStream() ) ;
        _streamIn = s.getInputStream() ;
        _dataOut.writeInt( IO_SERVER ) ;
        _dataOut.writeInt( bufferSize ) ;
        _dataOut.writeInt( time ) ;
        final int bs = bufferSize ;
        new Thread( 
           new Runnable(){
             public void run(){
               byte [] data = new byte[bs] ;
               try{
                  while(true){
                     int n = _streamIn.read( data , 0 , data.length ) ;
                     if( n <= 0 )break ;
                  }
                }catch(Exception ee ){
                   System.out.println( "Exception in read : "+ee ) ;
                   return ;
                }
                System.out.println( "Client finished" ) ;
             }
           }
        ).start() ;       
        
   }
   public CheckIo( String host , int port , Args a )
          throws Exception {
          
        Socket s  = new Socket( host , port ) ;
        _dataOut  = new DataOutputStream( s.getOutputStream() ) ;
        _streamIn = s.getInputStream() ;
        
        String command = a.argv(0) ;
        
        if( command.equals( "shutdown" ) ){
           _dataOut.writeInt( SHUTDOWN ) ;
           try{
              _streamIn.read() ;
           }catch(Exception ee){
           
           }
           
        }else if( command.equals( "clear" ) ){
           _dataOut.writeInt( CLEAR ) ;
           try{
              _streamIn.read() ;
           }catch(Exception ee){
           
           }
        }else if( command.equals( "display" ) ){
            new Thread( 
               new Runnable(){
                 public void run(){
                   try{
                      _dataOut.writeInt( DISPLAY ) ;
                      BufferedReader br = 
                          new BufferedReader(
                             new InputStreamReader( _streamIn ) ) ;
                      String line = null ;
                      while( ( line = br.readLine() ) != null ){
                         System.out.println(line);
                      }
                    }catch(Exception ee ){
                       System.out.println( "Exception in read : "+ee ) ;
                       return ;
                    }
                    System.out.println( "Client finished" ) ;
                 }
               }
            ).start() ;               
        }
        
   }
   public static void usage(){
      System.err.println( "Usage : ... [options] server" ) ;
      System.err.println( "        ... [options] io <counter> <bSize> <time/sec>" ) ;
      System.err.println( "        ... [options] shutdown" ) ;
      System.err.println( "        ... [options] clear" ) ;
      System.err.println( "        ... [options] display" ) ;
      System.err.println( "  Options : -port=<port> -host=<host>" ) ;
      System.exit(4);
   }
   public static void main(String [] args ) throws Exception {
   
      if( args.length < 1 )CheckIo.usage() ;
      
      Args a = new Args( args ) ;
            
      int    port    = 2000 ;
      String host    = "localhost" ;
      String tmp     = null ;
      
      if( ( tmp = a.getOpt("port") ) != null ){
         try{
            port = Integer.parseInt( tmp ) ;
         }catch(Exception e){
            CheckIo.usage() ;
         }
      }
      if( ( tmp = a.getOpt("host") ) != null )host = tmp ;
      
      if( a.argv(0).equals( "server" ) ){
      
         System.out.println( "Running server on port "+port ) ;
         new CheckIo( port ) ;
         
      }else if ( a.argv(0).equals( "io" ) ){
      
         if( a.argc() < 4 )CheckIo.usage() ;
         
         int bufferSize = 0 ;
         int time       = 0 ;
         int count      = 0 ;
         try{
            count      = Integer.parseInt( a.argv(1) ) ;
            bufferSize = Integer.parseInt( a.argv(2) ) ;
            time       = Integer.parseInt( a.argv(3) ) ;
         }catch(Exception e){
            CheckIo.usage() ;
         }
         for( int i  = 0 ; i < count ; i++ )
            new CheckIo( host , port , bufferSize , time ) ;
      }else{
         new CheckIo(  host , port , a ) ;
      }
   
   }
}
