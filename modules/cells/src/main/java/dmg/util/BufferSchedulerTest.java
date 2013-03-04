package dmg.util ;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

public class BufferSchedulerTest implements Runnable {

  private BufferScheduler _scheduler ;
  private Thread _consumerThread ;
  private Thread _producerThread ;
  private byte [] _inBuffer ;
  private byte [] _outBuffer ;
  private InputStream _inFile ;
  private OutputStream _outFile ;

  private int _sleepTime = 1000 ;
  private int _maxProducerCount;
  private int _bufferSize ;
  private int _nob ;
  private String _mode ;

  public BufferSchedulerTest( String [] args  ) throws Exception {

     _nob         = new Integer(args[0]);
     _bufferSize  = new Integer(args[1]);
     _mode        = args[2] ;

     _scheduler = new BufferScheduler( _nob , _bufferSize ) ;

     _consumerThread = new Thread( this ) ;
     _producerThread = new Thread( this ) ;

     _inBuffer  = new byte[_bufferSize] ;
     _outBuffer = new byte[_bufferSize] ;

     if( _mode.equals("memory") ){
        if( args.length < 4 ) {
            throw new IllegalArgumentException("");
        }
        _maxProducerCount = new Integer(args[3]);
     }else if( _mode.equals("filecopy" ) ){
        if( args.length < 5 ) {
            throw new IllegalArgumentException("");
        }
        _inFile  = new FileInputStream( args[3] ) ;
        _outFile = new FileOutputStream( args[4] ) ;
     }

     _consumerThread.start() ;
     _producerThread.start() ;

  }
  @Override
  public void run(){
     if( _mode.equals( "memory" ) ){
        runMemory() ;
     }else if( _mode.equals( "filecopy" ) ){
        runFilecopy();
     }
  }
  public void runFilecopy(){
    try{
    if( Thread.currentThread() == _consumerThread ){
      long startTime = System.currentTimeMillis() ;
      while(true){
         BufferDescriptor b = _scheduler.getFullBuffer() ;
         int size = b.getUsable() ;
         if( size < 0 ){
            _scheduler.release( b ) ;
            break ;
         }else{
            byte [] base = b.getBase() ;
            _outFile.write( base , 0 , b.getUsable() ) ;
            _scheduler.release( b ) ;
         }
      }
      long diff = System.currentTimeMillis() - startTime ;
      System.out.println( "Consumer : "+diff+" msec " ) ;
      System.out.println( ""+_scheduler  ) ;
      _outFile.close() ;
    }else if( Thread.currentThread() == _producerThread ){
        long startTime = System.currentTimeMillis() ;
        while( true ){
           BufferDescriptor b = _scheduler.getEmptyBuffer() ;
           byte [] base = b.getBase() ;
           int inbytes  = _inFile.read( base , 0 , base.length ) ;
           b.setUsable( inbytes ) ;
           _scheduler.release(  b ) ;
           if( inbytes < 0 ) {
               break;
           }
        }
        long diff = System.currentTimeMillis() - startTime ;
        System.out.println( "Producer : "+diff+" msec " ) ;
        _inFile.close() ;
    }
    }catch( Exception e ){
      System.out.println( "Exception : "+e ) ;
    }
  }
  public void runMemory(){
    try{
    if( Thread.currentThread() == _consumerThread ){
      long startTime = System.currentTimeMillis() ;
      while(true){
         BufferDescriptor b = _scheduler.getFullBuffer() ;
         int size = b.getUsable() ;
         if( size < 0 ){
            _scheduler.release( b ) ;
            break ;
         }else{
            byte [] base = b.getBase() ;
            System.arraycopy( base , 0 , _outBuffer , 0 ,size ) ;
            _scheduler.release( b ) ;
         }
      }
      long diff = System.currentTimeMillis() - startTime ;
      System.out.println( "Consumer : "+diff+" msec " ) ;
      System.out.println( ""+_scheduler  ) ;
    }else if( Thread.currentThread() == _producerThread ){
      long startTime = System.currentTimeMillis() ;
      for( int i = 0 ; i < _maxProducerCount ; i++ ){
         BufferDescriptor b = _scheduler.getEmptyBuffer() ;
         byte [] base = b.getBase() ;
         System.arraycopy( _inBuffer , 0 , base , 0 , _inBuffer.length ) ;
         b.setUsable( _inBuffer.length ) ;
         _scheduler.release(  b ) ;
      }
      BufferDescriptor b = _scheduler.getEmptyBuffer() ;
      b.setUsable(-1);
      _scheduler.release( b ) ;
      long diff = System.currentTimeMillis() - startTime ;
      System.out.println( "Producer : "+diff+" msec " ) ;
    }
    }catch( Exception e ){
      System.out.println( "Exception : "+e ) ;

    }
//       try{ Thread.sleep(_sleepTime) ; }
//       catch( InterruptedException ie ){} ;

  }
  private static final String USAGE =
     "Usage : ... <numOfBuffers> <sizeOfBuffers>" ;

  public static void main( String [] args ){
     if( args.length < 3){
       System.out.println( USAGE + "memory <producerCount>" ) ;
       System.out.println( USAGE + "filecopy <in> <out>" ) ;
       System.exit(3) ;
     }
     try{
        new BufferSchedulerTest( args ) ;
     }catch( IllegalArgumentException ae ){
       System.out.println( USAGE + "memory <producerCount>" ) ;
       System.out.println( USAGE + "filecopy <in> <out>" ) ;
     }catch( Exception e ){
        System.out.println( "Exception : "+e ) ;
     }
  }
}
