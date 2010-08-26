package diskCacheV111.util ;
import java.io.* ;
public class MultiIo implements Runnable {
   private int _count = 0 ;
   private int _size = 0 ;
   private int _threadCount = 0 ;
   public MultiIo( int size , int count , int threadCount ){
      _size        = size ;
      _threadCount = threadCount ;
      _count       = count ;
      
      for( int i = 0 ; i < _threadCount ; i++ ){
      
          new Thread(this).start() ;
      }
   
   }
   public void run(){
      try{
         byte [] data = new byte[_size] ;
         OutputStream out = 
         new FileOutputStream(  "/dev/null" ) ;
         long start = System.currentTimeMillis() ;
         for( int i = 0 ; i < _count ; i++ ){
            out.write( data , 0 , data.length ) ;
         }
         long total = (long)_size * (long)_count ;
         long diff = System.currentTimeMillis()-start;
         out.close() ;
         double rate = (double)total / (double)diff / 1024. / 1024. * 1000 ;
         System.out.println( ""+rate+" MB/sec" ) ;
      }catch(Exception xx ){
         System.err.println("Error : "+xx ) ;
      }
   }
   
   public static void main( String [] args ) throws Exception {
   
      if( args.length < 3 ){
         System.out.println( "Usage : ... <size> <count> <threads>" ) ;
         System.exit(4);
      }
      int size    = Integer.parseInt( args[0] ) ;
      int count   = Integer.parseInt( args[1] ) ;
      int threads = Integer.parseInt( args[2] ) ;
      new MultiIo( size , count , threads ) ;
   
   }


}
