package diskCacheV111.util ;

import java.util.* ;
import java.io.* ;

public class SysTimerTest extends Thread {
   private int _c ;
   public SysTimerTest( int c ){
      _c = c ;
   }
   public void run() {
      SysTimer sys = new SysTimer() ;
      SysTimer.Timestamp timestamp = sys.getRUsage() ;
      long sum = 0 ;
      long now = System.currentTimeMillis() ;
      
         try{
            Thread.currentThread().sleep(0,1) ;
         }catch(Exception ee){
         
         }
      
      System.out.println( "Diff : "+ ( System.currentTimeMillis() - now ) ) ;
//      System.out.println( "("+_c+") : "+sys.getDifference().toString() ) ;
/*
      for( int i= 0 ; i < 100000 ; i++ ){
         long l = 1 ;
         
         for( int j = 0 ; j < 1000 ; j++ )l *= 1234 ; 
                
         try{
            Thread.currentThread().sleep(1) ;
         }catch(Exception ee){
         
         }
      }
      SysTimer.Timestamp now = sys.getRUsage() ;
      now.substract( timestamp ) ;
      System.out.println( "("+_c+") : "+now.toString() ) ;
 */
   }
   public static void main( String [] args ){
      new SysTimerTest(1).start() ;
   }

}
