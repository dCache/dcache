// $Id: RandomAllocation.java,v 1.2 2001-06-22 09:58:58 cvs Exp $
package diskCacheV111.repository ;

import java.util.* ;

public class RandomAllocation implements SpaceMonitor {
   private long _usedSpace = 0 ;
   private long _totalSpace = 0 ;
   public RandomAllocation( long space ){
      _totalSpace = space ;
   }
   public synchronized void addSpaceRequestListener( SpaceRequestable listener ){
      throw new IllegalArgumentException("Not supported") ;
   
   }
   public synchronized void allocateSpace( long space , long millis )
          throws InterruptedException {
       long end = System.currentTimeMillis() + millis ;
       
       
       while( ( _totalSpace - _usedSpace ) < space ){
           long rest = end - System.currentTimeMillis() ;
           if( rest <= 0 )
              throw new
              InterruptedException("Wait timed out") ;
              
           wait( rest ) ;
       }
      _usedSpace += space ;
   }
   public synchronized void allocateSpace( long space )
          throws InterruptedException {
      while( ( _totalSpace - _usedSpace ) < space )wait() ;
      _usedSpace += space ;
   }
   public synchronized void freeSpace( long space ){
       _usedSpace -= space ;
       notifyAll() ;
   }
   public synchronized void setTotalSpace( long space ){
      _totalSpace = space ;
      notifyAll() ;
   }
   public synchronized long getFreeSpace(){
     return _totalSpace - _usedSpace ;
   }
   public synchronized long getTotalSpace(){
     return _totalSpace ;
   }
   public static void main( String [] args )throws Exception {
   
      final SpaceMonitor m = new RandomAllocation(1000) ;
      
      for( int i = 0 ; i < 10 ; i++ ){
           new Thread(
                 new Runnable(){
                   public void run() {
                     while(true){
                       try{m.allocateSpace(500);}catch(Exception ii){}
                       System.out.println("Got it");
                     }
                   }
                 }
           ).start() ;
      }
      new Thread(
                 new Runnable(){
                   public void run(){
                     while(true){
                       m.freeSpace(100);
                       System.out.println("freed");
                       try{ Thread.currentThread().sleep(500);}    
                       catch(Exception e){}
                     }
                   }
                 }
      ).start() ;
   }
}
