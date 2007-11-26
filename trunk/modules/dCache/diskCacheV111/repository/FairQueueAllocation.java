// $Id: FairQueueAllocation.java,v 1.7 2007-07-03 13:51:31 tigran Exp $
package diskCacheV111.repository ;

import java.util.ArrayList;
import java.util.List;
import java.util.MissingResourceException;

public class FairQueueAllocation implements SpaceMonitor {

   private long _usedSpace     = 0 ;
   private long _totalSpace    = 0 ;
   private final List<Long> _list     = new ArrayList<Long>() ;
   private final List<SpaceRequestable> _listener = new ArrayList<SpaceRequestable>() ;

   public FairQueueAllocation( long space ){
      _totalSpace = space ;
   }
   public synchronized void allocateSpace( long space , long millis )
          throws InterruptedException ,
                 MissingResourceException  {


      if( ( _list.size() > 0 ) ||
          (( _totalSpace - _usedSpace ) < space ) ){

          long end = System.currentTimeMillis() + millis ;
          Long x = Long.valueOf(space) ;
          _list.add(x) ;
          try{
             while( ( x != _list.get(0) ) ||
                 ( ( _totalSpace - _usedSpace ) < space ) ){

                 long rest = end - System.currentTimeMillis() ;
                 if( rest <= 0 )
                    throw new
                    InterruptedException("Wait timed out") ;

                 wait( rest ) ;
             }
          }finally{
             _list.remove(0);
          }
       }
      _usedSpace += space ;
   }
   private void triggerCallbacks( long space ){
      for( SpaceRequestable sr: _listener ) {
         sr.spaceNeeded(space) ;
      }
   }
   public synchronized void allocateSpace( long space )
          throws InterruptedException {


      if( ( _list.size() > 0 ) ||
          (( _totalSpace - _usedSpace ) < space ) ){

         Long x = Long.valueOf(space) ;
         _list.add(x) ;
         triggerCallbacks( space ) ;
         try{
            while( ( x != _list.get(0) ) ||
                   (( _totalSpace - _usedSpace ) < space ) )wait() ;
         }catch(InterruptedException ie ){
            for( int i = 0 ; i < _list.size() ; i++ ){
               if( _list.get(i) == x ){
                  _list.remove(i);
                  triggerCallbacks( -space ) ;
                  break ;
               }

            }
            throw ie ;
         }
         _list.remove(0);
      }
      _usedSpace += space ;
      //
      // there might be still others which needs to be informed.
      // (believe me, we have to to that.
      //
      if( _list.size() > 0 )notifyAll() ;
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
   public synchronized void addSpaceRequestListener( SpaceRequestable listener ){
      _listener.add( listener ) ;

   }
   public static void main( String [] args )throws Exception {

      final SpaceMonitor m = new FairQueueAllocation(1000) ;

      for( int i = 0 ; i < 10 ; i++ ){
           new Thread(
                 new Runnable(){
                   public void run() {
                     Thread t = Thread.currentThread() ;
                     while(true){
                       System.out.println("Waiting "+t.getName());
                       try{m.allocateSpace(500);}catch(Exception ii){}
                       System.out.println("Got it "+t.getName());
                     }
                   }
                 }
           ).start() ;
      }
      new Thread(
                 new Runnable(){
                   public void run(){
                     while(true){
                       m.freeSpace(1500);
                       System.out.println("freed");
                       try{ Thread.sleep(500);}
                       catch(Exception e){}
                     }
                   }
                 }
      ).start() ;
   }

}
