// $Id: FairQueueAllocation.java,v 1.4 2004-06-16 11:03:45 patrick Exp $
package diskCacheV111.repository ;

import java.util.* ;

public class FairQueueAllocation implements SpaceMonitor {
   private long _usedSpace     = 0 ;
   private long _totalSpace    = 0 ;
   private ArrayList _list     = new ArrayList() ;
   private ArrayList _listener = new ArrayList() ;
   
   public FairQueueAllocation( long space ){
      _totalSpace = space ;
   }
   public synchronized void allocateSpace( long space , long millis )
          throws InterruptedException ,
                 MissingResourceException  {
       
       
      if( ( _list.size() > 0 ) ||
          (( _totalSpace - _usedSpace ) < space ) ){

          long end = System.currentTimeMillis() + millis ;
          Long x = new Long(space) ;
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
      Iterator i = _listener.iterator() ;
      while( i.hasNext() )
         ((SpaceRequestable)i.next()).spaceNeeded(space) ;
   }
   public synchronized void allocateSpace( long space )
          throws InterruptedException {
          
          
      if( ( _list.size() > 0 ) ||
          (( _totalSpace - _usedSpace ) < space ) ){

         Long x = new Long(space) ;
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
      // (belive me, we have to to that.
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
                       try{ Thread.currentThread().sleep(500);}    
                       catch(Exception e){}
                     }
                   }
                 }
      ).start() ;
   }

}
