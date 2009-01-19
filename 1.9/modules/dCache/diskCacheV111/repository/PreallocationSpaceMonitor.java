// $Id: PreallocationSpaceMonitor.java,v 1.2 2003-10-24 09:00:14 cvs Exp $
package diskCacheV111.repository ;

import java.util.* ;

public class PreallocationSpaceMonitor implements SpaceMonitor {

   private SpaceMonitor _monitor = null ;
   private long         _preallocatedSpace = 0L ;
   private long         _preallocated      = 0L ;
   private long         _exceeded          = 0L ;
   private long         _maxAllowedSpace   = 0L ;
   
   public PreallocationSpaceMonitor( SpaceMonitor spaceMonitor ,
                                     long preallocatedSpace ,
                                     long maxAllowedSpace ){
     
      _monitor           = spaceMonitor ;
      _preallocatedSpace = preallocatedSpace ;  
      _preallocated      = preallocatedSpace ;  
      _maxAllowedSpace   = maxAllowedSpace ;        
                                     
   }
   public synchronized void allocateSpace( long space )
          throws InterruptedException,
                 IllegalArgumentException  {
    
      if( ( space = correctAllocation( space ) ) > 0L )
         _monitor.allocateSpace( space ) ;
      
      return ;     
   }
   public synchronized void allocateSpace( long space , long millis )
          throws InterruptedException,
                 IllegalArgumentException  {
    
      if( ( space = correctAllocation( space ) ) > 0L )
         _monitor.allocateSpace( space , millis ) ;
            
   }
   private long correctAllocation( long space ) 
           throws IllegalArgumentException {

      if( space <= _preallocatedSpace ){
         _preallocatedSpace -= space ;
         return 0 ;
      }
//      if( ! _allowOverbooking )
//         throw new
//         IllegalArgumentException("quota exceeded") ;
       
      space -= _preallocatedSpace ;
      _preallocatedSpace = 0L ;
      
      _exceeded += space ;
      
      return space ;
   
   }
   public synchronized void freeSpace( long space ){
      if( _exceeded == 0L ){
         _preallocatedSpace += space ;
         return ;
      }else if( space <= _exceeded ){
         _exceeded -= space ;
         _monitor.freeSpace( space ) ;
         return ;
      }else{     
         _monitor.freeSpace( _exceeded ) ;
         space -= _exceeded ;
         _exceeded = 0L ;
         _preallocatedSpace += space ;
         return ;
      }
   }
   public long getUsedSpace(){ return _preallocated - _preallocatedSpace ; }
   public void setTotalSpace( long space ){ _monitor.setTotalSpace(space) ; }
   public long getFreeSpace(){ return _monitor.getFreeSpace() ; }
   public long getTotalSpace(){ return _monitor.getTotalSpace() ; }
   
   public void addSpaceRequestListener( SpaceRequestable listener ){
      _monitor.addSpaceRequestListener( listener ) ;
   }
}
