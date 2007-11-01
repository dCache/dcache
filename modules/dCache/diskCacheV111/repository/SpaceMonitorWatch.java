// $Id: SpaceMonitorWatch.java,v 1.1.2.2 2007-09-25 00:29:12 timur Exp $
package diskCacheV111.repository ;

import java.util.* ;

/**
  * 
  *  SpaceMonitorWatch is a wrapper around the a space monitor, watching
  *  the allocs and 'frees'. Finally a correctSpace can be called with
  *  the correct, final size of the dataset.
  *
  */
public class SpaceMonitorWatch  implements SpaceMonitor {

   private SpaceMonitor _monitor = null ;
   private long _totalAllocated  = 0L ;
   private long _totalFreed      = 0L ;
   private int  _countAllocated  = 0 ;
   private int  _countFreed      = 0 ;
   private boolean _finished     = false ;
   
   public SpaceMonitorWatch( SpaceMonitor monitor ){
      _monitor = monitor ;
   }
   public synchronized void allocateSpace( long space )
          throws InterruptedException{
        if(_finished) {
            RuntimeException re = new IllegalStateException(
            " SpaceMonitorWatch: monitor allocation is not allowed anymore");
            re.fillInStackTrace();
            re.printStackTrace();
            throw re;
        }
       _monitor.allocateSpace( space ) ;
       _totalAllocated += space ;
       _countAllocated ++ ;
   }
   public synchronized void allocateSpace( long space , long millis )
          throws InterruptedException,
                 MissingResourceException  {
    
        if(_finished) {
            RuntimeException re = new IllegalStateException(
            " SpaceMonitorWatch: monitor allocation is not allowed anymore");
            re.fillInStackTrace();
            re.printStackTrace();
            throw re;
        }
        _monitor.allocateSpace( space , millis ) ;             
       _totalAllocated += space ;
       _countAllocated ++ ;
   }
   public synchronized void freeSpace( long space ){
        if(_finished) {
            RuntimeException re = new IllegalStateException(
            " SpaceMonitorWatch: monitor allocation is not allowed anymore");
            re.fillInStackTrace();
            re.printStackTrace();
            throw re;
        }
      _monitor.freeSpace( space ) ;
      _totalFreed += space ;
       _countFreed ++ ;
   }
   public void setTotalSpace( long space ){
      _monitor.setTotalSpace(space);
   }
   public long getFreeSpace() { return _monitor.getFreeSpace() ; }
   public long getTotalSpace() { return _monitor.getTotalSpace() ; }
   
   public void addSpaceRequestListener( SpaceRequestable listener ){
      _monitor.addSpaceRequestListener(listener);
   }   
   public String toString(){
      return "SpaceMonitorWatch(AL="+_totalAllocated+"/"+_countAllocated+
             ";FR="+_totalFreed+"/"+_countFreed+
             ";DF="+( _totalAllocated - _totalFreed)+")";
   }
   /**
     * correctSpace adjust the space calculation of the repository if the
     * correct size of the file is specified here.
     */
   public synchronized long correctSpace( long totalSpaceUsed )
          throws InterruptedException,
                 MissingResourceException  {
          _finished = true;
          long tooMuchAllocated = ( _totalAllocated - _totalFreed) - totalSpaceUsed ;
          if( tooMuchAllocated == 0 ){
              return 0L ;
          }else if( tooMuchAllocated > 0 ){
              _monitor.freeSpace( tooMuchAllocated ) ;
              return tooMuchAllocated ;
          }else{
              _monitor.allocateSpace( -tooMuchAllocated ) ;
              return tooMuchAllocated ;
          }
    }   
   
}
