// $Id: SpaceMonitor.java,v 1.3 2004-06-16 11:03:45 patrick Exp $
package diskCacheV111.repository ;

import java.util.* ;

public interface SpaceMonitor {
   public static final long NONBLOCKING = -1L ;
   public static final long BLOCKING    = 0L ;
   public void allocateSpace( long space )
          throws InterruptedException ;
   public void allocateSpace( long space , long millis )
          throws InterruptedException,
                 MissingResourceException  ;
   public void freeSpace( long space ) ;
   public void setTotalSpace( long space ) ;
   public long getFreeSpace() ;
   public long getTotalSpace() ;
   
   public void addSpaceRequestListener( SpaceRequestable listener ) ;
}
