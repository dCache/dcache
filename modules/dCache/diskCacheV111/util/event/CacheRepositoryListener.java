// $Id: CacheRepositoryListener.java,v 1.6 2002-01-15 10:48:12 cvs Exp $

package diskCacheV111.util.event ;

public interface CacheRepositoryListener extends CacheEventListener {

    /**
     * called when an entry changes state to RECIOUS
     * @param event
     */
     public void precious( CacheRepositoryEvent event ) ;

     /**
      * called when an entry changes state to CACHED
      * @param event
      */
     public void cached( CacheRepositoryEvent event ) ;

     /**
      * called when an entry physically created in the pool
      * @param event
      */
     public void created( CacheRepositoryEvent event ) ;
     public void touched( CacheRepositoryEvent event ) ;

     /**
      * called when an entry removed
      * @param event
      */
     public void removed( CacheRepositoryEvent event ) ;

     /**
      * called when an entry physically removed from the pool
      * @param event
      */
     public void destroyed( CacheRepositoryEvent event ) ;
     public void scanned( CacheRepositoryEvent event ) ;

     /**
      * called when an entry is ready to be delivered to clients
      * @param event
      */
     public void available( CacheRepositoryEvent event ) ;

     /**
      * called when an entry changed it's sticky state ( add/remove )
      * @param event
      */
     public void sticky( CacheRepositoryEvent event ) ;
     public void needSpace( CacheNeedSpaceEvent event ) ;
}
