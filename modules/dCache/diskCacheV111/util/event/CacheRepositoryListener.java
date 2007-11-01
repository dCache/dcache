// $Id: CacheRepositoryListener.java,v 1.6 2002-01-15 10:48:12 cvs Exp $

package diskCacheV111.util.event ;

public interface CacheRepositoryListener extends CacheEventListener {

     public void precious( CacheRepositoryEvent event ) ;
     public void cached( CacheRepositoryEvent event ) ;
     public void created( CacheRepositoryEvent event ) ;
     public void touched( CacheRepositoryEvent event ) ;
     public void removed( CacheRepositoryEvent event ) ;
     public void destroyed( CacheRepositoryEvent event ) ;
     public void scanned( CacheRepositoryEvent event ) ;
     public void available( CacheRepositoryEvent event ) ;
     public void sticky( CacheRepositoryEvent event ) ;
     public void needSpace( CacheNeedSpaceEvent event ) ;
}
