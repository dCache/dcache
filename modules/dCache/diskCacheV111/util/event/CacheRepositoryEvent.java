// $Id: CacheRepositoryEvent.java,v 1.3 2001-03-28 16:12:43 cvs Exp $
package diskCacheV111.util.event ;


import diskCacheV111.repository.CacheRepositoryEntry ;
public class CacheRepositoryEvent extends CacheEvent {
   private CacheRepositoryEntry _repositoryEntry = null ;
   public CacheRepositoryEvent( Object source ){
      super(source);
   }
   public CacheRepositoryEvent( Object source , CacheRepositoryEntry entry ){
      super(source);
      _repositoryEntry = entry ;
   }
   public CacheRepositoryEntry getRepositoryEntry(){ return _repositoryEntry ; }
   public String toString(){
      return "CacheRepositoryEvent for : "+_repositoryEntry;
   }
}
