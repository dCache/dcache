// $Id: CacheRepositoryEvent.java,v 1.3 2001-03-28 16:12:43 cvs Exp $
package diskCacheV111.util.event ;

import diskCacheV111.repository.CacheRepositoryEntry ;
import org.dcache.pool.repository.StickyRecord;

public class CacheRepositoryEvent extends CacheEvent {
   private CacheRepositoryEntry _repositoryEntry = null ;
   private StickyRecord _sticky;

   public CacheRepositoryEvent( Object source ){
      super(source);
   }
   public CacheRepositoryEvent( Object source , CacheRepositoryEntry entry ){
       this(source);
      _repositoryEntry = entry ;
   }
    public CacheRepositoryEvent( Object source , CacheRepositoryEntry entry, StickyRecord sticky ){
       this(source, entry);
       _sticky = sticky;
   }
   public CacheRepositoryEntry getRepositoryEntry(){ return _repositoryEntry ; }
   public StickyRecord getStickyRecord(){ return _sticky ; }
   public String toString(){
      return "CacheRepositoryEvent for : "+_repositoryEntry;
   }
}
