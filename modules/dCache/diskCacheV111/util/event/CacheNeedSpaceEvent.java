// $Id: CacheNeedSpaceEvent.java,v 1.1 2001-03-28 16:12:43 cvs Exp $
package diskCacheV111.util.event ;


import diskCacheV111.repository.CacheRepositoryEntry ;

public class CacheNeedSpaceEvent extends CacheRepositoryEvent {
   private long _size = 0 ;
   public CacheNeedSpaceEvent( Object source , long size  ){
      super(source);
      _size = size  ;
   }
   public long getRequiredSpace(){ return _size ; }
   public String toString(){
      return "CacheNeedSpaceEvent size = "+_size;
   }

}
