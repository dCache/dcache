// $Id: CacheRepository.java,v 1.13.2.1 2006-09-07 06:38:39 patrick Exp $

package diskCacheV111.repository ;

import java.util.* ;

import diskCacheV111.util.CacheException ;
import diskCacheV111.util.PnfsId ;
import diskCacheV111.util.PnfsHandler ;
import diskCacheV111.util.event.CacheRepositoryListener ;
import dmg.util.Logable ;

public interface CacheRepository extends SpaceMonitor {
   public static final int ERROR_IO_DISK = 204 ;
   public static final int ALLOW_CONTROL_RECOVERY = 0x1 ;
   public static final int ALLOW_INFO_RECOVERY    = 0x2 ;
   public static final int ALLOW_SPACE_RECOVERY   = 0x4 ;
   public static final int ALLOW_RECOVER_ANYWAY       = 0x8 ;
   
   public CacheRepositoryEntry createEntry( PnfsId pnfsId ) 
          throws CacheException ;
   
   public CacheRepositoryEntry getEntry( PnfsId pnfsId )
          throws CacheException ;
   public CacheRepositoryEntry getGenericEntry( PnfsId pnfsId )
          throws CacheException ;
   
   public boolean removeEntry(  CacheRepositoryEntry entry ) 
          throws CacheException ;

   public void reserveSpace( long space , boolean blocking )
          throws CacheException , InterruptedException ;
   public void freeReservedSpace( long space ) 
          throws CacheException ;
   public void applyReservedSpace( long space ) 
          throws CacheException ;
          
   public long getReservedSpace() ;
   
   public Iterator pnfsids() throws CacheException ;
   public List getValidPnfsidList() ; 
   public void addCacheRepositoryListener( 
                  CacheRepositoryListener listener ) ;
                  
   public void removeCacheRepositoryListener(
                  CacheRepositoryListener listener ) ;
   
   public void runInventory(Logable log) throws CacheException ;
   public void runInventory(Logable log, PnfsHandler pnfs , int flags ) 
          throws CacheException ;
   
   public long getPreciousSpace() ;
   public void setLogable( Logable logable ) ;
   public boolean contains(PnfsId pnfsId );
   
   public boolean isRepositoryOk() ;
}
