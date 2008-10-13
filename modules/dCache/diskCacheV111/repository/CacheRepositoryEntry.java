// $Id: CacheRepositoryEntry.java,v 1.13 2007-10-15 18:53:08 behrmann Exp $

package diskCacheV111.repository ;

import diskCacheV111.vehicles.StorageInfo ;
import diskCacheV111.util.CacheException ;
import diskCacheV111.util.PnfsId ;
import java.io.File ;
import java.util.List;

import org.dcache.pool.repository.StickyRecord;

public interface CacheRepositoryEntry {
   /*
   public static final int CACHED = 1 ;
   public static final int PRECIOUS = 2 ;
   public static final int STORE    = 4 ;
   public static final int CLIENT   = 8 ;
   public static final int RECEIVING = 0x10 ;
   public static final int SENDING   = 0x20 ;
   public static final int FROM_CLIENT = RECEIVING | CLIENT ;
   public static final int FROM_STORE  = RECEIVING | STORE ;
   public static final int TO_CLIENT   = SENDING | CLIENT ;
   public static final int TO_STORE    = SENDING | STORE ;
   */
   /**
     * Get the PnfsId of this entry.
     */
   public PnfsId getPnfsId() ;
   /**
     * Get the size of this entry (if available)
     */
   public long getSize() throws CacheException ;
   /**
     *  Set the storage info for this entry.
     */
   public void setStorageInfo( StorageInfo storageInfo )
          throws CacheException ;
   /**
     *  get the storage info of the related entry.
     *  Throws CacheException if the storage info is not
     *  (yet) available.
     */
   public StorageInfo getStorageInfo()
          throws CacheException ;

   public void setCached() throws CacheException ;
   public void setPrecious() throws CacheException ;
   public void setPrecious(boolean force) throws CacheException ;
   public void setReceivingFromClient() throws CacheException ;
   public void setReceivingFromStore() throws CacheException ;
   public void setSendingToStore( boolean sending ) throws CacheException ;
   public void setSticky( boolean sticky ) throws CacheException ;
   public void setRemoved() throws CacheException ;
   public boolean isSticky() throws CacheException ;
   public boolean isPrecious() throws CacheException;
   public boolean isCached() throws CacheException;
   public boolean isReceivingFromClient() throws CacheException;
   public boolean isReceivingFromStore() throws CacheException;
   public boolean isSendingToStore() throws CacheException;

   public boolean isBad() ;
   public void setBad( boolean bad ) ;

   public boolean isRemoved() throws CacheException;
   public boolean isDestroyed() throws CacheException;

   public File getDataFile() throws CacheException  ;
   public String getState() ;
   public long getCreationTime() throws CacheException;
   public long getLastAccessTime() throws CacheException;
   public void touch() throws CacheException;

   public CacheRepositoryStatistics getCacheRepositoryStatistics() throws CacheException;

   public  void decrementLinkCount() throws CacheException ;
   public  void incrementLinkCount() throws CacheException ;
   public  int getLinkCount() throws CacheException ;
  /*
   public void addRestoreClientMessage( CellMessage message ) throws CacheException;
   public Iterator  getRestoreClientMesssages() throws CacheException;
  */

   public void lock( boolean locked ) ;
   public void lock( long millisSeconds );
   public boolean isLocked();

	/**
	 *
	 * set/clean sticky flag
	 *
	 * @param sticky
	 * @param owner flag owner
	 * @param validTill time milliseconds since 00:00:00 1 Jan. 1970.
	 * @throws CacheException
	 */

	public void setSticky(boolean sticky, String owner, long validTill) throws CacheException ;

	/**
	 *
	 * @return list of StickyRecords held by the file
	 */
	public List<StickyRecord> stickyRecords() ;
}
