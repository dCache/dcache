// $Id: StorageInfoExtractable.java,v 1.3 2004-08-24 13:30:33 tigran Exp $

package diskCacheV111.util ;
import  diskCacheV111.vehicles.* ;
public interface StorageInfoExtractable {

   public StorageInfo getStorageInfo( String pnfsMountpoint , PnfsId pnfsId ) 
          throws CacheException ;
   public void setStorageInfo( String pnfsMountpoint , PnfsId pnfsId ,
                               StorageInfo storageInfo , int accessMode ) 
          throws CacheException ;
}
