// $Id: StorageInfoExtractable.java,v 1.5 2007-07-10 17:15:09 tigran Exp $

package diskCacheV111.util ;
import  diskCacheV111.vehicles.* ;

public interface StorageInfoExtractable {

   public StorageInfo getStorageInfo( String pnfsMountpoint , PnfsId pnfsId )
          throws CacheException ;
   public void setStorageInfo( String pnfsMountpoint , PnfsId pnfsId ,
                               StorageInfo storageInfo , int accessMode )
          throws CacheException ;
}
