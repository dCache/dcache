// $Id: StorageClassInfoFlushable.java,v 1.2 2006-04-03 05:36:40 patrick Exp $

package diskCacheV111.pools ;

public interface StorageClassInfoFlushable {

    void storageClassInfoFlushed(String hsm, String storageClass, long flushId, int requests, int failed) ;

}
