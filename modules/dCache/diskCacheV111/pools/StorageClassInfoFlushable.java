// $Id: StorageClassInfoFlushable.java,v 1.2 2006-04-03 05:36:40 patrick Exp $

package diskCacheV111.pools ;

import java.util.* ;

import dmg.cells.nucleus.* ;
import dmg.util.* ;

import diskCacheV111.util.* ;

public interface StorageClassInfoFlushable {

    public void storageClassInfoFlushed( String hsm , String storageClass , long flushId , int requests , int failed ) ;

}
