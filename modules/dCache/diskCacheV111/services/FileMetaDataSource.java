/*
 * $Id: FileMetaDataSource.java,v 1.1.2.1 2007-01-05 11:43:38 radicke Exp $
 */
package diskCacheV111.services;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileMetaData;
import diskCacheV111.util.PnfsId;

public interface FileMetaDataSource {
	
	public FileMetaData getMetaData(String path) throws CacheException ;
	public FileMetaData getMetaData(PnfsId pnfsId) throws CacheException ;	
}
/*
 * $Log: not supported by cvs2svn $
 * Revision 1.1  2006/11/07 10:40:44  tigran
 * new interface FileMataDataSource :
 *   returns fileMetaData by path or pnfsId
 *
 * two implementations: filesystem based and PnfsManager based
 *
 */
