/*
 * $Id: NameSpaceProvider.java,v 1.3 2005-08-11 08:35:28 tigran Exp $
 */


package diskCacheV111.namespace;

import diskCacheV111.util.*;

public interface NameSpaceProvider extends DcacheNameSpaceProvider {
        
    void setFileMetaData(PnfsId pnfsId, FileMetaData metaData);   
    FileMetaData getFileMetaData(PnfsId pnfsId) throws Exception ;
    
 
    PnfsId createEntry(String name, boolean type) throws Exception ;
    void deleteEntry( PnfsId pnfsId) throws Exception;
    void renameEntry( PnfsId pnfsId, String newName) throws Exception;
    
    String pnfsidToPath( PnfsId pnfsId) throws Exception ;
    PnfsId pathToPnfsid( String path, boolean followLinks) throws Exception;
    
    String[] getFileAttributeList(PnfsId pnfsId);
    Object getFileAttribute( PnfsId pnfsId, String attribute);
    void removeFileAttribute( PnfsId pnfsId, String attribute);
    void setFileAttribute( PnfsId pnfsId, String attribute, Object data);
    
}
