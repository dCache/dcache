/*
 * $Id: NameSpaceProvider.java,v 1.10 2007-09-21 15:09:57 tigran Exp $
 */


package diskCacheV111.namespace;

import diskCacheV111.util.FileMetaData;
import diskCacheV111.util.PnfsId;
import org.dcache.util.Checksum;
import java.util.Set;

public interface NameSpaceProvider extends DcacheNameSpaceProvider {

    /**
     * set file metadata - size, permissions, Owner and group
     * @param pnfsId
     * @param metaData
     * @throws Exception
     */
    void setFileMetaData(PnfsId pnfsId, FileMetaData metaData) throws Exception ;

    /**
     * get file metadata - size, permissions, Owner and group
     * @param pnfsId
     * @return
     * @throws Exception
     */
    FileMetaData getFileMetaData(PnfsId pnfsId) throws Exception ;


    /**
     * create file or directory for given path
     * @param path full path of new object
     * @param metaData initial values for object metadata, like owner, group, permissions mode
     * @param isDirectory create a directory if true
     * @return PnfsId of newly created object
     * @throws Exception
     */
    PnfsId createEntry(String path, FileMetaData metaData, boolean isDirectory) throws Exception ;
    
    /**
     * remove file or directory associated with given pnfsid
     * @param pnfsId
     * @throws Exception
     */
    void deleteEntry( PnfsId pnfsId) throws Exception;
    
    /**
     * remove file or directory
     * @param path
     * @throws Exception
     */
    void deleteEntry( String path ) throws Exception;
    
    void renameEntry( PnfsId pnfsId, String newName) throws Exception;

    String pnfsidToPath( PnfsId pnfsId) throws Exception ;
    PnfsId pathToPnfsid( String path, boolean followLinks) throws Exception;

    PnfsId getParentOf(PnfsId pnfsId) throws Exception;

    String[] getFileAttributeList(PnfsId pnfsId);
    Object getFileAttribute( PnfsId pnfsId, String attribute);
    void removeFileAttribute( PnfsId pnfsId, String attribute);
    void setFileAttribute( PnfsId pnfsId, String attribute, Object data);

    /**
    * Adds new or replaces existing checksum value for the specific file and checksum type.
    * The type of the checksum is arbitrary integer that client of this interface must chose
    * and use consistently afterwards
    * @param type the type (or algorithm) of the checksum
    * @param value HEX presentation of the digest (checksum)
    * @param pnfsId file
    */
    void addChecksum(PnfsId pnfsId, int type, String value) throws Exception;

    /**
    * Returns HEX presentation of the checksum value for the specific file and checksum type.
    * Returns null if value has not been set
    * @param type the type (or algorithm) of the checksum
    * @param pnfsId file
    */
    String getChecksum(PnfsId pnfsId, int type) throws Exception;

    /**
    * Clears checksum value storage for the specific file and checksum type.
    * @param type the type (or algorithm) of the checksum
    * @param pnfsId file
    */
    void removeChecksum(PnfsId pnfsId, int type) throws Exception;


    int[] listChecksumTypes(PnfsId pnfsId) throws Exception;
    
    Set<Checksum> getChecksums(PnfsId pnfsId) throws Exception;

}
