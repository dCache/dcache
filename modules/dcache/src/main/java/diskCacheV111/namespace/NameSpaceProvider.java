package diskCacheV111.namespace;

import com.google.common.collect.Range;

import javax.security.auth.Subject;

import java.util.List;
import java.util.Set;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RetentionPolicy;

import org.dcache.namespace.CreateOption;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.ListHandler;
import org.dcache.util.ChecksumType;
import org.dcache.util.Glob;
import org.dcache.vehicles.FileAttributes;

public interface NameSpaceProvider
{
    /**
     * When the mode field is not specified in createEntry, the mode
     * is inherited from the parent directory. When creating new
     * directories, this UMASK is applied to the inherited mode.
     */
    public final int UMASK_DIR = 0777;

    /**
     * When the mode field is not specified in createEntry, the mode
     * is inherited from the parent directory. When creating new
     * regular files, this UMASK is applied to the inherited mode.
     */
    public final int UMASK_FILE = 0666;

    /**
     * Special value used to indicate that a default value should be
     * used.
     */
    public static final int DEFAULT = -1;

    /**
     * set if there is no old value
     */
    public static final int SI_EXCLUSIVE = 0;
    /**
     * replace old value with new one
     */
    public static final int SI_OVERWRITE = 1;
    /**
     * append new value to the old one
     */
    public static final int SI_APPEND = 2;

    /**
     * Create a file for a given path and type.
     *
     * @param subject Subject of user who invoked this method.
     * @param path full path of new object
     * @param uid uid of new entry or -1 for default
     * @param gid gid of new entry or -1 for default
     * @param mode mode of new entry or -1 for default
     * @param requestedAttributes Attributes of the new file to return
     * @return FileAttributes of newly created file
     * @throws CacheException
     */
    FileAttributes createFile(Subject subject, String path, int uid, int gid, int mode,
                              Set<FileAttribute> requestedAttributes) throws CacheException;

    /**
     * Create a directory for a given path and type.
     *
     * @param subject Subject of user who invoked this method.
     * @param path full path of new object
     * @param uid uid of new entry or -1 for default
     * @param gid gid of new entry or -1 for default
     * @param mode mode of new entry or -1 for default
     * @return PnfsId of newly created object
     * @throws CacheException
     */
    PnfsId createDirectory(Subject subject, String path, int uid, int gid, int mode) throws CacheException;

    /**
     * Create a symbolic link with a given path.
     *
     * @param subject Subject of user who invoked this method.
     * @param path full path of new object
     * @param dest target where symbolik link points to
     * @param uid uid of new entry or -1 for default
     * @param gid gid of new entry or -1 for default
     * @return PnfsId of newly created object
     * @throws CacheException
     */
    PnfsId createSymLink(Subject subject, String path, String dest, int uid, int gid) throws CacheException;

    /**
     * remove file or directory associated with given pnfsid
     * @param subject Subject of user who invoked this method.
     * @param pnfsId
     * @throws CacheException
     */
    void deleteEntry(Subject subject, PnfsId pnfsId) throws CacheException;

    /**
     * remove file or directory
     * @param subject Subject of user who invoked this method.
     * @param path
     * @throws CacheException
     */
    void deleteEntry(Subject subject, String path) throws CacheException;

    void renameEntry(Subject subject, PnfsId pnfsId, String newName,
                     boolean overwrite) throws CacheException;

    String pnfsidToPath(Subject subject, PnfsId pnfsId) throws CacheException;
    PnfsId pathToPnfsid(Subject subject, String path, boolean followLinks) throws CacheException;

    PnfsId getParentOf(Subject subject, PnfsId pnfsId) throws CacheException;

    void removeFileAttribute(Subject subject, PnfsId pnfsId, String attribute) throws CacheException;

    /**
     * Clears checksum value storage for the specific file and checksum type.
     * @param subject Subject of user who invoked this method.
     * @param type the type (or algorithm) of the checksum
     * @param pnfsId file
     */
    void removeChecksum(Subject subject, PnfsId pnfsId, ChecksumType type)
        throws CacheException;

    /**
     * add a cache location for a file
     * @param subject Subject of user who invoked this method.
     * @param pnfsId of the file
     * @param cacheLocation the new location
     * @throws CacheException
     */
    void addCacheLocation(Subject subject, PnfsId pnfsId, String cacheLocation) throws CacheException;

    /**
     * get all cache location of the file
     * @param subject Subject of user who invoked this method.
     * @param pnfsId of the file
     * @return list containing locations or empty list, if locations are unknown
     * @throws CacheException
     */
    List<String> getCacheLocation(Subject subject, PnfsId pnfsId) throws CacheException;

    /**
     * clear cache locations
     * @param subject Subject of user who invoked this method.
     * @param pnfsId of the file
     * @param cacheLocation, "*" forces to remove all known locations
     * @param removeIfLast remove entry from namespace if last known location is removed
     * @throws CacheException
     */
    void clearCacheLocation(Subject subject, PnfsId pnfsId, String cacheLocation, boolean removeIfLast) throws CacheException;

    /**
     * Get files attributes defined by <code>attr</code>. It's allowed to return less
     * attributes than requested. Empty <code>attr</code> equals to file existence check.
     *
     * @param subject Subject of user who invoked this method.
     * @param pnfsId of the file
     * @param attr array of requested attributes
     * @return
     */
    FileAttributes getFileAttributes(Subject subject, PnfsId pnfsId,
                                     Set<FileAttribute> attr)
        throws CacheException;

    /**
     * Set files attributes defined by <code>attr</code>.
     *
     * A NameSpaceProvider may choose to adjust or ignore requests to set
     * a FileAttribute.  This must be reflected in the returned value.
     *
     * @param subject Subject of user who invoked this method.
     * @param pnfsId of the file
     * @param attr array of requested attributes
     * @param acquire attributes to query after the update, if any.
     * @return the updated attributes selected by acquire
     */
    FileAttributes setFileAttributes(Subject subject, PnfsId pnfsId,
            FileAttributes attr, Set<FileAttribute> fetch) throws CacheException;

    /**
     * Lists the content of a directory. The content is returned as a
     * directory stream. An optional glob pattern andc optional
     * zero-based range can be used to limit the listing. For each
     * entry the ListHandler is invoked.
     *
     * The glob syntax is limitted to single character (question mark)
     * and multi character (asterix) wildcards. If glob is null, then
     * no filtering is applied.
     *
     * When a range is specified, only the part of the result set that
     * falls within the range is return. There is no guarantee that
     * the result set from two invocations is the same. For instance,
     * there is no guarantee that first listing [0;999] and then
     * listing [1000;1999] will actually cover the first 2000 entries:
     * Files may have been added or deleted from the directory, or the
     * ordering may have changed for some reason.
     *
     * @param subject Subject of user who invoked this method
     * @param path Path to directory to list
     * @param glob Pattern to limit the result set; may be null
     * @param range The range of entries to return; may be null
     * @param attrs The file attributes to query for each entry
     * @param handler Handler called for each entry
     */
    void list(Subject subject, String path, Glob glob, Range<Integer> range,
              Set<FileAttribute> attrs, ListHandler handler)
        throws CacheException;

    /**
     * Set up a temporary upload location for a file.
     *
     * The file can can be written to the returned path and then moved to its final
     * location using <code>commitUpload</code>. Alternatively the operation can be
     * cancelled by calling <code>cancelUpload</code>.
     *
     * @param subject the subject of user who invoked this method
     * @param path the path of the file to upload
     * @param uid the UID of new file or -1 for default
     * @param gid the GID of new file or -1 for default
     * @param mode the permission mask of the new entry or -1 for default
     * @param size optional expected size
     * @param al optional access latency of new file
     * @param rp optional retention policy of new file
     * @param spaceToken optional token of space reservation to write file to
     * @param options options specifying how the path should be created
     * @return A temporary upload path that must eventually be committed or cancelled
     */
    FsPath createUploadPath(Subject subject, FsPath path, int uid, int gid, int mode,
                            Long size, AccessLatency al, RetentionPolicy rp, String spaceToken,
                            Set<CreateOption> options)
            throws CacheException;

    /**
     * Move a file written to a temporary upload location to its final location.
     *
     * @param subject the subject of user who invoked this method
     * @param uploadPath the temporary path as returned by createUploadPath
     * @param path the path of file that is uploaded
     * @param options options specifying how the path should be committed
     * @return PnfsId of committed file
     */
    PnfsId commitUpload(Subject subject, FsPath uploadPath, FsPath path, Set<CreateOption> options) throws CacheException;

    /**
     * Remove temporary upload location.
     *
     * Removes the location previously created by a call to createUploadPath. Any
     * file that was written is deleted and future writes to the temporary path
     * will fail.
     *
     * @param subject the subject of user who invoked this method
     * @param uploadPath the temporary path as returned by createUploadPath
     * @param path the path of file that is uploaded
     */
    void cancelUpload(Subject subject, FsPath uploadPath, FsPath path) throws CacheException;
}
