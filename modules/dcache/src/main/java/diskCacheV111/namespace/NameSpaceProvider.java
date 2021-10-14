package diskCacheV111.namespace;

import com.google.common.collect.Range;
import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.NoAttributeCacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RetentionPolicy;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import javax.security.auth.Subject;
import org.dcache.namespace.CreateOption;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.namespace.ListHandler;
import org.dcache.util.ChecksumType;
import org.dcache.util.Glob;
import org.dcache.vehicles.FileAttributes;

/**
 * Any mechanism of storing dCache namespace must implement this interface.
 * <p>
 * Note that classes that implement NameSpaceProvider are NOT required to handle Restriction: it is
 * the caller's responsibility to enforce this.
 */
public interface NameSpaceProvider {

    /**
     * A Link represents the appearance of a file or directory within the namespace.  The parent is
     * always a directory.  Listing that directory will yield the target with the given name.
     */
    public class Link {

        private final PnfsId parent;
        private final String name;

        public Link(PnfsId parent, String name) {
            this.parent = parent;
            this.name = name;
        }

        public PnfsId getParent() {
            return parent;
        }

        public String getName() {
            return name;
        }
    }

    /**
     * The modes for setting an extended attribute.  The semantics are based on NFSv4 extended
     * attributes (see RFC 8276).
     */
    public enum SetExtendedAttributeMode {
        /**
         * Create a new extended attribute.  Fails with AttributeExistsCacheException if the
         * attribute already exists.
         */
        CREATE,

        /**
         * Replace an existing extended attribute.  Fails with NoAttributeCacheException if the
         * attribute does not already exist.
         */
        REPLACE,

        /**
         * Create a new extended attribute or replace the value, if the attribute exists.
         */
        EITHER
    }

    /**
     * When the mode field is not specified in createEntry, the mode is inherited from the parent
     * directory. When creating new directories, this UMASK is applied to the inherited mode.
     */
    int UMASK_DIR = 0777;

    /**
     * When the mode field is not specified in createEntry, the mode is inherited from the parent
     * directory. When creating new regular files, this UMASK is applied to the inherited mode.
     */
    int UMASK_FILE = 0666;

    /**
     * set if there is no old value
     */
    int SI_EXCLUSIVE = 0;
    /**
     * replace old value with new one
     */
    int SI_OVERWRITE = 1;
    /**
     * append new value to the old one
     */
    int SI_APPEND = 2;

    /**
     * Create a file for a given path and type.
     *
     * @param subject           Subject of user who invoked this method.
     * @param path              full path of new object
     * @param assignAttributes  attributes of the newly create file
     * @param requestAttributes attributes of the new file to return
     * @return the FileAttributes of newly created file
     * @throws CacheException
     */
    FileAttributes createFile(Subject subject, String path,
          FileAttributes assignAttributes, Set<FileAttribute> requestAttributes)
          throws CacheException;

    /**
     * Create a directory for a given path and type.
     *
     * @param subject    Subject of user who invoked this method.
     * @param path       full path of new object
     * @param attributes attributes of the newly create directory
     * @return PnfsId of newly created object
     * @throws CacheException
     */
    PnfsId createDirectory(Subject subject, String path,
          FileAttributes attributes) throws CacheException;

    /**
     * Create a symbolic link with a given path.
     *
     * @param subject    Subject of user who invoked this method.
     * @param path       full path of new object
     * @param dest       target where symbolik link points to
     * @param attributes attributes of the newly create symbolic link
     * @return PnfsId of newly created object
     * @throws CacheException
     */
    PnfsId createSymLink(Subject subject, String path, String dest,
          FileAttributes attributes) throws CacheException;

    /**
     * remove file or directory associated with given pnfsid
     *
     * @param subject Subject of user who invoked this method.
     * @param allowed Only delete if one of these file types.
     * @param pnfsId
     * @param attr    Attributes of deleted file or directory.
     * @return Requested attributes.
     * @throws CacheException
     */
    FileAttributes deleteEntry(Subject subject, Set<FileType> allowed, PnfsId pnfsId,
          Set<FileAttribute> attr) throws CacheException;

    /**
     * remove file or directory
     *
     * @param subject Subject of user who invoked this method.
     * @param allowed Only delete if one of these file types.
     * @param path
     * @param attr    Attributes of deleted file or directory
     * @return Requested attributes.
     * @throws CacheException
     */
    FileAttributes deleteEntry(Subject subject, Set<FileType> allowed, String path,
          Set<FileAttribute> attr) throws CacheException;

    /**
     * Remove file or directory. Path and PnfsID must describe the same object.
     *
     * @param subject Subject of user who invoked this method.
     * @param allowed Only delete if one of these file types.
     * @param pnfsId  PnfsID of file to delete
     * @param path    Path of file to delete
     * @param attr    Attributes of deleted file or directory.
     * @return Requested attributes.
     * @throws CacheException
     */
    FileAttributes deleteEntry(Subject subject, Set<FileType> allowed, PnfsId pnfsId,
          String path, Set<FileAttribute> attr) throws CacheException;

    void rename(Subject subject, @Nullable PnfsId pnfsId, String sourcePath, String destinationPath,
          boolean overwrite)
          throws CacheException;

    String pnfsidToPath(Subject subject, PnfsId pnfsId) throws CacheException;

    PnfsId pathToPnfsid(Subject subject, String path, boolean followLinks) throws CacheException;

    /**
     * Find the locations of the target file or directory within the namespace. If the target is a
     * directory then there is (at most) one location.  If the target is a file then more than one
     * location is returned if the file has hard links.
     *
     * @param subject Subject of the user who invoked this method
     * @param pnfsId  the target to locate
     * @return The locations where the target may be found.
     * @throws CacheException
     */
    Collection<Link> find(Subject subject, PnfsId pnfsId) throws CacheException;

    void removeFileAttribute(Subject subject, PnfsId pnfsId, String attribute)
          throws CacheException;

    /**
     * Clears checksum value storage for the specific file and checksum type.
     *
     * @param subject Subject of user who invoked this method.
     * @param type    the type (or algorithm) of the checksum
     * @param pnfsId  file
     */
    void removeChecksum(Subject subject, PnfsId pnfsId, ChecksumType type)
          throws CacheException;

    /**
     * add a cache location for a file
     *
     * @param subject       Subject of user who invoked this method.
     * @param pnfsId        of the file
     * @param cacheLocation the new location
     * @throws CacheException
     */
    void addCacheLocation(Subject subject, PnfsId pnfsId, String cacheLocation)
          throws CacheException;

    /**
     * get all cache location of the file
     *
     * @param subject Subject of user who invoked this method.
     * @param pnfsId  of the file
     * @return list containing locations or empty list, if locations are unknown
     * @throws CacheException
     */
    List<String> getCacheLocation(Subject subject, PnfsId pnfsId) throws CacheException;

    /**
     * clear cache locations
     *
     * @param subject        Subject of user who invoked this method.
     * @param pnfsId         of the file
     * @param cacheLocation, "*" forces to remove all known locations
     * @param removeIfLast   remove entry from namespace if last known location is removed
     * @throws CacheException
     */
    void clearCacheLocation(Subject subject, PnfsId pnfsId, String cacheLocation,
          boolean removeIfLast) throws CacheException;

    /**
     * Get files attributes defined by <code>attr</code>. It's allowed to return less attributes
     * than requested. Empty <code>attr</code> equals to file existence check.
     *
     * @param subject Subject of user who invoked this method.
     * @param pnfsId  of the file
     * @param attr    array of requested attributes
     * @return
     */
    FileAttributes getFileAttributes(Subject subject, PnfsId pnfsId,
          Set<FileAttribute> attr)
          throws CacheException;

    /**
     * Set files attributes defined by <code>attr</code>.
     * <p>
     * A NameSpaceProvider may choose to adjust or ignore requests to set a FileAttribute.  This
     * must be reflected in the returned value.
     *
     * @param subject Subject of user who invoked this method.
     * @param pnfsId  of the file
     * @param attr    array of requested attributes
     * @param fetch   attributes to query after the update, if any.
     * @return the updated attributes selected by acquire
     */
    FileAttributes setFileAttributes(Subject subject, PnfsId pnfsId,
          FileAttributes attr, Set<FileAttribute> fetch) throws CacheException;

    /**
     * Lists the content of a directory. The content is returned as a directory stream. An optional
     * glob pattern andc optional zero-based range can be used to limit the listing. For each entry
     * the ListHandler is invoked.
     * <p>
     * The glob syntax is limitted to single character (question mark) and multi character (asterix)
     * wildcards. If glob is null, then no filtering is applied.
     * <p>
     * When a range is specified, only the part of the result set that falls within the range is
     * return. There is no guarantee that the result set from two invocations is the same. For
     * instance, there is no guarantee that first listing [0;999] and then listing [1000;1999] will
     * actually cover the first 2000 entries: Files may have been added or deleted from the
     * directory, or the ordering may have changed for some reason.
     *
     * @param subject Subject of user who invoked this method
     * @param path    Path to directory to list
     * @param glob    Pattern to limit the result set; may be null
     * @param range   The range of entries to return; may be null
     * @param attrs   The file attributes to query for each entry
     * @param handler Handler called for each entry
     */
    void list(Subject subject, String path, Glob glob, Range<Integer> range,
          Set<FileAttribute> attrs, ListHandler handler)
          throws CacheException;

    /**
     * Set up a temporary upload location for a file.
     * <p>
     * The file can can be written to the returned path and then moved to its final location using
     * <code>commitUpload</code>. Alternatively the operation can be cancelled by calling
     * <code>cancelUpload</code>.
     *
     * @param subject    the subject of user who invoked this method
     * @param path       the path of the file to upload
     * @param rootPath   a base path relative to which the upload directory may optionally be
     *                   created
     * @param size       optional expected size
     * @param al         optional access latency of new file
     * @param rp         optional retention policy of new file
     * @param spaceToken optional token of space reservation to write file to
     * @param options    options specifying how the path should be created
     * @return A temporary upload path that must eventually be committed or cancelled
     */
    FsPath createUploadPath(Subject subject, FsPath path, FsPath rootPath,
          Long size, AccessLatency al, RetentionPolicy rp, String spaceToken,
          Set<CreateOption> options)
          throws CacheException;

    /**
     * Move a file written to a temporary upload location to its final location.
     *
     * @param subject    the subject of user who invoked this method
     * @param uploadPath the temporary path as returned by createUploadPath
     * @param path       the path of file that is uploaded
     * @param options    options specifying how the path should be committed
     * @param fetch      attributes of the file to return
     * @return Requested file attributes of the committed file.
     */
    FileAttributes commitUpload(Subject subject, FsPath uploadPath, FsPath path,
          Set<CreateOption> options, Set<FileAttribute> fetch) throws CacheException;

    /**
     * Remove temporary upload location.
     * <p>
     * Removes the location previously created by a call to createUploadPath. Any file that was
     * written is deleted and future writes to the temporary path will fail.
     *
     * @param subject     the subject of user who invoked this method
     * @param uploadPath  the temporary path as returned by createUploadPath
     * @param path        the path of file that is uploaded
     * @param attr        the desired attributes of the deleted files
     * @param explanation a short description explaining why the upload was cancelled.
     * @return the files deleted by the operation
     */
    Collection<FileAttributes> cancelUpload(Subject subject, FsPath uploadPath, FsPath path,
          Set<FileAttribute> attr, String explanation) throws CacheException;

    /**
     * Obtain the value of an extended attribute.
     *
     * @param subject The user making the request.
     * @param path    The file from which the extended attribute is read.
     * @param name    The ID of the extended attribute.
     * @return The contents of this extended attribute.
     * @throws FileNotFoundCacheException     if the path does not exist.
     * @throws PermissionDeniedCacheException if the user is not allowed to read this attribute.
     * @throws CacheException                 a generic failure in reading the attribute.
     */
    byte[] readExtendedAttribute(Subject subject, FsPath path, String name)
          throws CacheException;

    /**
     * Create or modify the value of an extended attribute.
     *
     * @param subject The user making the request.
     * @param path    The file for which the extended attribute is created or modified.
     * @param name    The ID of the extended attribute.
     * @param value   The value of the attribute if the operation is successful.
     * @param mode    How the attribute value is to be updated.
     * @throws FileNotFoundCacheException     if the path does not exist.
     * @throws PermissionDeniedCacheException if the user is not allowed to modify this attribute.
     * @throws AttributeExistsCacheException  if mode is SetExtendedAttributeMode.CREATE and the
     *                                        attribute exists.
     * @throws NoAttributeCacheException      if mode is SetExtendedAttributeMode.MODIFY and the
     *                                        attribute does not exist.
     * @throws CacheException                 a generic failure in modify the attribute.
     */
    void writeExtendedAttribute(Subject subject, FsPath path, String name,
          byte[] value, SetExtendedAttributeMode mode)
          throws CacheException;

    /**
     * List all currently existing extended attributes for a file.
     *
     * @param subject The user making the request.
     * @param path    The file from which all extended attribute are listed.
     * @throws FileNotFoundCacheException     if the path does not exist.
     * @throws PermissionDeniedCacheException if the user is not allowed to list attributes of this
     *                                        file.
     * @throws CacheException                 a generic failure in listing the attributes.
     */
    Set<String> listExtendedAttributes(Subject subject, FsPath path)
          throws CacheException;

    /**
     * Remove an extended attribute from a file.
     *
     * @param subject The user making the request.
     * @param path    The file from which the extended attribute is deleted.
     * @param name    The extended attribute to remove.
     * @throws FileNotFoundCacheException     if the path does not exist.
     * @throws PermissionDeniedCacheException if the user is not allowed to remove the attribute.
     * @throws NoAttributeCacheException      if the attribute does not exist.
     * @throws CacheException                 a generic failure in removing the attribute.
     */
    void removeExtendedAttribute(Subject subject, FsPath path, String name)
          throws CacheException;

    /**
     * Update FS stat cache.
     */
    void updateFsStat() throws CacheException;

    /**
     * Remove a label from a file.
     *
     * @param subject The user making the request.
     * @param path    The file from which the label is deleted.
     * @param label   The labelto remove.
     * @throws FileNotFoundCacheException     if the path does not exist.
     * @throws PermissionDeniedCacheException if the user is not allowed to remove the label.
     * @throws CacheException                 a generic failure in removing the attribute.
     */
    void removeLabel(Subject subject, FsPath path, String label) throws CacheException;


}
