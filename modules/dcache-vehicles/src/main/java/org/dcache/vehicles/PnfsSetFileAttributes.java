package org.dcache.vehicles;

import java.util.Collection;
import java.util.EnumSet;
import java.util.Set;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PnfsMessage;
import org.dcache.namespace.FileAttribute;

/**
 * Vehicle for setting files combined attributes.
 *
 * On successful reply, {@code #getFileAttributes} returns the updated.
 * attribute values.  Be aware that the NameSpaceProvider may decline to update
 * arguments or update them to a different value; currently, this effect is
 * limited to FileAttribute.ACCESS_TIME and FileAttribute.CHANGE_TIME.
 *
 * Sending this message with a FileAttributes object containing an
 * FileAttribute.XATTR value will establish the specified key-value pairs as
 * extended attributes of the target.  If the target already has extended
 * attributes then those with the same name are overwritten and the others are
 * left unmodified.
 *
 * If the set of FileAttribute values to acquire includes FileAttribute.XATTR
 * then all extended attributes are included in the reply.
 *
 * @since 1.9.4
 */
public class PnfsSetFileAttributes extends PnfsMessage {

    private static final long serialVersionUID = -6750531802534981651L;

    private FileAttributes _fileAttributes;
    private transient Collection<String> _locations;
    private final Set<FileAttribute> _acquire;

    /**
     * Construct request by PnfsId.
     *
     * @param pnfsid
     * @param attr
     */
    public PnfsSetFileAttributes(PnfsId pnfsid, FileAttributes attr) {
        super(pnfsid);
        _fileAttributes = attr;
        _acquire = EnumSet.noneOf(FileAttribute.class);
    }

    public PnfsSetFileAttributes(PnfsId pnfsid, FileAttributes attr,
            Set<FileAttribute> acquire) {
        super(pnfsid);
        _fileAttributes = attr;
        _acquire = acquire;
    }

    /**
     * Construct request by path.
     */
    public PnfsSetFileAttributes(String path, FileAttributes attr, Set<FileAttribute> acquire) {
        setPnfsPath(path);
        _fileAttributes = attr;
        _acquire = acquire;
    }

    public Set<FileAttribute> getAcquire() {
        return _acquire == null ? EnumSet.noneOf(FileAttribute.class) : _acquire;
    }

    /**
     * Update the FileAttributes.
     */
    public void setFileAttributes(FileAttributes attributes) {
        _fileAttributes = attributes;
    }

    /**
     * Get requested attributes.
     *
     * @return
     */
    public FileAttributes getFileAttributes() {
        return _fileAttributes;
    }

    /**
     * @return the locations included by the sender.
     */
    public Collection<String> getLocations() {
        return _locations;
    }

    /**
     * Store for post-processing.
     *
     * @param locations included by the sender.
     */
    public void setLocations(Collection<String> locations) {
        _locations = locations;
    }
}
