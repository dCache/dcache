package org.dcache.vehicles;

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
 * @since 1.9.4
 */
public class PnfsSetFileAttributes extends PnfsMessage {

    private static final long serialVersionUID = -6750531802534981651L;

    private FileAttributes _fileAttributes;
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
     *
     * @param path
     * @param attr
     */
    public PnfsSetFileAttributes(String path, FileAttributes attr) {
        setPnfsPath(path);
        _fileAttributes = attr;
        _acquire = EnumSet.noneOf(FileAttribute.class);
    }

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

}
