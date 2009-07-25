package org.dcache.vehicles;

import diskCacheV111.vehicles.*;
import diskCacheV111.util.PnfsId;
import org.dcache.namespace.FileAttribute;

/**
 * Vehicle for get files combined attributes.
 *
 * @since 1.9.4
 */
public class PnfsGetFileAttributes extends PnfsMessage {

    private static final long serialVersionUID = -6750531802534981651L;

    private FileAttributes _fileAttributes;
    private FileAttribute[] _attributes;

    /**
     * Construct request by PnfsId.
     *
     * @param pnfsid
     * @param attr
     */
    public PnfsGetFileAttributes(PnfsId pnfsid, FileAttribute...attr) {
        super(pnfsid);
        _attributes = attr;
    }

    /**
     * Construct request by path.
     *
     * @param path
     * @param attr
     */
    public PnfsGetFileAttributes(String path, FileAttribute...attr) {
        super(path);
        _attributes = attr;
    }

    /**
     * Set file attributes.
     *
     * @param fileAttributes
     */
    public void setFileAttributes(FileAttributes fileAttributes) {
        _fileAttributes = fileAttributes;
    }

    /**
     * Get requested attributes. Note that PnfsManager may return less attributes than requested.
     *
     * @return
     */
    public FileAttributes getFileAttributes() {
        return _fileAttributes;
    }

    /**
     * Get array of requested {@link FileAttributes}. Empty array indicates that
     * client interested in file existence only.
     * @return
     */
    public FileAttribute[] getRequestedAttributes() {
        return _attributes;
    }
}
