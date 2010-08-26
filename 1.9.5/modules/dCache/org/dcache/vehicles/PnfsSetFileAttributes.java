package org.dcache.vehicles;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PnfsMessage;

/**
 * Vehicle for setting files combined attributes.
 *
 * @since 1.9.4
 */
public class PnfsSetFileAttributes extends PnfsMessage {

    private static final long serialVersionUID = -6750531802534981651L;

    private FileAttributes _fileAttributes;

    /**
     * Construct request by PnfsId.
     *
     * @param pnfsid
     * @param attr
     */
    public PnfsSetFileAttributes(PnfsId pnfsid, FileAttributes attr) {
        super(pnfsid);
        _fileAttributes = attr;
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
