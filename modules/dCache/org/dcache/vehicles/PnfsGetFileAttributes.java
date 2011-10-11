package org.dcache.vehicles;

import diskCacheV111.vehicles.*;
import diskCacheV111.util.PnfsId;
import org.dcache.namespace.FileAttribute;
import java.util.Set;


/**
 * Vehicle for get files combined attributes.
 *
 * @since 1.9.4
 */
public class PnfsGetFileAttributes extends PnfsMessage {

    private static final long serialVersionUID = -6750531802534981651L;

    protected FileAttributes _fileAttributes;
    protected Set<FileAttribute> _attributes;

    /**
     * Construct request by PnfsId.
     *
     * @param pnfsid
     * @param attr
     */
    public PnfsGetFileAttributes(PnfsId pnfsid, Set<FileAttribute> attr) {
        super(pnfsid);
        _attributes = attr;
        setReplyRequired(true);
    }

    /**
     * Construct request by path.
     *
     * @param path
     * @param attr
     */
    public PnfsGetFileAttributes(String path, Set<FileAttribute> attr) {
        super();
        setPnfsPath(path);
        _attributes = attr;
        setReplyRequired(true);
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
     * Get set of requested {@link FileAttributes}. An empty set
     * indicates that client interested in file existence only.
     * @return
     */
    public Set<FileAttribute> getRequestedAttributes() {
        return _attributes;
    }

    @Override
    public boolean invalidates(Message message)
    {
        return false;
    }

    @Override
    public boolean fold(Message message)
    {
        if (message instanceof PnfsGetFileAttributes) {
            PnfsId pnfsId = getPnfsId();
            String path = getPnfsPath();
            Set<FileAttribute> requested = getRequestedAttributes();
            PnfsGetFileAttributes other =
                (PnfsGetFileAttributes) message;
            if ((pnfsId == null || pnfsId.equals(other.getPnfsId())) &&
                (path == null || path.equals(other.getPnfsPath())) &&
                (getSubject().equals(other.getSubject())) &&
                (other.getRequestedAttributes().containsAll(requested))) {
                setPnfsId(other.getPnfsId());
                setPnfsPath(other.getPnfsPath());
                setFileAttributes(other.getFileAttributes());
                return true;
            }
        }

        return false;
    }

    @Override
    public String toString()
    {
        return super.toString() + ";" +
            ((_fileAttributes == null)
             ? "[noMetaData]"
             : _fileAttributes.toString());
    }
}
