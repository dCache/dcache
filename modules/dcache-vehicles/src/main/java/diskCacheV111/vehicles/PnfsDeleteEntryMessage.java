// $Id: PnfsDeleteEntryMessage.java,v 1.3 2005-01-14 13:56:25 tigran Exp $

package diskCacheV111.vehicles;

import java.util.EnumSet;
import java.util.Set;

import diskCacheV111.util.PnfsId;

import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.vehicles.FileAttributes;

import static java.util.Objects.requireNonNull;

public class PnfsDeleteEntryMessage extends PnfsMessage
{
    private static final long serialVersionUID = 7375207858020099787L;

    /**
     * Allowed FileTypes to delete. If the entry is not of this type,
     * then the operation will fail.
     */
    private final Set<FileType> _allowed;

    /**
     * Requested set of attributes for the item being deleted.
     */
    private final Set<FileAttribute> _requestedAttributes;

    private FileAttributes _attributes;

    public PnfsDeleteEntryMessage(String path)
    {
        this(null, path, EnumSet.allOf(FileType.class), EnumSet.noneOf(FileAttribute.class));
    }

    public PnfsDeleteEntryMessage(String path, Set<FileType> allowed)
    {
        this(null, path, allowed, EnumSet.noneOf(FileAttribute.class));
    }

    public PnfsDeleteEntryMessage(PnfsId pnfsId, String path,
                                  Set<FileType> allowed)
    {
        this(pnfsId, path, allowed, EnumSet.noneOf(FileAttribute.class));
    }

    public PnfsDeleteEntryMessage(PnfsId pnfsId, String path,
                                  Set<FileType> allowed, Set<FileAttribute> attr)
    {
        super(pnfsId);
        _allowed = allowed;
        _requestedAttributes = requireNonNull(attr);
        setPnfsPath(path);
        setReplyRequired(false);
    }

    public Set<FileType> getAllowedFileTypes()
    {
        return (_allowed == null) ? EnumSet.allOf(FileType.class) : _allowed;
    }

    public Set<FileAttribute> getRequestedAttributes()
    {
        return _requestedAttributes;
    }

    public void setFileAttributes(FileAttributes attributes)
    {
        _attributes = requireNonNull(attributes);
    }

    public FileAttributes getFileAttributes()
    {
        return _attributes;
    }
}
