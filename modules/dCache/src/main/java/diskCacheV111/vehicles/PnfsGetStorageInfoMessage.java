// $Id: PnfsGetStorageInfoMessage.java,v 1.7 2006-04-11 09:47:53 tigran Exp $
package diskCacheV111.vehicles;

import java.util.Set;

import diskCacheV111.util.*;
import org.dcache.vehicles.FileAttributes;
import org.dcache.namespace.FileAttribute;

import static org.dcache.namespace.FileAttribute.*;

public class PnfsGetStorageInfoMessage extends PnfsGetFileMetaDataMessage {

    private StorageInfo  _storageInfo = null ;
    private boolean      _followLinks = true;

    private static final long serialVersionUID = -2574949600859502380L;

    public PnfsGetStorageInfoMessage()
    {
        super();
        _attributes.add(STORAGEINFO);
    }

    public PnfsGetStorageInfoMessage(Set<FileAttribute> attr)
    {
        super(attr);
        _attributes.add(STORAGEINFO);
    }

    public PnfsGetStorageInfoMessage(PnfsId pnfsId)
    {
        super(pnfsId);
        _attributes.add(STORAGEINFO);
    }

    /* To ensure backwards compatibility with pre 1.9.6 clients, we
     * explicitly add attributes compatible with
     * PnfsGetStorageInfoMessage to the set of requested attributes if
     * the attribute set is null.
     */
    @Override
    public Set<FileAttribute> getRequestedAttributes()
    {
        Set<FileAttribute> attributes = _attributes;
        if (attributes == null) {
            attributes = super.getRequestedAttributes();
            attributes.add(STORAGEINFO);
        }
        return attributes;
    }

    @Override
    public void setFileAttributes(FileAttributes fileAttributes)
    {
        super.setFileAttributes(fileAttributes);

        /* For backwards compatibility with old versions we set this
         * field. We do this even though we don't use the field.
         */
        if (fileAttributes.isDefined(STORAGEINFO)) {
            _storageInfo = fileAttributes.getStorageInfo();
        }
    }

    public StorageInfo getStorageInfo()
    {
        return
            (_fileAttributes == null || !_fileAttributes.isDefined(STORAGEINFO))
            ? null
            : _fileAttributes.getStorageInfo();
    }

    public boolean resolve() { return this._followLinks; }
    public void setResolve(boolean followLinks)
    {
        _followLinks = followLinks;
        super.setResolve(followLinks);
    }
}
