// $Id: PnfsGetStorageInfoMessage.java,v 1.7 2006-04-11 09:47:53 tigran Exp $
package diskCacheV111.vehicles;

import java.util.Set;

import diskCacheV111.util.PnfsId;

import org.dcache.namespace.FileAttribute;
import org.dcache.vehicles.FileAttributes;

import static org.dcache.namespace.FileAttribute.*;

public class PnfsGetStorageInfoMessage extends PnfsGetFileMetaDataMessage {

    private boolean      _followLinks = true;

    private static final long serialVersionUID = -2574949600859502380L;

    public PnfsGetStorageInfoMessage()
    {
        super();
        _attributes.add(STORAGEINFO);
        _attributes.add(ACCESS_LATENCY);
        _attributes.add(RETENTION_POLICY);
        _attributes.add(SIZE);
    }

    public PnfsGetStorageInfoMessage(Set<FileAttribute> attr)
    {
        super(attr);
        _attributes.add(STORAGEINFO);
        _attributes.add(ACCESS_LATENCY);
        _attributes.add(RETENTION_POLICY);
        _attributes.add(SIZE);
    }

    public PnfsGetStorageInfoMessage(PnfsId pnfsId)
    {
        super(pnfsId);
        _attributes.add(STORAGEINFO);
        _attributes.add(ACCESS_LATENCY);
        _attributes.add(RETENTION_POLICY);
        _attributes.add(SIZE);
    }

    @Override
    public void setFileAttributes(FileAttributes fileAttributes)
    {
        // For compatibility with pre 2.6 pools: 'rh restore' issues a PnfsGetStorageInfoMessage
        // and expects the size to be part of the storage info.
        if (fileAttributes.isDefined(STORAGEINFO) && fileAttributes.isDefined(SIZE)) {
            fileAttributes.getStorageInfo().setLegacySize(fileAttributes.getSize());
        }
        super.setFileAttributes(fileAttributes);
    }

    public StorageInfo getStorageInfo()
    {
        return
            (_fileAttributes == null || !_fileAttributes.isDefined(STORAGEINFO))
            ? null
            : _fileAttributes.getStorageInfo();
    }

    @Override
    public boolean resolve() { return this._followLinks; }
    @Override
    public void setResolve(boolean followLinks)
    {
        _followLinks = followLinks;
        super.setResolve(followLinks);
    }
}
