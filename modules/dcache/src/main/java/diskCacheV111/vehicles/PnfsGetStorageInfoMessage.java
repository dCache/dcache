package diskCacheV111.vehicles;

import java.util.Set;

import diskCacheV111.util.FileMetaData;
import diskCacheV111.util.PnfsId;

import org.dcache.namespace.FileAttribute;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.PnfsGetFileAttributes;

import static org.dcache.namespace.FileAttribute.*;

public class PnfsGetStorageInfoMessage extends PnfsGetFileAttributes
{
    private static final long serialVersionUID = -2574949600859502380L;

    public PnfsGetStorageInfoMessage(Set<FileAttribute> attr)
    {
        this();
        _attributes.addAll(attr);
    }

    public PnfsGetStorageInfoMessage()
    {
        this((PnfsId) null);
    }

    public PnfsGetStorageInfoMessage(PnfsId pnfsId)
    {
        super(pnfsId, FileMetaData.getKnownFileAttributes());
        _attributes.add(PNFSID);
        _attributes.add(STORAGEINFO);
        _attributes.add(ACCESS_LATENCY);
        _attributes.add(RETENTION_POLICY);
        _attributes.add(SIZE);
        setReplyRequired(true);
    }

    @Override
    public void setFileAttributes(FileAttributes fileAttributes)
    {
        super.setFileAttributes(fileAttributes);
    }

    public StorageInfo getStorageInfo()
    {
        return
            (_fileAttributes == null || !_fileAttributes.isDefined(STORAGEINFO))
            ? null
            : _fileAttributes.getStorageInfo();
    }
}
