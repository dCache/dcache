package diskCacheV111.vehicles;

import static com.google.common.base.Preconditions.checkArgument;
import static org.dcache.namespace.FileAttribute.HSM;
import static org.dcache.namespace.FileAttribute.PNFSID;
import static org.dcache.namespace.FileAttribute.STORAGECLASS;
import static org.dcache.namespace.FileAttribute.STORAGEINFO;

import diskCacheV111.util.PnfsId;
import java.util.EnumSet;
import javax.annotation.Nonnull;
import org.dcache.namespace.FileAttribute;
import org.dcache.vehicles.FileAttributes;

public class PoolMgrGetPoolMsg extends PoolManagerMessage {

    private static final long serialVersionUID = 8907604668091102254L;

    private final FileAttributes _fileAttributes;
    private Pool _pool;

    public PoolMgrGetPoolMsg(FileAttributes fileAttributes) {
        checkArgument(fileAttributes.isDefined(getRequiredAttributes()),
              "Required attributes are missing.");

        _fileAttributes = fileAttributes;
        setReplyRequired(true);
    }

    @Nonnull
    public FileAttributes getFileAttributes() {
        return _fileAttributes;
    }

    @Nonnull
    public StorageInfo getStorageInfo() {
        return _fileAttributes.getStorageInfo();
    }

    @Nonnull
    public PnfsId getPnfsId() {
        return _fileAttributes.getPnfsId();
    }

    public Pool getPool() {
        return _pool;
    }

    public void setPool(Pool pool) {
        _pool = pool;
    }

    @Override
    public String toString() {
        if (getReturnCode() == 0) {
            return "PnfsId=" + getPnfsId()
                  + ";StorageInfo=" + getStorageInfo() + ";"
                  + ((_pool == null) ? "" : _pool);
        } else {
            return super.toString();
        }
    }

    public static EnumSet<FileAttribute> getRequiredAttributes() {
        return EnumSet.of(PNFSID, STORAGEINFO, STORAGECLASS, HSM);
    }

    @Override
    public String getDiagnosticContext() {
        return super.getDiagnosticContext() + ' ' + getPnfsId();
    }
}
