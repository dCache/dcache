package diskCacheV111.vehicles;

import javax.annotation.Nonnull;

import java.util.EnumSet;

import diskCacheV111.util.PnfsId;

import dmg.cells.nucleus.CellAddressCore;

import org.dcache.namespace.FileAttribute;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Preconditions.checkArgument;
import static org.dcache.namespace.FileAttribute.*;

public class PoolMgrGetPoolMsg extends PoolManagerMessage
{
    private static final long serialVersionUID = 8907604668091102254L;

    private final FileAttributes _fileAttributes;
    private String _poolName;
    private CellAddressCore _poolAddress;

    public PoolMgrGetPoolMsg(FileAttributes fileAttributes)
    {
        checkArgument(fileAttributes.isDefined(getRequiredAttributes()), "Required attributes are missing.");

	_fileAttributes = fileAttributes;
	setReplyRequired(true);
    }

    @Nonnull
    public FileAttributes getFileAttributes()
    {
	return _fileAttributes;
    }

    @Nonnull
    public StorageInfo getStorageInfo()
    {
	return _fileAttributes.getStorageInfo();
    }

    @Nonnull
    public PnfsId getPnfsId()
    {
	return _fileAttributes.getPnfsId();
    }

    public String getPoolName()
    {
	return _poolName;
    }

    public void setPoolName(String poolName)
    {
	_poolName = poolName;
    }

    public void setPoolAddress(CellAddressCore poolAddress)
    {
        _poolAddress = poolAddress;
    }

    public CellAddressCore getPoolAddress()
    {
        return _poolAddress;
    }

    @Override
    public String toString()
    {
        if (getReturnCode() == 0) {
            return "PnfsId=" + getPnfsId()
                    + ";StorageInfo=" + getStorageInfo()
                    + ((_poolName == null) ? "" : ";PoolName=" + _poolName)
                    + ((_poolAddress == null) ? "" : "PoolAddress=" + _poolAddress);
        } else {
            return super.toString();
        }
    }

    public static EnumSet<FileAttribute> getRequiredAttributes()
    {
        return EnumSet.of(PNFSID, STORAGEINFO, STORAGECLASS, HSM);
    }

    @Override
    public String getDiagnosticContext() {
        return super.getDiagnosticContext() + ' ' + getPnfsId();
    }
}
