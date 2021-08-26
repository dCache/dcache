package diskCacheV111.vehicles;

import diskCacheV111.util.PnfsId;

import dmg.cells.nucleus.CellAddressCore;

/**
 * An info message from some HSM-attached pool that describes a tape operation.
 * It extends the existing information message by including various
 * tape-specific details.
 */
public class StorageInfoMessage extends PnfsFileInfoMessage
{
    private static final long serialVersionUID = -4601114937008749384L;
    public static final String RESTORE_MSG_TYPE = "restore";
    public static final String STORE_MSG_TYPE = "store";

    private long _transferTime;
    private String _hsmInstance;
    private String _hsmType;
    private String _provider;

    public StorageInfoMessage(CellAddressCore address, PnfsId pnfsId, boolean restore)
    {
        super(restore ? RESTORE_MSG_TYPE : STORE_MSG_TYPE, "pool", address,
                pnfsId);
    }

    public void setTransferTime(long transferTime)
    {
        _transferTime = transferTime;
    }

    public long getTransferTime()
    {
        return _transferTime;
    }

    public void setHsmInstance(String name)
    {
        _hsmInstance = name;
    }

    public String getHsmInstance()
    {
        return _hsmInstance;
    }

    public void setHsmType(String type)
    {
        _hsmType = type;
    }

    public String getHsmType()
    {
        return _hsmType;
    }

    public void setHsmProvider(String provider)
    {
        _provider = provider;
    }

    public String getHsmProvider()
    {
        return _provider;
    }

    @Override
    public String toString()
    {
        return "StorageInfoMessage{" +
               "transferTime=" + _transferTime +
               "} " + super.toString();
    }

    @Override
    public void accept(InfoMessageVisitor visitor)
    {
        visitor.visit(this);
    }
}
