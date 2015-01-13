package diskCacheV111.services;

import diskCacheV111.vehicles.IpProtocolInfo;
import diskCacheV111.vehicles.transferManager.RemoteTransferManagerMessage;
import diskCacheV111.vehicles.transferManager.TransferManagerMessage;

public class RemoteTransferManager extends TransferManager
{
    @Override
    protected IpProtocolInfo getProtocolInfo(TransferManagerMessage message)
    {
        return ((RemoteTransferManagerMessage) message).getProtocolInfo();
    }
}
