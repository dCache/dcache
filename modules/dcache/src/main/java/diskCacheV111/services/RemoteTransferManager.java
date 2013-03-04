package diskCacheV111.services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import diskCacheV111.vehicles.IpProtocolInfo;
import diskCacheV111.vehicles.transferManager.RemoteTransferManagerMessage;
import diskCacheV111.vehicles.transferManager.TransferManagerMessage;

public class RemoteTransferManager extends TransferManager
{
    private static final Logger log =
        LoggerFactory.getLogger(RemoteTransferManager.class);

    public RemoteTransferManager(String cellName, String args)
        throws Exception
    {
        super(cellName, args);
    }

    @Override
    protected IpProtocolInfo getProtocolInfo(TransferManagerMessage message)
    {
        return ((RemoteTransferManagerMessage) message).getProtocolInfo();
    }
}
