package diskCacheV111.services;

import org.globus.util.GlobusURL;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.transferManager.RemoteTransferManagerMessage;
import diskCacheV111.vehicles.transferManager.TransferManagerMessage;
import diskCacheV111.vehicles.IpProtocolInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
