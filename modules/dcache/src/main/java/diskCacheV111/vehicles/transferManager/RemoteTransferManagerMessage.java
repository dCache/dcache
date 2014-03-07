package diskCacheV111.vehicles.transferManager;

import java.net.URI;

import diskCacheV111.util.FsPath;
import diskCacheV111.vehicles.IpProtocolInfo;

public class RemoteTransferManagerMessage extends TransferManagerMessage
{
    private static final long serialVersionUID = -7005244124485666180L;
    private IpProtocolInfo _protocolInfo;

    public RemoteTransferManagerMessage(URI uri,
                                        FsPath pnfsPath,
                                        boolean store,
                                        Long credentialId,
                                        IpProtocolInfo protocolInfo)
    {
        super(pnfsPath.toString(), uri.toString(), store, credentialId);
        _protocolInfo = protocolInfo;
    }

    public IpProtocolInfo getProtocolInfo()
    {
        return _protocolInfo;
    }
}



