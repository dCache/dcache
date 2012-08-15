package diskCacheV111.vehicles.transferManager;

import diskCacheV111.vehicles.IpProtocolInfo;
import diskCacheV111.util.FsPath;

import java.net.URI;

import org.dcache.auth.AuthorizationRecord;
import static com.google.common.base.Preconditions.*;

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

    public RemoteTransferManagerMessage(URI uri,
                                        FsPath pnfsPath,
                                        boolean store,
                                        Long credentialId,
                                        String spaceReservationId,
                                        boolean spaceReservationStrict,
                                        Long size,
                                        IpProtocolInfo protocolInfo)
    {
        super(pnfsPath.toString(),
              uri.toString(),
              store,
              credentialId,
              spaceReservationId,
              spaceReservationStrict,
              size);
        _protocolInfo = protocolInfo;
    }

    public IpProtocolInfo getProtocolInfo()
    {
        return _protocolInfo;
    }
}



