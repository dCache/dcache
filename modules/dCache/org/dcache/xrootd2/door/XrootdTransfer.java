package org.dcache.xrootd2.door;

import javax.security.auth.Subject;
import java.net.InetSocketAddress;
import java.util.UUID;

import org.dcache.vehicles.XrootdProtocolInfo;
import org.dcache.util.RedirectedTransfer;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.FsPath;
import diskCacheV111.vehicles.ProtocolInfo;

import dmg.cells.nucleus.CellPath;

public class XrootdTransfer extends RedirectedTransfer<InetSocketAddress>
{
    private long _checksum;
    private UUID _uuid;
    private InetSocketAddress _doorAddress;
    private int _fileHandle;
    private boolean _uuidSupported;

    public XrootdTransfer(PnfsHandler pnfs, Subject subject, FsPath path) {
        super(pnfs, subject, path);
    }

    public synchronized void setFileHandle(int fileHandle) {
        _fileHandle = fileHandle;
    }

    public synchronized int getFileHandle() {
        return _fileHandle;
    }

    public synchronized void setChecksum(long checksum) {
        _checksum = checksum;
    }

    public synchronized void setUUID(UUID uuid) {
        _uuid = uuid;
    }

    public synchronized void setDoorAddress(InetSocketAddress doorAddress) {
        _doorAddress = doorAddress;
    }

    public synchronized void setUUIDSupported(boolean uuidSupported) {
        _uuidSupported = uuidSupported;
    }

    public boolean isUUIDSupported() {
        return _uuidSupported;
    }

    protected synchronized ProtocolInfo createProtocolInfo() {
        InetSocketAddress client = getClientAddress();
        XrootdProtocolInfo protocolInfo =
            new XrootdProtocolInfo(XrootdDoor.XROOTD_PROTOCOL_STRING,
                                   XrootdDoor.XROOTD_PROTOCOL_MAJOR_VERSION,
                                   XrootdDoor.XROOTD_PROTOCOL_MINOR_VERSION,
                                   client.getAddress().getHostName(),
                                   client.getPort(),
                                   new CellPath(getCellName(), getDomainName()),
                                   getPnfsId(),
                                   _fileHandle,
                                   _checksum,
                                   _uuid,
                                   _doorAddress);
        protocolInfo.setPath(_path.toString());
        return protocolInfo;
    }

    @Override
    protected ProtocolInfo createProtocolInfoForPoolManager() {
        return createProtocolInfo();
    }

    @Override
    protected ProtocolInfo createProtocolInfoForPool() {
        return createProtocolInfo();
    }
}
