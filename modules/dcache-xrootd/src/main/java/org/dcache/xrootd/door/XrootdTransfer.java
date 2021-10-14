package org.dcache.xrootd.door;

import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.vehicles.ProtocolInfo;
import dmg.cells.nucleus.CellPath;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.UUID;
import javax.security.auth.Subject;
import org.dcache.auth.attributes.Restriction;
import org.dcache.util.RedirectedTransfer;
import org.dcache.vehicles.XrootdProtocolInfo;
import org.dcache.xrootd.protocol.XrootdProtocol;

public class XrootdTransfer extends RedirectedTransfer<InetSocketAddress> {

    private UUID _uuid;
    private InetSocketAddress _doorAddress;
    private int _fileHandle;
    private Serializable _delegatedCredential;

    public XrootdTransfer(PnfsHandler pnfs, Subject subject, Restriction restriction, FsPath path) {
        super(pnfs, subject, restriction, path);
    }

    public synchronized void setFileHandle(int fileHandle) {
        _fileHandle = fileHandle;
    }

    public synchronized int getFileHandle() {
        return _fileHandle;
    }

    public synchronized void setUUID(UUID uuid) {
        _uuid = uuid;
    }

    public synchronized void setDoorAddress(InetSocketAddress doorAddress) {
        _doorAddress = doorAddress;
    }

    public void setDelegatedCredential(Serializable _delegatedCredential) {
        this._delegatedCredential = _delegatedCredential;
    }

    protected synchronized ProtocolInfo createProtocolInfo() {
        return createXrootdProtocolInfo();
    }

    @Override
    protected ProtocolInfo getProtocolInfoForPoolManager() {
        return createProtocolInfo();
    }

    @Override
    protected ProtocolInfo getProtocolInfoForPool() {
        XrootdProtocolInfo info = createXrootdProtocolInfo();
        info.setDelegatedCredential(_delegatedCredential);
        return info;
    }

    private XrootdProtocolInfo createXrootdProtocolInfo() {
        InetSocketAddress client = getClientAddress();
        return new XrootdProtocolInfo(XrootdDoor.XROOTD_PROTOCOL_STRING,
              XrootdProtocol.PROTOCOL_VERSION_MAJOR,
              XrootdProtocol.PROTOCOL_VERSION_MINOR,
              client,
              new CellPath(getCellName(), getDomainName()),
              getPnfsId(),
              _fileHandle,
              _uuid,
              _doorAddress);
    }
}
