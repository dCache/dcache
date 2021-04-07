package org.dcache.xrootd.door;

import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.vehicles.ProtocolInfo;
import dmg.cells.nucleus.CellPath;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import javax.security.auth.Subject;
import org.dcache.auth.Subjects;
import org.dcache.auth.attributes.Restriction;
import org.dcache.util.RedirectedTransfer;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.XrootdProtocolInfo;
import org.dcache.xrootd.protocol.XrootdProtocol;
import org.dcache.xrootd.tpc.XrootdTpcInfo;
import org.dcache.xrootd.util.ParseException;

public class XrootdTransfer extends RedirectedTransfer<InetSocketAddress>
{
    private UUID _uuid;
    private InetSocketAddress _doorAddress;
    private int _fileHandle;
    private Serializable _delegatedCredential;
    private final XrootdTpcInfo tpcInfo;

    public XrootdTransfer(PnfsHandler pnfs, Subject subject,
            Restriction restriction, FsPath path, Map<String,String> opaque) throws ParseException {
        super(pnfs, subject, restriction, path);
        tpcInfo = new XrootdTpcInfo(opaque);
        try {
            tpcInfo.setUid(Subjects.getUid(subject));
        } catch (NoSuchElementException e) {
            /** No UID, leave <code>null</code>*/
        }
        try {
            tpcInfo.setGid(Subjects.getPrimaryGid(subject));
        } catch (NoSuchElementException e) {
            /** No Primary GID, leave <code>null</code>*/
        }
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
        /*
         * In order to conform with xroot unix protocol if (a) we do TPC from a dCache source
         * (b) signed hash verification is on rather than TLS.
         */
        info.setTpcUid(tpcInfo.getUid());
        info.setTpcGid(tpcInfo.getGid());
        return info;
    }

    private XrootdProtocolInfo createXrootdProtocolInfo()
    {
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

    @Override
    protected FileAttributes fileAttributesForNameSpace()
    {
        FileAttributes attributes = super.fileAttributesForNameSpace();

        if (isTpcDestination()) {
            attributes.updateXattr("xdg.origin.url", buildSourceUrl());
        }

        return attributes;
    }

    private boolean isTpcDestination()
    {
        return tpcInfo.getSrc() != null;
    }

    private String buildSourceUrl()
    {
        StringBuilder sb = new StringBuilder("xroot://").append(tpcInfo.getSrcHost());
        Integer port = tpcInfo.getSrcPort();
        if (port != null && port != XrootdProtocol.DEFAULT_PORT) {
            sb.append(':').append(port);
        }
        String sourcePath = tpcInfo.getLfn() == null ? _path.toString() : tpcInfo.getLfn();
        if (!sourcePath.startsWith("/")) {
            sb.append('/');
        }
        sb.append(sourcePath);
        return sb.toString();
    }
}
