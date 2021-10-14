package org.dcache.vehicles;

import static java.util.Arrays.asList;

import com.google.common.collect.Sets;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.IpProtocolInfo;
import dmg.cells.nucleus.CellPath;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.EnumSet;
import java.util.UUID;

public class XrootdProtocolInfo implements IpProtocolInfo {

    private static final long serialVersionUID = -7070947404762513894L;

    public enum Flags {
        POSC
    }

    private final String _name;

    private final int _minor;

    private final int _major;

    private final CellPath _pathToDoor;

    private final PnfsId _pnfsId;

    private final int _xrootdFileHandle;

    private final UUID _uuid;

    private final InetSocketAddress _doorAddress;

    private final EnumSet<Flags> _flags;

    private InetSocketAddress _clientSocketAddress;

    private Serializable delegatedCredential;

    private Long _tpcUid;

    private Long _tpcGid;

    public XrootdProtocolInfo(String protocol, int major, int minor,
          InetSocketAddress clientAddress, CellPath pathToDoor, PnfsId pnfsID,
          int xrootdFileHandle, UUID uuid,
          InetSocketAddress doorAddress, Flags... flags) {
        _name = protocol;
        _minor = minor;
        _major = major;
        _clientSocketAddress = clientAddress;
        _pathToDoor = pathToDoor;
        _pnfsId = pnfsID;
        _xrootdFileHandle = xrootdFileHandle;
        _uuid = uuid;
        _doorAddress = doorAddress;
        _flags = Sets.newEnumSet(asList(flags), Flags.class);
    }

    public Serializable getDelegatedCredential() {
        return delegatedCredential;
    }

    public Long getTpcUid() {
        return _tpcUid;
    }

    public Long getTpcGid() {
        return _tpcGid;
    }

    @Override
    public String getProtocol() {
        return _name;
    }

    @Override
    public int getMinorVersion() {
        return _minor;
    }

    @Override
    public int getMajorVersion() {
        return _major;
    }

    @Override
    public String getVersionString() {
        return _name + "-" + _major + "." + _minor;
    }

    @Override
    public String toString() {
        return getVersionString() + ':' + _clientSocketAddress.getAddress().getHostAddress() + ":"
              + _clientSocketAddress.getPort();
    }

    public CellPath getXrootdDoorCellPath() {
        return _pathToDoor;
    }

    public PnfsId getPnfsId() {
        return _pnfsId;
    }

    public int getXrootdFileHandle() {
        return _xrootdFileHandle;
    }

    public UUID getUUID() {
        return _uuid;
    }

    public InetSocketAddress getDoorAddress() {
        return _doorAddress;
    }

    @Override
    public InetSocketAddress getSocketAddress() {
        return _clientSocketAddress;
    }

    public void setSocketAddress(InetSocketAddress address) {
        _clientSocketAddress = address;
    }

    public EnumSet<Flags> getFlags() {
        return _flags;
    }

    public void setDelegatedCredential(Serializable delegatedCredential) {
        this.delegatedCredential = delegatedCredential;
    }

    public void setTpcUid(Long tpcUid) {
        _tpcUid = tpcUid;
    }

    public void setTpcGid(Long tpcGid) {
        _tpcGid = tpcGid;
    }
}
