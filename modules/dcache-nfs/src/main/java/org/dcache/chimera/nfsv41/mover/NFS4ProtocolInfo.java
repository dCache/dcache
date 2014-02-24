package org.dcache.chimera.nfsv41.mover;

import java.net.InetSocketAddress;

import diskCacheV111.vehicles.IpProtocolInfo;

import org.dcache.chimera.nfs.v4.xdr.stateid4;

public class NFS4ProtocolInfo implements IpProtocolInfo {

    private static final long serialVersionUID = -2283394435195441798L;
    private static final String _protocolName = "NFS4";
    private static final int _minor = 1;
    private static final int _major = 4;
    private final stateid4 _stateId;
    private final InetSocketAddress _socketAddress;

    public NFS4ProtocolInfo(InetSocketAddress clientSocketAddress, stateid4 stateId) {
        _stateId = stateId;
        _socketAddress = clientSocketAddress;
    }

    //
    // the ProtocolInfo interface
    //
    @Override
    public String getProtocol() {
        return _protocolName;
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
        return _protocolName + "-" + _major + "." + _minor;
    }

    public boolean isFileCheckRequired() {
        return false;
    }

    public stateid4 stateId() {
        return _stateId;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getVersionString()).append(":").append(_socketAddress);

        return sb.toString();
    }

    @Override
    public InetSocketAddress getSocketAddress() {
        return _socketAddress;
    }
}
