package org.dcache.chimera.nfsv41.mover;

import com.google.common.net.InetAddresses;
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
    private final byte[] _fh;

    public NFS4ProtocolInfo(InetSocketAddress clientSocketAddress, stateid4 stateId, byte[] fh) {
        _stateId = stateId;
        _socketAddress = clientSocketAddress;
        _fh = fh;
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

    public byte[] getNfsFileHandle() {
        return _fh;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getVersionString()).append(":")
                .append(InetAddresses.toAddrString(_socketAddress.getAddress()))
                .append(':')
                .append(_socketAddress.getPort());

        return sb.toString();
    }

    @Override
    public InetSocketAddress getSocketAddress() {
        return _socketAddress;
    }
}
