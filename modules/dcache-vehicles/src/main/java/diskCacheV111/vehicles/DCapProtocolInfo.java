package diskCacheV111.vehicles;

import java.net.InetSocketAddress;

import dmg.cells.nucleus.CellPath;

public class DCapProtocolInfo implements IpProtocolInfo {

    private final String _name;
    private final int _minor;
    private final int _major;
    private final InetSocketAddress _addr;
    private long _transferTime;
    private long _bytesTransferred;
    private int _sessionId;
    private boolean _writeAllowed;
    private boolean _isPassive;
    private CellPath _door;

    private static final long serialVersionUID = 7432555710192378884L;

    public DCapProtocolInfo(String protocol, int major, int minor,
            InetSocketAddress addr) {
        _name = protocol;
        _minor = minor;
        _major = major;
        _addr = addr;
    }

    public int getSessionId() {
        return _sessionId;
    }

    public void setSessionId(int sessionId) {
        _sessionId = sessionId;
    }

    //
    // the ProtocolInfo interface
    //
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
        return _name + '-' + _major + '.' + _minor;
    }

    //
    // and the private stuff
    //
    public void setBytesTransferred(long bytesTransferred) {
        _bytesTransferred = bytesTransferred;
    }

    public void setTransferTime(long transferTime) {
        _transferTime = transferTime;
    }

    public long getTransferTime() {
        return _transferTime;
    }

    public long getBytesTransferred() {
        return _bytesTransferred;
    }

    //
    @Override
    public String toString() {
        String sb = getVersionString() + ',' +
                    _addr.getAddress().getHostAddress() +
                    ':' + _addr.getPort();

        return sb;
    }

    //
    // io mode
    //
    public boolean isWriteAllowed() {
        return _writeAllowed;
    }

    public void setAllowWrite(boolean allow) {
        _writeAllowed = allow;
    }

    public boolean isPassive() {
        return _isPassive;
    }

    public void isPassive(boolean passive) {
        _isPassive = passive;
    }

    public CellPath door() {
        return _door;
    }

    public void door(CellPath door) {
        _door = door;
    }

    @Override
    public InetSocketAddress getSocketAddress() {
        return _addr;
    }
}
