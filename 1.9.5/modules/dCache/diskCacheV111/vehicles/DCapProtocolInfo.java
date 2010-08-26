/*
 * $Id: DCapProtocolInfo.java,v 1.8 2007-05-24 13:51:05 tigran Exp $
 */
package diskCacheV111.vehicles;

import diskCacheV111.util.VOInfo;
import dmg.cells.nucleus.CellPath;

public class DCapProtocolInfo implements IpProtocolInfo, GridProtocolInfo {

    private final String _name;
    private final int _minor;
    private final int _major;
    private final String[] _hosts;
    private final int _port;
    private long _transferTime = 0;
    private long _bytesTransferred = 0;
    private int _sessionId = 0;
    private boolean _writeAllowed = false;
    private boolean _fileCheck = true;
    private boolean _isPassive = false;
    private CellPath _door = null;
    private VOInfo _voInfo = null;

    private static final long serialVersionUID = 7432555710192378884L;

    public DCapProtocolInfo(String protocol, int major, int minor,
            String[] hosts, int port) {
        _name = protocol;
        _minor = minor;
        _major = major;
        _hosts = new String[hosts.length];
        System.arraycopy(hosts, 0, _hosts, 0, hosts.length);
        _port = port;
    }

    public DCapProtocolInfo(String protocol, int major, int minor, String host,
            int port) {

        this(protocol, major, minor, new String[] { host }, port);
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
    public String getProtocol() {
        return _name;
    }

    public int getMinorVersion() {
        return _minor;
    }

    public int getMajorVersion() {
        return _major;
    }

    public String getVersionString() {
        return _name + "-" + _major + "." + _minor;
    }

    //
    // and the private stuff
    //
    public int getPort() {
        return _port;
    }

    public String[] getHosts() {
        return _hosts;
    }

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
        StringBuffer sb = new StringBuffer();
        sb.append(getVersionString());
        for (int i = 0; i < _hosts.length; i++)
            sb.append(",").append(_hosts[i]);
        sb.append(":").append(_port);

        return sb.toString();
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

    // check file availability in the pool
    public boolean isFileCheckRequired() {
        return _fileCheck;
    }

    public void fileCheckRequaried(boolean isRequaried) {
        _fileCheck = isRequaried;
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

    public void setVOInfo( VOInfo voInfo) {
        _voInfo = voInfo;
    }

    public VOInfo getVOInfo() {
        return _voInfo;
    }
}
