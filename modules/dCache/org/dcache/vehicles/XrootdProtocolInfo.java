package org.dcache.vehicles;

import java.util.UUID;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.IpProtocolInfo;
import dmg.cells.nucleus.CellPath;

public class XrootdProtocolInfo implements IpProtocolInfo {

	private static final long serialVersionUID = -7070947404762513894L;

	private String _name;

	private int _minor;

	private int _major;

	private String[] _hosts;

	private int _port;

	private CellPath _pathToDoor;

	private PnfsId _pnfsId;

	private int _xrootdFileHandle;

	private long _checksum;

    private String _path;

    private UUID _uuid;

	public XrootdProtocolInfo(String protocol,  int major,int minor,
			String host, int port, CellPath pathToDoor, PnfsId pnfsID, int xrootdFileHandle, long checksum, UUID uuid) {

		_name = protocol;
		_minor = minor;
		_major = major;
		_hosts = new String[1];
		_hosts[0] = host;
		_port = port;
		_pathToDoor = pathToDoor;
		_pnfsId = pnfsID;
		_xrootdFileHandle = xrootdFileHandle;
		_checksum = checksum;
		_uuid = uuid;
	}

	public XrootdProtocolInfo(String protocol, int major, int minor,
			String[] hosts, int port, CellPath pathToDoor) {
		_name = protocol;
		_minor = minor;
		_major = major;
		_hosts = new String[hosts.length];
		System.arraycopy(hosts, 0, _hosts, 0, hosts.length);
		_port = port;
		_pathToDoor = pathToDoor;
	}

	public String[] getHosts() {
		return _hosts;
	}

	public int getPort() {
		return _port;
	}

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

	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(getVersionString());
		for (int i = 0; i < _hosts.length; i++)
			sb.append(",").append(_hosts[i]);
		sb.append(":").append(_port);

		return sb.toString();
	}

	public CellPath getXrootdDoorCellPath() {
		return _pathToDoor;
	}

	public void setXrootdDoorCellPath(CellPath toDoor) {
		_pathToDoor = toDoor;
	}

	public PnfsId getPnfsId() {
		return _pnfsId;
	}

	public int getXrootdFileHandle() {
		return _xrootdFileHandle;
	}

	public boolean isFileCheckRequired() {
//		we do it the fast way. The PoolMgr will not check whether a file is really on the pool where
//		it is supposed to be. This saves one message.
		return false;
	}

	/**
	 * Gets the checkum of the initial xrootd OpenRequest. It is used to check that the open received
	 * by the door and the (re-)open received by the mover are identical. Think of it as a hashcode but
	 * without the StreamID to be considered
	 * @return the checksum
	 */
	public long getChecksum() {
		return _checksum;
	}

	public UUID getUUID() {
	    return _uuid;
	}

    public void setPath(String path)
    {
        _path = path;
    }

    public String getPath()
    {
        return _path;
    }
}
