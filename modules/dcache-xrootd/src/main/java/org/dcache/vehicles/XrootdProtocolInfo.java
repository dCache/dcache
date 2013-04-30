package org.dcache.vehicles;

import java.net.InetSocketAddress;
import java.util.UUID;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.IpProtocolInfo;

import dmg.cells.nucleus.CellPath;

public class XrootdProtocolInfo implements IpProtocolInfo {

	private static final long serialVersionUID = -7070947404762513894L;

	private String _name;

	private int _minor;

	private int _major;

        private InetSocketAddress _clientSocketAddress;

	private CellPath _pathToDoor;

	private PnfsId _pnfsId;

	private int _xrootdFileHandle;

	private String _path;

	private UUID _uuid;

	private InetSocketAddress _doorAddress;

	public XrootdProtocolInfo(String protocol,  int major,int minor,
		InetSocketAddress clientAddress, CellPath pathToDoor, PnfsId pnfsID,
			int xrootdFileHandle, UUID uuid,
			InetSocketAddress doorAddress) {

		_name = protocol;
		_minor = minor;
		_major = major;
                _clientSocketAddress = clientAddress;
		_pathToDoor = pathToDoor;
		_pnfsId = pnfsID;
		_xrootdFileHandle = xrootdFileHandle;
		_uuid = uuid;
		_doorAddress = doorAddress;
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

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(getVersionString());
                sb.append(_clientSocketAddress.getAddress().getHostAddress());
		sb.append(":").append(_clientSocketAddress.getPort());

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

	public UUID getUUID() {
		return _uuid;
	}

	public InetSocketAddress getDoorAddress() {
		return _doorAddress;
	}

	public void setPath(String path)
	{
		_path = path;
	}

	public String getPath()
	{
		return _path;
	}

        @Override
        public InetSocketAddress getSocketAddress()
        {
            return _clientSocketAddress;
        }
}
