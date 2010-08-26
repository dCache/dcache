/*
 * $Id:NFS4ProtocolInfo.java 140 2007-06-07 13:44:55Z tigran $
 */
package org.dcache.chimera.nfsv41.mover;

/*
 * dCache specific part
 */
import java.net.InetAddress;

import org.dcache.chimera.nfs.v4.xdr.stateid4;

import diskCacheV111.vehicles.IpProtocolInfo;
import dmg.cells.nucleus.CellPath;

public class NFS4ProtocolInfo implements IpProtocolInfo {

	private static final long serialVersionUID = -2283394435195441798L;


	private static final String _protocolName = "NFS4";
	private static final int _minor = 1;
	private static final int _major = 4;
	private final InetAddress _clientIp;
	private final stateid4 _stateId;

	private String[] _knownNames = null;

	private CellPath _door = null;


	public NFS4ProtocolInfo(InetAddress clientIp, stateid4 stateId) {
		_clientIp = clientIp;
		_stateId = stateId;
	}

	public String[] getHosts() {

		if( _knownNames == null ) {
			_knownNames = new String[1];
			_knownNames[0] = _clientIp.getHostName();
		}
		return _knownNames;
	}

	public int getPort() {
		return 0;
	}

	//
	// the ProtocolInfo interface
	//
	public String getProtocol() {
		return _protocolName;
	}

	public int getMinorVersion() {
		return _minor;
	}

	public int getMajorVersion() {
		return _major;
	}

	public String getVersionString() {
		return _protocolName + "-" + _major + "." + _minor;
	}

	public boolean isFileCheckRequired() {
		return false;
	}

    public CellPath door() {
        return _door;
    }

    public void door(CellPath door) {
        _door = door;
    }

    public stateid4 stateId() {
        return _stateId;
    }

}

