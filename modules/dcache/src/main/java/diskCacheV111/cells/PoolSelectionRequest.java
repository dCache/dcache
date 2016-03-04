package diskCacheV111.cells;

import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.StorageInfo;


/**
   Pool Selection Request passed to Decision Unit within list of potential
   contidates for read or wite reques.
*/


public class PoolSelectionRequest {

	private final long _size;
	private final String _pnfsid;
	private final ProtocolInfo _protocolInfo;
	private final StorageInfo _storageInfo;

	PoolSelectionRequest(long size, String pnfsid, StorageInfo storageInfo, ProtocolInfo protocolInfo){
		_size = size;
		_pnfsid = pnfsid;
		_storageInfo = storageInfo;
		_protocolInfo = protocolInfo;
	}

    public String getPnfsId() {
		return _pnfsid;
	}

    public long getSize() {
		return _size;
	}

    public StorageInfo getStorageInfo(){
		return _storageInfo;
	}

    public ProtocolInfo getProtocolInfo(){
		return _protocolInfo;
	}


}
