package diskCacheV111.cells;

import  diskCacheV111.vehicles.*;
import  diskCacheV111.util.*;
import  diskCacheV111.movers.* ;
import  dmg.cells.nucleus.*;
import  dmg.util.*;

import  java.util.*;
import  java.io.*;
import  java.net.*;
import  java.lang.reflect.* ;


/**
   Pool Selection Request passed to Decision Unit within list of potential
   contidates for read or wite reques.
*/


public class PoolSelectionRequest {

	private long _size;
	String _pnfsid;
	private ProtocolInfo _protocolInfo;
	private StorageInfo _storageInfo;

	PoolSelectionRequest(long size, String pnfsid, StorageInfo storageInfo, ProtocolInfo protocolInfo){
		_size = size;
		_pnfsid = pnfsid;
		_storageInfo = storageInfo;
		_protocolInfo = protocolInfo;
	};

	public String getPnfsId() {
		return _pnfsid;
	};

	public long getSize() {
		return _size;
	};

	public StorageInfo getStorageInfo(){
		return _storageInfo;
	};

	public ProtocolInfo getProtocolInfo(){
		return _protocolInfo;
	};



}
