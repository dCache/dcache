package org.dcache.vehicles;

import java.util.Collection;

import diskCacheV111.movers.NetIFContainer;
import diskCacheV111.vehicles.Message;

public class XrootdDoorAdressInfoMessage extends Message {

	private static final long serialVersionUID = -5306759219838126273L;

	private int xrootdFileHandle;
	private Collection networkInterfaces;
	private int serverPort;
	private final boolean uuidEnabledPool;

	public XrootdDoorAdressInfoMessage(int xrootdFileHandle, int serverPort, Collection networkInterfaces) {

		this(xrootdFileHandle, serverPort, networkInterfaces, false);
	}

	public XrootdDoorAdressInfoMessage(int xrootdFileHandle,
	                                   int serverPort,
	                                   Collection<NetIFContainer> networkInterfaces,
	                                   boolean uuidEnabled) {
		this.xrootdFileHandle = xrootdFileHandle;
		this.serverPort = serverPort;

		this.networkInterfaces = networkInterfaces;
		this.uuidEnabledPool = uuidEnabled;
	}

	public Collection getNetworkInterfaces() {
		return networkInterfaces;
	}

	public int getXrootdFileHandle() {
		return xrootdFileHandle;
	}

	public int getServerPort() {
		return serverPort;
	}

	public boolean isUUIDEnabledPool() {
		return uuidEnabledPool;
	}

}
