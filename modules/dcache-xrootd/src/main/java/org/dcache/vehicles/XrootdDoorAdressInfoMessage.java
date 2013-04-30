package org.dcache.vehicles;

import java.net.InetSocketAddress;

import diskCacheV111.vehicles.Message;

import static com.google.common.base.Preconditions.checkNotNull;

public class XrootdDoorAdressInfoMessage extends Message
{
    private static final long serialVersionUID = -5306759219838126273L;

    private final int xrootdFileHandle;

    private InetSocketAddress socketAddress;

    public XrootdDoorAdressInfoMessage(int xrootdFileHandle, InetSocketAddress socketAddress)
    {
        this.xrootdFileHandle = xrootdFileHandle;
        this.socketAddress = checkNotNull(socketAddress);
    }

    public int getXrootdFileHandle()
    {
        return xrootdFileHandle;
    }

    public InetSocketAddress getSocketAddress()
    {
        return socketAddress;
    }
}
