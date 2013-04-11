package org.dcache.vehicles;

import com.google.common.collect.Iterables;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.InetSocketAddress;
import java.util.Collection;

import diskCacheV111.movers.NetIFContainer;
import diskCacheV111.vehicles.Message;

import static com.google.common.base.Preconditions.checkNotNull;

public class XrootdDoorAdressInfoMessage extends Message
{
    private static final long serialVersionUID = -5306759219838126273L;

    private final int xrootdFileHandle;
    @Deprecated // Remove in 2.7
    private Collection<NetIFContainer> networkInterfaces;
    @Deprecated // Remove in 2.7
    private int serverPort;

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

    private void readObject(ObjectInputStream in)
            throws IOException, ClassNotFoundException
    {
        in.defaultReadObject();
        if (socketAddress == null) {
            socketAddress = new InetSocketAddress(Iterables
                    .get(Iterables.get(networkInterfaces, 0).getInetAddresses(), 0), serverPort);
        }
    }
}
