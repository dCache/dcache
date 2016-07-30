/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2016 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package diskCacheV111.services.space;

import com.google.common.util.concurrent.ListenableFuture;

import diskCacheV111.vehicles.PoolAcceptFileMessage;
import diskCacheV111.vehicles.PoolIoFileMessage;
import diskCacheV111.vehicles.PoolManagerMessage;
import diskCacheV111.vehicles.PoolMgrSelectWritePoolMsg;

import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;

import org.dcache.poolmanager.RemotePoolManagerHandler;
import org.dcache.poolmanager.SerializablePoolManagerHandler;

/**
 * PoolManagerHandler published by space manager.
 *
 * Decides whether to submit requests to space manager or pass them on to a
 * PoolManagerHandler obtained from pool manager.
 */
public class SpaceManagerHandler extends RemotePoolManagerHandler
{
    private static final long serialVersionUID = -8954797295015774582L;

    protected final SerializablePoolManagerHandler inner;

    public SpaceManagerHandler(CellAddressCore destination, SerializablePoolManagerHandler inner)
    {
        super(destination);
        this.inner = inner;
    }

    @Override
    public <T extends PoolManagerMessage> ListenableFuture<T> sendAsync(CellEndpoint endpoint, T msg, long timeout)
    {
        return shouldIntercept(msg) ? super.sendAsync(endpoint, msg, timeout) : inner.sendAsync(endpoint, msg, timeout);
    }

    @Override
    public void send(CellEndpoint endpoint, CellMessage envelope, PoolManagerMessage msg)
    {
        if (shouldIntercept(msg)) {
            super.send(endpoint, envelope, msg);
        } else {
            inner.send(endpoint, envelope, msg);
        }
    }

    @Override
    public <T extends PoolIoFileMessage> ListenableFuture<T> startAsync(
            CellEndpoint endpoint, CellAddressCore pool, T msg, long timeout)
    {
        return shouldIntercept(msg)
               ? super.startAsync(endpoint, pool, msg, timeout)
               : inner.startAsync(endpoint, pool, msg, timeout);
    }

    @Override
    public void start(CellEndpoint endpoint, CellMessage envelope, PoolIoFileMessage msg)
    {
        if (shouldIntercept(msg)) {
            super.start(endpoint, envelope, msg);
        } else {
            inner.start(endpoint, envelope, msg);
        }
    }

    @Override
    public SerializablePoolManagerHandler.Version getVersion()
    {
        return new Version(super.getVersion(), inner.getVersion());
    }

    @Override
    public String toString()
    {
        return "spacemanager={" + super.toString() + ", " + inner.getVersion() + "}";
    }

    protected <T extends PoolManagerMessage> boolean shouldIntercept(T msg)
    {
        return msg instanceof PoolMgrSelectWritePoolMsg;
    }

    protected <T extends PoolIoFileMessage> boolean shouldIntercept(T msg)
    {
        return (msg instanceof PoolAcceptFileMessage) && msg.getFileAttributes().getStorageInfo().getKey("LinkGroupId") != null;
    }

    public static SerializablePoolManagerHandler.Version extractWrappedVersion(SerializablePoolManagerHandler.Version version)
    {
        return (version instanceof Version) ? ((Version) version).b : version;
    }

    protected static class Version implements SerializablePoolManagerHandler.Version
    {
        private final SerializablePoolManagerHandler.Version a;

        private final SerializablePoolManagerHandler.Version b;

        public Version(SerializablePoolManagerHandler.Version a, SerializablePoolManagerHandler.Version b)
        {
            this.a = a;
            this.b = b;
        }

        @Override
        public boolean equals(SerializablePoolManagerHandler.Version other)
        {
            return other instanceof Version && ((Version) other).a.equals(a) && ((Version) other).b.equals(b);
        }
    }
}
