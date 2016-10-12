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
import com.google.common.util.concurrent.MoreExecutors;

import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.PoolAcceptFileMessage;
import diskCacheV111.vehicles.PoolIoFileMessage;
import diskCacheV111.vehicles.PoolManagerMessage;
import diskCacheV111.vehicles.PoolMgrSelectWritePoolMsg;

import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;

import org.dcache.cells.FutureCellMessageAnswerable;
import org.dcache.poolmanager.SerializablePoolManagerHandler;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * PoolManagerHandler published by space manager.
 *
 * Decides whether to submit requests to space manager or pass them on to a
 * PoolManagerHandler obtained from pool manager.
 */
public class SpaceManagerHandler implements SerializablePoolManagerHandler
{
    private static final long serialVersionUID = -8954797295015774582L;

    protected final CellAddressCore destination;
    protected final SerializablePoolManagerHandler inner;

    public SpaceManagerHandler(CellAddressCore destination, SerializablePoolManagerHandler inner)
    {
        this.destination = requireNonNull(destination);
        this.inner = inner;
    }

    @Override
    public <T extends PoolManagerMessage> ListenableFuture<T> sendAsync(CellEndpoint endpoint, T msg, long timeout)
    {
        if (shouldIntercept(msg)) {
            return submit(endpoint, new CellPath(destination), msg, timeout);
        } else {
            return inner.sendAsync(endpoint, msg, timeout);
        }
    }

    @Override
    public void send(CellEndpoint endpoint, CellMessage envelope, PoolManagerMessage msg)
    {
        if (shouldIntercept(msg)) {
            checkArgument(envelope.getSourcePath().hops() > 0, "Envelope is missing source address.");
            envelope.getDestinationPath().insert(this.destination);
            envelope.setMessageObject(msg);
            endpoint.sendMessage(envelope, CellEndpoint.SendFlag.PASS_THROUGH);
        } else {
            inner.send(endpoint, envelope, msg);
        }
    }

    @Override
    public <T extends PoolIoFileMessage> ListenableFuture<T> startAsync(
            CellEndpoint endpoint, CellAddressCore pool, T msg, long timeout)
    {
        if (shouldIntercept(msg)) {
            return submit(endpoint, new CellPath(destination, pool), msg, timeout);
        } else {
            return inner.startAsync(endpoint, pool, msg, timeout);
        }
    }

    @Override
    public void start(CellEndpoint endpoint, CellMessage envelope, PoolIoFileMessage msg)
    {
        if (shouldIntercept(msg)) {
            checkArgument(envelope.getSourcePath().hops() > 0, "Envelope is missing source address.");
            envelope.getDestinationPath().insert(destination);
            envelope.setMessageObject(msg);
            endpoint.sendMessage(envelope, CellEndpoint.SendFlag.PASS_THROUGH);
        } else {
            inner.start(endpoint, envelope, msg);
        }
    }

    @Override
    public SerializablePoolManagerHandler.Version getVersion()
    {
        return new Version(destination, inner.getVersion());
    }

    @Override
    public String toString()
    {
        return "spacemanager={" + destination + ", " + inner + "}";
    }

    @SuppressWarnings("unchecked")
    protected <T extends Message> ListenableFuture<T> submit(CellEndpoint endpoint, CellPath path, T msg, long timeout)
    {
        FutureCellMessageAnswerable<T> callback = new FutureCellMessageAnswerable<>((Class<T>) msg.getClass());
        endpoint.sendMessage(new CellMessage(path, msg), callback, MoreExecutors.directExecutor(), timeout);
        return callback;
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
        return (version instanceof Version) ? ((Version) version).version : version;
    }

    protected static class Version implements SerializablePoolManagerHandler.Version
    {
        private static final long serialVersionUID = -464748685222944300L;

        private final CellAddressCore destination;

        private final SerializablePoolManagerHandler.Version version;

        public Version(CellAddressCore destination, SerializablePoolManagerHandler.Version version)
        {
            this.destination = requireNonNull(destination);
            this.version = requireNonNull(version);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Version version1 = (Version) o;
            return destination.equals(version1.destination) && version.equals(version1.version);
        }

        @Override
        public int hashCode()
        {
            return 31 * destination.hashCode() + version.hashCode();
        }
    }
}
