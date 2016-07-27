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
package org.dcache.poolmanager;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.PoolIoFileMessage;
import diskCacheV111.vehicles.PoolManagerMessage;

import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;

import org.dcache.cells.FutureCellMessageAnswerable;

import static java.util.Objects.requireNonNull;

/**
 * Implementation of PoolManagerHandler that delegates all requests to a pool manager.
 */
public class RemotePoolManagerHandler implements SerializablePoolManagerHandler
{
    private static final long serialVersionUID = -7662812702828746542L;

    protected final CellAddressCore destination;

    public RemotePoolManagerHandler(CellAddressCore destination)
    {
        this.destination = requireNonNull(destination);
    }

    @Override
    public <T extends PoolManagerMessage> ListenableFuture<T> sendAsync(CellEndpoint endpoint, T msg, long timeout)
    {
        return submit(endpoint, new CellPath(destination), msg, timeout);
    }

    @Override
    public void send(CellEndpoint endpoint, CellMessage envelope, PoolManagerMessage msg)
    {
        envelope.getDestinationPath().insert(destination);
        envelope.setMessageObject(msg);
        endpoint.sendMessage(envelope);
    }

    @Override
    public <T extends PoolIoFileMessage> ListenableFuture<T> startAsync(
            CellEndpoint endpoint, CellAddressCore pool, T msg, long timeout)
    {
        return submit(endpoint, new CellPath(destination, pool), msg, timeout);
    }

    @Override
    public void start(CellEndpoint endpoint, CellMessage envelope, PoolIoFileMessage msg)
    {
        envelope.getDestinationPath().insert(destination);
        envelope.setMessageObject(msg);
        endpoint.sendMessage(envelope);
    }

    @Override
    public SerializablePoolManagerHandler.Version getVersion()
    {
        return new Version(destination);
    }

    @SuppressWarnings("unchecked")
    protected <T extends Message> ListenableFuture<T> submit(CellEndpoint endpoint, CellPath path, T msg, long timeout)
    {
        FutureCellMessageAnswerable<T> callback = new FutureCellMessageAnswerable<>((Class<T>) msg.getClass());
        endpoint.sendMessage(new CellMessage(path, msg), callback, MoreExecutors.directExecutor(), timeout);
        return callback;
    }

    protected static class Version implements SerializablePoolManagerHandler.Version
    {
        private static final long serialVersionUID = 2150977133489318602L;

        private final CellAddressCore destination;

        public Version(CellAddressCore destination)
        {
            this.destination = requireNonNull(destination);
        }

        @Override
        public boolean equals(SerializablePoolManagerHandler.Version other)
        {
            return other instanceof Version && ((Version) other).destination.equals(destination);
        }
    }
}
