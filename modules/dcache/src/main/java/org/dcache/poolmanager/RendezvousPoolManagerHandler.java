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

import com.google.common.collect.Ordering;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import java.util.Iterator;
import java.util.List;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.PoolIoFileMessage;
import diskCacheV111.vehicles.PoolManagerMessage;
import diskCacheV111.vehicles.PoolMgrGetPoolMsg;

import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;

import org.dcache.cells.FutureCellMessageAnswerable;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.Objects.requireNonNull;

/**
 */
public class RendezvousPoolManagerHandler implements SerializablePoolManagerHandler
{
    private static final long serialVersionUID = 8055227719635104693L;

    private final List<CellAddressCore> backends;

    private final CellAddressCore serviceAddress;

    public RendezvousPoolManagerHandler(CellAddressCore serviceAddress, List<CellAddressCore> backends)
    {
        checkArgument(!backends.isEmpty());
        checkArgument(Ordering.natural().isOrdered(backends));
        this.serviceAddress = serviceAddress;
        this.backends = backends;
    }

    public CellAddressCore backendFor(PnfsId pnfsId)
    {
        Iterator<CellAddressCore> iterator = backends.iterator();
        CellAddressCore address = iterator.next();
        int min = hash(pnfsId, address);
        while (iterator.hasNext()) {
            CellAddressCore nextAddress = iterator.next();
            int nextHash = hash(pnfsId, nextAddress);
            if (nextHash < min) {
                min = nextHash;
                address = nextAddress;
            }
        }
        return address;
    }

    public CellAddressCore backendFor(PoolManagerMessage msg)
    {
        if (msg instanceof PoolMgrGetPoolMsg) {
            return backendFor(((PoolMgrGetPoolMsg) msg).getFileAttributes().getPnfsId());
        }
        return serviceAddress;
    }

    public CellAddressCore backendFor(PoolIoFileMessage msg)
    {
        return backendFor(msg.getFileAttributes().getPnfsId());
    }

    private static int hash(PnfsId pnfsId, CellAddressCore address)
    {
        return Hashing.murmur3_32().newHasher()
                .putObject(pnfsId, PnfsId.funnel())
                .putString(address.toString(), US_ASCII)
                .hash().asInt();
    }

    @Override
    public <T extends PoolManagerMessage> ListenableFuture<T> sendAsync(CellEndpoint endpoint, T msg, long timeout)
    {
        return submit(endpoint, new CellPath(backendFor(msg)), msg, timeout);
    }

    @Override
    public void send(CellEndpoint endpoint, CellMessage envelope, PoolManagerMessage msg)
    {
        envelope.getDestinationPath().insert(backendFor(msg));
        envelope.setMessageObject(msg);
        endpoint.sendMessage(envelope);
    }

    @Override
    public <T extends PoolIoFileMessage> ListenableFuture<T> startAsync(
            CellEndpoint endpoint, CellAddressCore pool, T msg, long timeout)
    {
        return submit(endpoint, new CellPath(backendFor(msg), pool), msg, timeout);
    }

    @Override
    public void start(CellEndpoint endpoint, CellMessage envelope, PoolIoFileMessage msg)
    {
        envelope.getDestinationPath().insert(backendFor(msg));
        envelope.setMessageObject(msg);
        endpoint.sendMessage(envelope);
    }

    @Override
    public SerializablePoolManagerHandler.Version getVersion()
    {
        return new Version(serviceAddress, backends);
    }

    @SuppressWarnings("unchecked")
    protected <T extends Message> ListenableFuture<T> submit(CellEndpoint endpoint, CellPath path, T msg, long timeout)
    {
        FutureCellMessageAnswerable<T> callback = new FutureCellMessageAnswerable<>((Class<T>) msg.getClass());
        endpoint.sendMessage(new CellMessage(path, msg), callback, MoreExecutors.directExecutor(), timeout);
        return callback;
    }

    @Override
    public String toString()
    {
        return "rendezvous{service=" + serviceAddress + ", backends=" + backends + "}";
    }

    protected static class Version implements SerializablePoolManagerHandler.Version
    {
        private static final long serialVersionUID = 7093718261159908650L;

        private final CellAddressCore serviceAddress;

        private final List<CellAddressCore> backends;

        public Version(CellAddressCore serviceAddress, List<CellAddressCore> backends)
        {
            this.serviceAddress = requireNonNull(serviceAddress);
            this.backends = requireNonNull(backends);
        }

        @Override
        public boolean equals(SerializablePoolManagerHandler.Version other)
        {
            return other instanceof Version &&
                   ((Version) other).serviceAddress.equals(serviceAddress) &&
                   ((Version) other).backends.equals(backends);
        }
    }
}
