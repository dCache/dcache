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

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.PoolIoFileMessage;
import diskCacheV111.vehicles.PoolManagerMessage;
import diskCacheV111.vehicles.RestoreHandlerInfo;
import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import java.io.Serializable;
import org.dcache.cells.FutureCellMessageAnswerable;

/**
 * Implementation of PoolManagerHandler that delegates all requests to a pool manager.
 */
public class RemotePoolManagerHandler implements SerializablePoolManagerHandler {

    private static final long serialVersionUID = -7662812702828746542L;

    protected final CellAddressCore destination;

    public RemotePoolManagerHandler(CellAddressCore destination) {
        this.destination = requireNonNull(destination);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends PoolManagerMessage> ListenableFuture<T> sendAsync(CellEndpoint endpoint,
          T msg, long timeout) {
        if (msg instanceof PoolManagerGetRestoreHandlerInfo) {
            return (ListenableFuture<T>) Futures.transform(
                  submit(endpoint, new CellPath(destination), "xrc ls", RestoreHandlerInfo[].class,
                        timeout),
                  a -> new PoolManagerGetRestoreHandlerInfo(asList(a)));
        }
        return submit(endpoint, new CellPath(destination), msg, timeout);
    }

    @Override
    public void send(CellEndpoint endpoint, CellMessage envelope, PoolManagerMessage msg) {
        checkArgument(envelope.getSourcePath().hops() > 0, "Envelope is missing source address.");
        envelope.getDestinationPath().insert(destination);
        envelope.setMessageObject(msg);
        endpoint.sendMessage(envelope, CellEndpoint.SendFlag.PASS_THROUGH);
    }

    @Override
    public <T extends PoolIoFileMessage> ListenableFuture<T> startAsync(
          CellEndpoint endpoint, CellAddressCore pool, T msg, long timeout) {
        return submit(endpoint, new CellPath(pool), msg, timeout);
    }

    @Override
    public void start(CellEndpoint endpoint, CellMessage envelope, PoolIoFileMessage msg) {
        checkArgument(envelope.getSourcePath().hops() > 0, "Envelope is missing source address.");
        envelope.setMessageObject(msg);
        endpoint.sendMessage(envelope, CellEndpoint.SendFlag.PASS_THROUGH);
    }

    @Override
    public SerializablePoolManagerHandler.Version getVersion() {
        return new Version(destination);
    }

    @SuppressWarnings("unchecked")
    protected <T extends Message> ListenableFuture<T> submit(CellEndpoint endpoint, CellPath path,
          T msg, long timeout) {
        return submit(endpoint, path, msg, (Class<T>) msg.getClass(), timeout);
    }

    protected <T extends Serializable> ListenableFuture<T> submit(CellEndpoint endpoint,
          CellPath path, Serializable msg, Class<T> reply, long timeout) {
        FutureCellMessageAnswerable<T> callback = new FutureCellMessageAnswerable<>(reply);
        endpoint.sendMessage(new CellMessage(path, msg), callback, MoreExecutors.directExecutor(),
              timeout);
        return callback;
    }

    @Override
    public String toString() {
        return destination.toString();
    }

    protected static class Version implements SerializablePoolManagerHandler.Version {

        private static final long serialVersionUID = 2150977133489318602L;

        private final CellAddressCore destination;

        public Version(CellAddressCore destination) {
            this.destination = requireNonNull(destination);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Version version = (Version) o;
            return destination.equals(version.destination);
        }

        @Override
        public int hashCode() {
            return destination.hashCode();
        }
    }
}
