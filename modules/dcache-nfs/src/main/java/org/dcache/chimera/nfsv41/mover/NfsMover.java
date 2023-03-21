/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2013 - 2023 Deutsches Elektronen-Synchrotron
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
package org.dcache.chimera.nfsv41.mover;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.DiskErrorCacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.vehicles.PoolIoFileMessage;
import dmg.cells.nucleus.CellPath;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.channels.CompletionHandler;
import org.dcache.nfs.ChimeraNFSException;
import org.dcache.nfs.status.NfsIoException;
import org.dcache.nfs.v4.NFS4State;
import org.dcache.nfs.v4.NFSv41Session;
import org.dcache.nfs.v4.StateOwner;
import org.dcache.nfs.v4.xdr.stateid4;
import org.dcache.pool.classic.Cancellable;
import org.dcache.pool.movers.MoverChannelMover;
import org.dcache.pool.repository.ReplicaDescriptor;
import org.dcache.vehicles.FileAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NfsMover extends MoverChannelMover<NFS4ProtocolInfo, NfsMover> {

    private static final Logger _log = LoggerFactory.getLogger(NfsMover.class);
    private NFSv41Session _session;
    private final NfsTransferService _nfsTransferService;
    private final NFS4State _state;
    private final PnfsHandler _namespace;
    private volatile CompletionHandler<Void, Void> _completionHandler;

    public NfsMover(ReplicaDescriptor handle, PoolIoFileMessage message, CellPath pathToDoor,
          NfsTransferService nfsTransferService, PnfsHandler pnfsHandler) {
        super(handle, message, pathToDoor, nfsTransferService);
        _nfsTransferService = nfsTransferService;
        org.dcache.chimera.nfs.v4.xdr.stateid4 legacyStateid = getProtocolInfo().stateId();
        _state = new MoverState(null, new stateid4(legacyStateid.other, legacyStateid.seqid.value));
        _namespace = pnfsHandler;
    }

    public stateid4 getStateId() {
        return _state.stateid();
    }

    public byte[] getNfsFilehandle() {
        return getProtocolInfo().getNfsFileHandle();
    }

    @Override
    protected String getStatus() {
        StringBuilder s = new StringBuilder();
        s.append("NFSv4.1/pNFS,OS=")
              .append(getStateId())
              .append(",cl=[")
              .append(getProtocolInfo().getSocketAddress().getAddress().getHostAddress())
              .append("]");
        return s.toString();
    }

    /**
     * Enable access with this mover.
     *
     * @param completionHandler to be called when mover finishes.
     * @return handle to cancel mover if needed
     * @throws InterruptedIOException  if mover was cancelled
     * @throws DiskErrorCacheException
     */
    public Cancellable enable(final CompletionHandler<Void, Void> completionHandler)
          throws DiskErrorCacheException, InterruptedIOException {

        open();
        _completionHandler = completionHandler;
        _nfsTransferService.add(this);
        return (e) -> disable(null);

    }

    /**
     * Disable access with this mover. If {@code error} is not a {@code null}, the {@link
     * CompletionHandler#failed(Throwable, Object)} method will be called.
     *
     * @param error error to report, or {@code null} on success
     */
    void disable(Throwable error) {

        boolean isActive = _nfsTransferService.remove(NfsMover.this);
        if (!isActive) {
            _log.info("Skip disabling disposed mover: {} {} - {}", getFileAttributes().getPnfsId(), getIoMode(), getStatus());
            return;
        }

        detachSession();
        try {
            getMoverChannel().close();
        } catch (IOException e) {
            _log.error("failed to close RAF {}", e.toString());
        } finally {
            if (error == null) {
                _completionHandler.completed(null, null);
            } else {
                _completionHandler.failed(error, null);
            }
        }
    }

    /**
     * Attach mover tho the client's NFSv41 session.
     *
     * @param session to attach to
     */
    synchronized void attachSession(NFSv41Session session) {
        if (_session == null) {
            _session = session;
            _session.getClient().attachState(_state);
        }
    }

    /**
     * Detach mover from the client's session.
     */
    synchronized void detachSession() {
        if (_session != null) {
            _session.getClient().detachState(_state);
            _session = null;
        }
    }

    /**
     * A special {@link NFS4State} to kill the mover when disposed.
     */
    private class MoverState extends NFS4State {

        MoverState(StateOwner owner, stateid4 stateid) {
            super(owner, stateid);
        }

        @Override
        protected void dispose() {
            detachSession();
        }
    }

    public void commitFileSize(long size) throws ChimeraNFSException {
        try {
            _namespace.setFileAttributes(getFileAttributes().getPnfsId(),
                  FileAttributes.ofSize(size));
        } catch (CacheException e) {
            throw new NfsIoException("Failed to update file size in the namespace", e);
        }
    }

    public synchronized boolean hasSession() {
        return (_session != null);
    }
}
