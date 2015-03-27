/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2013 Deutsches Elektronen-Synchrotron
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;

import java.io.IOException;
import java.nio.channels.CompletionHandler;
import java.util.Collections;
import java.util.Set;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.ChecksumFactory;
import diskCacheV111.util.DiskErrorCacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.vehicles.PoolIoFileMessage;

import dmg.cells.nucleus.CellPath;

import org.dcache.nfs.v4.NFS4State;
import org.dcache.nfs.v4.NFSv41Session;
import org.dcache.nfs.v4.xdr.stateid4;
import org.dcache.nfs.v4.xdr.verifier4;
import org.dcache.pool.classic.Cancellable;
import org.dcache.pool.movers.ChecksumChannel;
import org.dcache.pool.movers.IoMode;

import org.dcache.pool.movers.MoverChannel;
import org.dcache.pool.movers.MoverChannelMover;
import org.dcache.pool.repository.ReplicaDescriptor;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.util.Checksum;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Preconditions.checkState;

public class NfsMover extends MoverChannelMover<NFS4ProtocolInfo, NfsMover> {

    private static final Logger _log = LoggerFactory.getLogger(NfsTransferService.class);
    private NFSv41Session _session;
    private final NFSv4MoverHandler _nfsIO;
    private final NFS4State _state;
    private final PnfsHandler _namespace;
    private volatile CompletionHandler<Void, Void> _completionHandler;
    private final verifier4 _bootVerifier;

    private final ChecksumFactory _checksumFactory;
    private ChecksumChannel _checksumChannel;

    public NfsMover(ReplicaDescriptor handle, PoolIoFileMessage message, CellPath pathToDoor,
            NfsTransferService nfsTransferService, PnfsHandler pnfsHandler,
            ChecksumFactory checksumFactory) {
        super(handle, message, pathToDoor, nfsTransferService, MoverChannel.AllocatorMode.SOFT);
        _nfsIO = nfsTransferService.getNfsMoverHandler();
        _state = new MoverState();
        _namespace = pnfsHandler;
        _bootVerifier = nfsTransferService.getBootVerifier();
        _checksumFactory = checksumFactory;
    }

    @Override
    public Set<Checksum> getActualChecksums() {
        return (_checksumChannel == null)
                ? Collections.<Checksum>emptySet()
                : Optional.fromNullable(_checksumChannel.getChecksum()).asSet();
    }

    @Override
    public Set<Checksum> getExpectedChecksums() {
        return Collections.emptySet();
    }

    @Override
    public synchronized RepositoryChannel openChannel() throws DiskErrorCacheException {
        checkState(_checksumChannel == null);
        RepositoryChannel channel = super.openChannel();
        try {
            if (getIoMode() == IoMode.WRITE && _checksumFactory != null) {
                channel = _checksumChannel = new ChecksumChannel(channel, _checksumFactory);
            }
        } catch (Throwable t) {
            /* This should only happen in case of JVM Errors or if the checksum digest cannot be
             * instantiated (which, barring bugs, should never happen).
             */
            try {
                channel.close();
            } catch (IOException e) {
                t.addSuppressed(e);
            }
            Throwables.propagate(t);
        }
        return channel;
    }

    public stateid4 getStateId() {
        org.dcache.chimera.nfs.v4.xdr.stateid4 legacyStateid =  getProtocolInfo().stateId();
        return new stateid4(legacyStateid.other, legacyStateid.seqid.value);
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
     * @param completionHandler to be called when mover finishes.
     * @return handle to cancel mover if needed
     * @throws DiskErrorCacheException
     */
    public Cancellable enable(final CompletionHandler<Void,Void> completionHandler) throws DiskErrorCacheException {

        open();
        _completionHandler = completionHandler;
        _nfsIO.add(this);
        return new Cancellable() {
            @Override
            public void cancel() {
                disable(null);
            }
        };

    }

    /**
     * Disable access with this mover. If {@code error} is not a {@code null},
     * the {@link CompletionHandler#failed(Throwable, A)} method will be called.
     * @param error error to report, or {@code null} on success
     */
    void disable(Throwable error) {
        _nfsIO.remove(NfsMover.this);
        detachSession();
        try {
            getMoverChannel().close();
        } catch (IOException e) {
            _log.error("failed to close RAF {}", e.toString());
        }
        if(error == null) {
            _completionHandler.completed(null, null);
        } else {
            _completionHandler.failed(error, null);
        }
    }

    /**
     * Attach mover tho the client's NFSv41 session.
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

        MoverState() {
            super(NfsMover.this.getStateId());
        }

        @Override
        protected void dispose() {
            detachSession();
        }
    }

    public void commitFileSize(long size) throws CacheException {
        FileAttributes attributes = new FileAttributes();
        attributes.setSize(size);
        _namespace.setFileAttributes(getFileAttributes().getPnfsId(), attributes);
    }

    public verifier4 getBootVerifier() {
        return _bootVerifier;
    }

    public synchronized boolean hasSession() {
        return (_session != null);
    }
}
