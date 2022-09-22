/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2013 - 2017 Deutsches Elektronen-Synchrotron
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
package org.dcache.pool.movers;

import diskCacheV111.vehicles.ProtocolInfo;
import dmg.cells.nucleus.CellPath;
import java.net.InetSocketAddress;
import java.nio.channels.CompletionHandler;
import java.nio.file.OpenOption;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.security.auth.Subject;
import org.dcache.pool.classic.Cancellable;
import org.dcache.pool.repository.ReplicaDescriptor;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;
import org.dcache.vehicles.FileAttributes;

/**
 * A Mover is the part of a file transfer that runs on a Pool.
 * <p/>
 * An implementation of this interface is the sole representation of the transfer on the pool.
 * <p/>
 * The interface is not to be confused with the legacy MoverProtocol interface.
 */
public interface Mover<T extends ProtocolInfo> {

    /**
     * Provides attributes of the file being transferred.
     */
    FileAttributes getFileAttributes();

    /**
     * Provides protocol specific information about the transfer.
     */
    T getProtocolInfo();

    /**
     * Returns transfer duration time in milliseconds.
     *
     * @return transfer time in milliseconds.
     */
    long getTransferTime();

    /**
     * Number of bytes transferred by this mover.
     */
    long getBytesTransferred();

    /**
     * Time stamp of the last transferred byte in milliseconds since the epoch.
     */
    long getLastTransferred();

    /**
     * Provides an identity used by the door to identify the transfer.
     */
    long getClientId();

    /**
     * Set transfer status.
     * <p>
     * The provided status and error message will be sent to billing and to the door. Only the first
     * error status set is kept. Any subsequent errors are suppressed.
     */
    void setTransferStatus(int errorCode, String errorMessage);

    /**
     * Provides the queue on which the mover is scheduled.
     */
    String getQueueName();

    /**
     * Provides a code for the last error, or zero if there was no error.
     */
    int getErrorCode();

    /**
     * Provides a message for the last error, or the empty string if there was no error.
     */
    String getErrorMessage();

    /**
     * Identification of who created the transfer (used by billing).
     */
    String getInitiator();

    /**
     * Returns true if this is a transfer between two pools.
     */
    boolean isPoolToPoolTransfer();

    /**
     * Provides the identity of the entity that submitted the transfer.
     */
    Subject getSubject();

    /**
     * Provides a descriptor for the open repository entry of the file being transferred.
     */
    ReplicaDescriptor getIoHandle();

    /**
     * Provide the channel used for the transfer.  If no channel has been opened then the returned
     * value is empty.
     */
    Optional<RepositoryChannel> getChannel();


    /**
     * Get set of options specifying how the file is opened. The READ and WRITE options determine if
     * the file should be opened for reading and/or writing.
     */
    Set<? extends OpenOption> getIoMode();

    /**
     * Provides a path to the door that requested the mover.
     */
    CellPath getPathToDoor();

    /**
     * Returns any checksums computed during the transfer.
     */
    @Nonnull
    Set<Checksum> getActualChecksums();

    /**
     * Add desired additional checksum values.
     */
    void addChecksumType(ChecksumType checksum);

    /**
     * Returns any known-good checksums obtained from the client.
     */
    @Nonnull
    Set<Checksum> getExpectedChecksums();

    /**
     * Returns the billable name space path of the file being transferred.
     */
    String getBillingPath();

    /**
     * Returns the temporary name space path of the file being transferred.
     */
    String getTransferPath();

    /**
     * Initiates the actual transfer phase. The operation is asynchronous. Completion is signaled
     * through the <code>completionHandler</code>.
     */
    Cancellable execute(CompletionHandler<Void, Void> completionHandler);

    /**
     * Releases any resources held by the mover. Since closing a mover triggers a fair amount of
     * post processing, this operation is asynchronous. Completion is signaled through the
     * <code>completionHandler</code>.
     * <p>
     * A mover can and must be closed even if <code>execute</code> was not called or failed.
     */
    void close(CompletionHandler<Void, Void> completionHandler);

    /**
     * Provide a list of the IP address and port number of all currently active TCP connections.  An
     * empty list indicates that there is current no established connections.  The mover may order
     * the connections in some protocol-specific fashion.  A mover that is unable to provide
     * connection information should return null.
     */
    @Nullable
    default List<InetSocketAddress> remoteConnections() {
        return null;
    }

    /**
     * Returns the {@link InetSocketAddress} of the local endpoint used by clients to access the mover.
     */
    InetSocketAddress getLocalEndpoint();

    /**
     * Provide the expected total number of bytes transferred for this transfer, if known.  Returns
     * null if this value is unknown.
     */
    @Nullable
    default Long getBytesExpected() {
        return null;
    }
}
