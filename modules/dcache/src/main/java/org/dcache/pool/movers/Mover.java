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
package org.dcache.pool.movers;

import javax.security.auth.Subject;

import java.nio.channels.CompletionHandler;
import java.util.Set;

import diskCacheV111.util.FsPath;
import diskCacheV111.vehicles.ProtocolInfo;

import dmg.cells.nucleus.CellPath;

import org.dcache.pool.classic.Cancellable;
import org.dcache.pool.repository.ReplicaDescriptor;
import org.dcache.util.Checksum;
import org.dcache.vehicles.FileAttributes;

/**
 * A Mover is the part of a file transfer that runs on a Pool.
 * <p/>
 * An implementation of this interface is the sole representation of the transfer
 * on the pool.
 * <p/>
 * The interface is not to be confused with the legacy MoverProtocol interface.
 */
public interface Mover<T extends ProtocolInfo>
{
    /**
     * Provides attributes of the file being transferred.
     */
    FileAttributes getFileAttributes();

    /**
     * Provides protocol specific information about the transfer.
     */
    T getProtocolInfo();

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
     *
     * The provided status and error message will be sent to billing and to
     * the door. Only the first error status set is kept. Any subsequent
     * errors are suppressed.
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
     * Indicates whether this is a WRITE or a READ
     */
    IoMode getIoMode();

    /**
     * Provides a path to the door that requested the mover.
     */
    CellPath getPathToDoor();

    /**
     * Returns any checksums computed during the transfer.
     */
    Set<Checksum> getActualChecksums();

    /**
     * Returns any known-good checksums obtained from the client.
     */
    Set<Checksum> getExpectedChecksums();

    /**
     * Returns the name space path of the file being transferred.
     */
    FsPath getPath();

    /**
     * Initiates the actual transfer phase. The operation is asynchronous.
     */
    Cancellable execute(CompletionHandler<Void, Void> completionHandler);

    /**
     * Initiates any postprocessing. This marks the end of the transfer and the mover's descriptor
     * will be closed. The operation is asynchronous.
     */
    void postprocess(CompletionHandler<Void, Void> completionHandler);
}
