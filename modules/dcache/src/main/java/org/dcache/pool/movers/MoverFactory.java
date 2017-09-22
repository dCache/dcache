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

import com.google.common.collect.Sets;
import java.nio.file.OpenOption;
import java.util.Set;

import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.PoolIoFileMessage;

import dmg.cells.nucleus.CellPath;
import java.nio.file.StandardOpenOption;

import org.dcache.pool.repository.ReplicaDescriptor;

/**
 * Mover factories provide means for creating movers for transfer requests.
 */
public interface MoverFactory
{
    /**
     * Creates a new mover for the given file and request.
     *
     * Upon closing the mover, the mover must close the <code>handle</code>
     * and signal the request initiator (door) about the completion. A mover
     * typically delegates this to a PostTransferService, which also enforces
     * the checksum policy and notifies billing.
     *
     * @param handle Handle to the replica to move
     * @param message The request message from the initiator
     * @param pathToDoor Cell path to the initiator
     * @return A mover than will serve the transfer
     * @throws CacheException If the mover could not be created
     */
    Mover<?> createMover(ReplicaDescriptor handle, PoolIoFileMessage message, CellPath pathToDoor) throws CacheException;

    /**
     * Get set of option which have to be used by ReplicaDescriptor when
     * a new RepositoryChannel is created.
     * @return set of open options.
     */
    default Set<? extends OpenOption> getChannelCreateOptions() {
        return Sets.newHashSet(StandardOpenOption.CREATE);
    }
}
