/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2022 Deutsches Elektronen-Synchrotron
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
package org.dcache.pool.repository;

import diskCacheV111.util.CacheException;
import org.dcache.util.Checksum;

/**
 * A ReplicaDescriptor where the underlying replica may be modified.
 */
public interface ModifiableReplicaDescriptor extends ReplicaDescriptor {

    /**
     * Commit changes on file.
     * <p>
     * The file must not be modified after the descriptor has been committed.
     * <p>
     * Committing adjusts space reservation to match the actual file size. It may cause the file
     * size in the storage info and in PNFS to be updated. Committing sets the repository entry to
     * its target state.
     * <p>
     * In case of problems, the descriptor is not closed and an exception is thrown.
     * <p>
     * Committing a descriptor multiple times causes an IllegalStateException.
     *
     * @throws IllegalStateException     if the descriptor is already committed or closed.
     * @throws FileSizeMismatchException if file size does not match the expected size.
     * @throws CacheException            if the repository or PNFS state could not be updated.
     */
    void commit()
          throws IllegalStateException, InterruptedException, FileSizeMismatchException, CacheException;

    /**
     * Add checksums of the file.
     * <p>
     * The checksums are not in any way verified. Only valid checksums should be added. The
     * checksums will be stored in the name space on commit or close.
     *
     * @param checksum Checksum of the file
     */
    void addChecksums(Iterable<Checksum> checksum);

    /**
     * Sets the last access time of the replica.
     * <p>
     * Only applicable to writes.
     *
     * @param time
     */
    void setLastAccessTime(long time);
}
