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

package org.dcache.pool.classic;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Set;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileCorruptedCacheException;

import org.dcache.pool.repository.ReplicaDescriptor;
import org.dcache.pool.repository.ReplicaRecord;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;

/**
 * Encapsulates checksum policies.
 */
public interface ChecksumModule
{
    /**
     * Verify the integrity of a file identified as broken.  If knownChecksums
     * is empty then the replica's data is assumed to be correct.
     * <p>
     * Checksums may be calculated in addition to those required to verify
     * the knownChecksums.  Any such additional checksums are returned and are
     * intended to be stored locally and sent to the namespace.
     * @param entry The broken replica
     * @param knownChecksums Any known checksums for valid data.
     * @throws IOException if checksums could not be calculated
     * @throws FileCorruptedCacheException if the stored data is corrupt
     * @throws InterruptedException if the pool is shut down during the verification process.
     * @return Any additional checksums calculated
     */
    @Nonnull
    public Set<Checksum> verifyBrokenFile(ReplicaRecord entry,
            @Nonnull Set<Checksum> knownChecksums)
            throws IOException, FileCorruptedCacheException, InterruptedException;


    /**
     * Applies the post-transfer checksum policy.
     *
     * Should be called after a write to the pool.
     *
     * @param handle A write descriptor
     * @param actualChecksums Checksums computed during the transfer
     * @throws CacheException If the checksums could not be verified
     * @throws FileCorruptedCacheException If checksums do not match
     * @throws NoSuchAlgorithmException If no suitable checksum algorithm is supported
     * @throws IOException If an I/O error happened while computing the checksum
     * @throws InterruptedException If the thread is interrupted
     */
    void enforcePostTransferPolicy(
            ReplicaDescriptor handle, @Nonnull Iterable<Checksum> actualChecksums)
            throws CacheException, NoSuchAlgorithmException, IOException, InterruptedException;

    /**
     * Applies the pre-flush checksum policy.
     *
     * Should be called before an HSM flush.
     *
     * @param handle A replica descriptor
     * @throws CacheException If the checksums could not be verified
     * @throws FileCorruptedCacheException If checksums do not match
     * @throws NoSuchAlgorithmException If no suitable checksum algorithm is supported
     * @throws IOException If an I/O error happened while computing the checksum
     * @throws InterruptedException If the thread is interrupted
     */
    void enforcePreFlushPolicy(ReplicaDescriptor handle)
            throws CacheException, InterruptedException, NoSuchAlgorithmException, IOException;

    /**
     * Applies the post-restore policy.
     *
     * Should be called after restoring a file from HSM.
     *
     * @param handle A write descriptor
     * @param checksums Any additional expected checksums
     * @throws CacheException If the checksums could not be verified
     * @throws FileCorruptedCacheException If checksums do not match
     * @throws NoSuchAlgorithmException If no suitable checksum algorithm is supported
     * @throws IOException If an I/O error happened while computing the checksum
     * @throws InterruptedException If the thread is interrupted
     */
    void enforcePostRestorePolicy(ReplicaDescriptor handle, @Nonnull Set<Checksum> checksums)
            throws CacheException, NoSuchAlgorithmException, IOException, InterruptedException;

    /**
     * Verifies the checksum of a file.
     *
     * @param handle A replica descriptor
     * @throws CacheException If the checksums could not be verified
     * @throws FileCorruptedCacheException If checksums do not match
     * @throws NoSuchAlgorithmException If no suitable checksum algorithm is supported
     * @throws IOException If an I/O error happened while computing the checksum
     * @throws InterruptedException If the thread is interrupted
     * @return Any checksum computed for the file
     */
    @Nonnull
    Iterable<Checksum> verifyChecksum(ReplicaDescriptor handle)
            throws NoSuchAlgorithmException, IOException, InterruptedException, CacheException;

    /**
     * Verifies the checksum of a file.
     *
     * @param channel A repository channel
     * @param checksums Expected checksums for file
     * @throws CacheException If the checksums could not be verified
     * @throws FileCorruptedCacheException If checksums do not match
     * @throws NoSuchAlgorithmException If no suitable checksum algorithm is supported
     * @throws IOException If an I/O error happened while computing the checksum
     * @throws InterruptedException If the thread is interrupted
     * @return Any checksum computed for the file
     */
    @Nonnull
    Iterable<Checksum> verifyChecksum(RepositoryChannel channel, @Nonnull Iterable<Checksum> checksums)
            throws NoSuchAlgorithmException, IOException, InterruptedException, CacheException;
}
