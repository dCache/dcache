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

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.ChecksumFactory;
import diskCacheV111.util.FileCorruptedCacheException;

import org.dcache.pool.repository.ReplicaDescriptor;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.util.Checksum;

/**
 * Encapsulates checksum policies.
 */
public interface ChecksumModule
{
    /**
     * The policy implemented by a ChecksumModule is determined by these policy flags.
     */
    enum PolicyFlag {
        /** Validate checksum on file read. Not implemented. */
        ON_READ,

        /** Validate checksum before flush to HSM. */
        ON_FLUSH,

        /** Validate checsum after restore from HSM. */
        ON_RESTORE,

        /** Validate checksum after file was written to pool. */
        ON_WRITE,

        /** Validate checksum while file is being written to pool. */
        ON_TRANSFER,

        /** Enforce availability of a checksum on upload. */
        ENFORCE_CRC,

        /** Retrieve checksums from HSM after restore and register in name space. */
        GET_CRC_FROM_HSM,

        /** Background checksum verification. */
        SCRUB
    }

    /**
     * Determines if the policy is enabled.
     *
     * @param policy A checksum policy
     * @return true if the policy is enabled, false otherwise
     */
    boolean hasPolicy(PolicyFlag policy);

    /**
     * Returns a supported checksum factory for one of the known checksums
     * of a file, or a default factory if none are supported or known.
     *
     * @param handle A replica descriptor
     * @return A checksum factory
     * @throws NoSuchAlgorithmException If no suitable checksum algorithm is supported
     * @throws CacheException If the checksums of the file could not be retrieved
     */
    ChecksumFactory getPreferredChecksumFactory(ReplicaDescriptor handle)
            throws NoSuchAlgorithmException, CacheException;

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
            ReplicaDescriptor handle, Iterable<Checksum> actualChecksums)
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
     * @throws CacheException If the checksums could not be verified
     * @throws FileCorruptedCacheException If checksums do not match
     * @throws NoSuchAlgorithmException If no suitable checksum algorithm is supported
     * @throws IOException If an I/O error happened while computing the checksum
     * @throws InterruptedException If the thread is interrupted
     */
    void enforcePostRestorePolicy(ReplicaDescriptor handle)
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
    Iterable<Checksum> verifyChecksum(RepositoryChannel channel, Iterable<Checksum> checksums)
            throws NoSuchAlgorithmException, IOException, InterruptedException, CacheException;
}
