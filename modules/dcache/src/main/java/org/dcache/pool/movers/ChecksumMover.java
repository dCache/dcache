package org.dcache.pool.movers;

import javax.annotation.Nonnull;

import java.security.NoSuchAlgorithmException;
import java.util.Set;

import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;

/**
 * A ChecksumMover is a checksum aware Mover.
 */
public interface ChecksumMover
{
    /**
     * Return an actual checksum computed on the fly during the transfer. Called
     * after runIO.
     *
     * @return a checksum value for the data or null if none is available.
     */
    @Nonnull
    Set<Checksum> getActualChecksums();

    /**
     * Obtain an expected checksum provided by the remote party. Called after
     * runIO.
     *
     * @return an expected checksum value for the data or null if none is available.
     */
    Checksum getExpectedChecksum();
}
