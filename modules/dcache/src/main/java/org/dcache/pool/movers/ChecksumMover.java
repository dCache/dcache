package org.dcache.pool.movers;

import java.security.NoSuchAlgorithmException;

import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;

/**
 * A ChecksumMover is a checksum aware Mover.
 */
public interface ChecksumMover
{
    /**
     * Instruct the mover to enable on-the-fly checksum calculation.  This
     * method is called before runIO.  The pool provides a default or suggested
     * algorithm that the mover is free to ignore in favour of some preferred
     * algorithm.
     * @param suggestedAlgorithm a default algorithm
     */
    void enableTransferChecksum(ChecksumType suggestedAlgorithm)
            throws NoSuchAlgorithmException;

    /**
     * Return an actual checksum computed on the fly during the transfer. Called
     * after runIO.
     *
     * @return a checksum value for the data or null if none is available.
     */
    Checksum getActualChecksum();

    /**
     * Obtain an expected checksum provided by the remote party. Called after
     * runIO.
     *
     * @return an expected checksum value for the data or null if none is available.
     */
    Checksum getExpectedChecksum();
}
