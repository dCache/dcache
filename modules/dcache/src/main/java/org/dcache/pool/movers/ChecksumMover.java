package org.dcache.pool.movers;

import javax.annotation.Nonnull;

import java.util.EnumSet;
import java.util.Set;
import java.util.function.Consumer;

import diskCacheV111.vehicles.ProtocolInfo;

import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;

/**
 * A ChecksumMover is a mover that provides checksum information.  This
 * information is either desired checksum values or known checksums of incoming
 * files.
 */
public interface ChecksumMover
{
    /**
     * Supply a list of desired checksum types to be calculated while receiving
     * data.  This method is called once before runIO if the pool is accepting
     * data from an external source.  The method is not called when the pool
     * is delivering data.
     *
     * @param info The protocol info for this mover.
     * @return checksum types that are desired but currently unknown.
     */
    @Nonnull
    Set<ChecksumType> desiredChecksums(@Nonnull ProtocolInfo info);

    /**
     * Register a checksum consumer that will accept client-supplied checksum
     * values with which the pool will test data data integrity.  Typically,
     * these checksums are provided by the remote party (e.g., the client) as
     * part of the upload process.
     * <p>
     * Note that any checksum known to the door should be provided by including
     * them in the FileAttributes delivered to the pool.  Such checksums are
     * included automatically.
     * <p>
     * This method is called once before runIO if the pool is accepting data
     * from an external source.  The method is not called when the pool is
     * delivering data.
     * <p>
     * A consumer is provided to decouple the method invocation from the client
     * activity in delivering the checksum; e.g., the client could supply the
     * checksum value after delivering the data.
     * @param integrityChecker something that uses checksums to verify data integrity
     */
    void acceptIntegrityChecker(@Nonnull Consumer<Checksum> integrityChecker);
}
