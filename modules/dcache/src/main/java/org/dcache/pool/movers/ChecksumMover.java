package org.dcache.pool.movers;

import diskCacheV111.util.ChecksumFactory;
import org.dcache.util.Checksum;
import diskCacheV111.vehicles.ProtocolInfo;


/**
 * A ChecksumMover is a Mover that supports on-transfer checksum and the
 * possibility of a client-supplied checksum.
 */
public interface ChecksumMover
{
    /**
     * Obtain the mover's preferred algorithm for calculated the on-transfer
     * checksum calculation.  If on-transfer is enabled then this method is
     * called once, before runIO.  If on-transfer is disabled then this method
     * is not called.
     *
     * @return Preferred algorithm or null to use pool's default algorithm
     */
    public ChecksumFactory getOnTransferChecksumFactory(ProtocolInfo protocolInfo);


    /**
     * Obtain the mover's preferred algorithm for calculating the on-write
     * checksum.  If on-write checksum calculation is enabled and on-transfer
     * checksum calculation is disabled then this method is called once, after
     * runIO.  If on-write is disabled or on-transfer is enabled then this
     * method is not called.
     *
     * @return Preferred algorithm or null to use pool's default algorithm
     */
    public ChecksumFactory getOnWriteChecksumFactory(ProtocolInfo protocolInfo);


    /**
     * This method is called to provide the mover with the ChecksumFactory with
     * which it should calculate the on-transfer checksum.  It is called once,
     * before runIO, if on-transfer checksum calculation is enabled.
     * If on-transfer is disabled then this method is not called.
     * @see #getTransferChecksum()
     */
    public void setOnTransferChecksumFactory(ChecksumFactory checksum);


    /**
     * This method is called before runIO to inform the mover whether on-write
     * checksum calculation is enabled.
     */
    public void setOnWriteEnabled(boolean enabled);


    /**
     * This method is called after runIO to discover if the client provided
     * a checksum for the data.
     * @return the client-supplied checksum or null
     */
    public Checksum getClientChecksum();


    /**
     * Obtain the client checksum calculated from the supplied ChecksumFactory.
     * @see #setOnTransferChecksumFactory(diskCacheV111.util.ChecksumFactory)
     * @return the checksum or null if no checksum was calculated.
     */
    public Checksum getTransferChecksum();
}
