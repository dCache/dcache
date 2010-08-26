package org.dcache.pool.movers;

/**
 *
 * Interface for movers which can't calculate
 * transfered byte them self.
 *
 * @since 1.9.3
 *
 */
public interface ManualMover extends MoverProtocol {

    /**
     * Set number of transfered bytes. The total transfered bytes count will
     * be increased. All negative values  ignored. The last access time is updated.
     *
     * @param bytesTransferred
     */
    public void setBytesTransferred(long bytesTransferred);
}
