package org.dcache.ftp.proxy;

import org.dcache.ftp.TransferMode;

import java.net.InetSocketAddress;

import dmg.cells.nucleus.CellInfoProvider;

/**
 * A ProxyAdaper implementation is responsible for relaying an FTP data
 * connection between a pool and a client.  Once created, the adapter is
 * configured through various methods and then {@code #start} is called.  The
 * {@code #getInernalAddress} method is available to discover to which TCP
 * endpoint the pool should connect.
 * <p>
 * A ProxyAdapter is are aware when all desired data has been transferred.  When
 * this happens the return value from {@code #isAlive} becomes false.  It is
 * recommended that the {@code #join} method be used to ensure all data has been
 * sent and there is an orderly shutdown of TCP connections.
 * <p>
 * The {@code #close} method is always called exactly once, after {@code #start}
 * has returned.  This has two effects: it forcing the adapter to stop any active
 * transfer and allowing the proxy adapter to clean up any remaining state.
 */
public interface ProxyAdapter extends CellInfoProvider
{
    /**
     * The direction in which data will flow for this transfer.
     */
    public enum Direction {
        UPLOAD, DOWNLOAD
    }


    /**
     * Sets the largest block size to be used in mode E. Blocks larger
     * than this are divided into smaller blocks.
     */
    void setMaxBlockSize(int size);

    /**
     * Configure the adapter for the desired data protocol.
     * @param mode The data protocol chosen by the client.
     */
    default void setMode(TransferMode mode)
    {
        // by default, the adapter is mode agnostic.
    }

    /**
     * Returns the endpoint to which the pool should connect.
     */
    InetSocketAddress getInternalAddress();

    /**
     * Specifies in which direction data will flow.  This method is called
     * precisely once, before {@code #start}.
     */
    default void setDataDirection(Direction dir)
    {
        // By default, the ProxyAdapter is data direction agnostic.
    }

    /**
     * Start the adapter.
     */
    void start();

    /**
     * Tests if a transfer is still underway.
     */
    boolean isAlive();

    /**
     *  Waits for a transfer to complete.
     */
    void join(long millis) throws InterruptedException;

    /**
     * Cancel any ongoing transfer and clean up any state.
     */
    void close();

    /**
     * Returns any error that occurred transfer.
     * @return a description of the error or null if no error has occurred.
     */
    String getError();

    /**
     * Returns true if an error has occurred.
     */
    boolean hasError();
}
