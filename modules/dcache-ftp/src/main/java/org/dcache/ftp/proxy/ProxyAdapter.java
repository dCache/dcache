package org.dcache.ftp.proxy;

import java.net.InetSocketAddress;

public interface ProxyAdapter {

    /**
     * Returns any error that occurred transfer, or null if no error
     * occurred. The transfer failed if an error is returned.
     */
    String getError();

    /**
     * Returns true if and only if getError() return non-null.
     */
    boolean hasError();

    /**
     * Sets the largest block size to be used in mode E. Blocks larger
     * than this are divided into smaller blocks.
     */
    void setMaxBlockSize(int size);

    /**
     * Sets the adapter to use either mode E or mode S.
     */
    void setModeE(boolean modeE);

    /**
     * Returns the address that we use to listen for connections from the
     * pool.
     *
     * This is needed in order to tell the pool were to connect to.
     */
    InetSocketAddress getInternalAddress();

    /**
     * Configures the adapter to transfer data from the client to the
     * pool.
     */
    void setDirClientToPool();

    /**
     * Configures the adapter to transfer data from the pool to the
     * client.
     *
     * This direction is not supported in mode E.
     */
    void setDirPoolToClient();

    /**
     * Interrupt the thread driving the adapter and close the server
     * sockets.
     */
    void close();

    /*
     * The methods below are actually part of Thread API
     * They are here to satisfy the pre-existing code in AbstractFtpDoorV1
     */

    /**
     * Start the thread driving the adapter
     */
    void start();

    /**
     * Tests if the thread driving the adapter is alive
     */
    boolean isAlive();

    /**
     *  Waits for the  thread driving the adapter to die.
     */
    void join() throws InterruptedException;

    /**
     *  Waits a certain time for the thread driving the adapter to die.
     */
    void join(long millis) throws InterruptedException;
}
