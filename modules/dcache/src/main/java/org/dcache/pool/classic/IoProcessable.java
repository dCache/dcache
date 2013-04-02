package org.dcache.pool.classic;

/**
 *
 * @Since 1.9.11
 */
public interface IoProcessable {

    long getTransferTime();

    long getBytesTransferred();

    public double getTransferRate();

    public long getLastTransferred();


//    public double getTransferRate() ;
    public String getClient();
    public long   getClientId() ;

    /**
     * Initiate termination of the current invocation of the
     * <code>run</code> method.
     *
     * @return true if successful, false otherwise
     */
    public void kill();
}
