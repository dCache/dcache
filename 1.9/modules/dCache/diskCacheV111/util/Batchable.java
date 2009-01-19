// $Id: Batchable.java,v 1.4 2004-11-08 23:01:27 timur Exp $
package diskCacheV111.util ;

public interface Batchable extends Runnable {

    /**
     * invoked by job scheduler at the time when job added into a queue
     * @param id assigned by the job scheduler
     */
    public void queued(int id) ;

    /**
     * invoked by job scheduler at the time when job is removed form a queue
     */
    public void unqueued() ;

//    public double getTransferRate() ;
    public String getClient();
    public long   getClientId() ;

    /**
     * Initiate termination of the current invocation of the
     * <code>run</code> method.
     *
     * @return true if successful, false otherwise
     */
    public boolean kill();
}
