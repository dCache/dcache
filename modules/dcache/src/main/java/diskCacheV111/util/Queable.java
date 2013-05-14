package diskCacheV111.util;

public interface Queable extends Runnable {
    /**
     * invoked by job scheduler at the time when job added into a queue
     * @param id assigned by the job scheduler
     */
    public void queued(int id);

    /**
     * invoked by job scheduler at the time when job is removed form a queue
     */
    public void unqueued();

    /**
     * Initiate termination of the current invocation of the
     * <code>run</code> method.
     *
     * @return true if successful, false otherwise
     */
    public void kill();
}
