package diskCacheV111.doors;

public abstract class AbstractInterruptibleLineBasedInterpreter implements LineBasedInterpreter
{
    private boolean isStopped;

    /**
     * The thread to interrupt when the command poller is
     * closed. May be null if interrupts are disabled.
     */
    private Thread executingThread;

    /**
     * Enables interrupt upon stop. Until the next call of
     * disableInterrupt(), a call to <code>stop</code> will cause
     * the calling thread to be interrupted.
     *
     * @throws InterruptedException if command poller is already
     * closed
     */
    protected synchronized void enableInterrupt()
            throws InterruptedException
    {
        if (isStopped) {
            throw new InterruptedException();
        }
        executingThread = Thread.currentThread();
    }

    /**
     * Disables interrupt upon stop.
     */
    protected synchronized void disableInterrupt()
    {
        executingThread = null;
    }

    public synchronized void shutdown()
    {
        if (!isStopped) {
            isStopped = true;

            if (executingThread != null) {
                executingThread.interrupt();
            }
        }
    }
}
