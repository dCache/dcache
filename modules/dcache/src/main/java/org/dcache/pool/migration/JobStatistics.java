package org.dcache.pool.migration;

/**
 * Encapsulates statistics about a job.
 */
public class JobStatistics
{
    private int _completed;
    private int _attempts;
    private long _transferred;
    private long _total;

    public synchronized int getCompleted()
    {
        return _completed;
    }

    public synchronized long getTransferred()
    {
        return _transferred;
    }

    public synchronized int getAttempts()
    {
        return _attempts;
    }

    public synchronized void addCompleted(long bytes)
    {
        _completed++;
        _transferred += bytes;
    }

    public synchronized void addAttempt()
    {
        _attempts++;
    }

    public synchronized long getTotal()
    {
        return _total;
    }

    public synchronized void addToTotal(long bytes)
    {
        _total += bytes;
    }
}
