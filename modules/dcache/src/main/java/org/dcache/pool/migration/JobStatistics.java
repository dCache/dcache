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

    synchronized public int getCompleted()
    {
        return _completed;
    }

    synchronized public long getTransferred()
    {
        return _transferred;
    }

    synchronized public int getAttempts()
    {
        return _attempts;
    }

    synchronized public void addCompleted(long bytes)
    {
        _completed++;
        _transferred += bytes;
    }

    synchronized public void addAttempt()
    {
        _attempts++;
    }

    synchronized public long getTotal()
    {
        return _total;
    }

    synchronized public void addToTotal(long bytes)
    {
        _total += bytes;
    }
}
