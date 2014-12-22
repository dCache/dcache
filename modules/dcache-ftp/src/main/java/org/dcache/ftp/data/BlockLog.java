package org.dcache.ftp.data;

import java.text.MessageFormat;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * A log of transferred blocks. Adjacent blocks are merged, so for a
 * sequential transfer only information about a single block will be
 * maintained.
 *
 * Overlapping blocks are detected and reported via an exception.
 *
 * The class is thread safe.
 */
public class BlockLog
{
    private SortedMap<Long,Long> _blocks = new TreeMap<>();
    private boolean _eof;

    private static final String _overlapMsg
            = "Overlapping block detected between ({0}-{1}) and ({2}-{3}).";

    private ErrorListener _errorListener;

    private long _limit;

    public BlockLog(ErrorListener errorListener)
    {
        _errorListener = errorListener;
        _limit = Long.MAX_VALUE;
    }

    /**
     * Adds a block to the log. Blocks do not need to be added
     * sequentially.
     *
     * @param position A non-negative starting posisiton
     * @param size     A non-negative block size
     * @throws FTPException if blocks overlap
     */
    public synchronized void addBlock(long position, long size)
            throws FTPException
    {
        if (size == 0) {
            return;
        }

        long begin = position;
        long end   = position + size;

        SortedMap<Long,Long> headMap = _blocks.headMap(position);
        SortedMap<Long,Long> tailMap = _blocks.tailMap(position);

        if (!headMap.isEmpty()) {
            long prevBegin = headMap.lastKey();
            long prevEnd = _blocks.get(prevBegin);

            /* Consistency check.
             */
            if (prevEnd > begin) {
                String err =
                        MessageFormat.format(_overlapMsg,
                                             begin, end, prevBegin, prevEnd);
                throw new FTPException(err);
            }

            /* Merge blocks.
             */
            if (prevEnd == begin) {
                begin = prevBegin;
                _blocks.remove(prevBegin);
            }
        }
        if (!tailMap.isEmpty()) {
            long nextBegin = tailMap.firstKey();
            long nextEnd   = _blocks.get(nextBegin);

            /* Consistency check.
             */
            if (end > nextBegin) {
                String err =
                        MessageFormat.format(_overlapMsg,
                                             begin, end, nextBegin, nextEnd);
                throw new FTPException(err);
            }

            /* Merge blocks.
             */
            if (end == nextBegin) {
                end = nextEnd;
                _blocks.remove(nextBegin);
            }
        }

        _blocks.put(begin, end);

        notifyAll();

        /* The transfer can be throttled by setting a limit. Once
         * everything up to that limit has been received, we block
         * until the limit is raised. This assumes that addBlock was
         * called from the same thread, which transfers the data.
         */
        try {
            while (_limit <= getCompleted()) {
                wait();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Indicates that the end of the transfer has been reached. The
     * main purpose is to make sure that waitCompleted() returns.
     */
    public synchronized void setEof()
    {
        _eof = true;
        notifyAll();
    }

    /**
     * Returns true when setEof() has been called.
     */
    public synchronized boolean isEof()
    {
        return _eof;
    }

    /**
     * Returns the number of fragments received, i.e. continous areas
     * of the file for which either the byte before or the byte after
     * the area has not been received.
     */
    public synchronized int getFragments()
    {
        return _blocks.size();
    }

    /**
     * Returns true when the complete file has been received. This is
     * the case if EOF has been set and the complete file only
     * contains one fragment.
     */
    public synchronized boolean isComplete()
    {
        return isEof() && getFragments() <= 1;
    }

    /**
     * Returns the number of consecutive bytes from the beginning of
     * the file that have been transferred.
     */
    public synchronized long getCompleted()
    {
        return !_blocks.containsKey(0L) ? 0L : _blocks.get(0L);
    }

    /**
     * Blocks until getCompleted() returns a value larger than or
     * equal to position or until setEof() has been called.
     */
    public synchronized void waitCompleted(long position)
            throws InterruptedException
    {
        if (_limit < position) {
            setLimit(position);
        }
        while (getCompleted() < position && !isEof()) {
            wait();
        }
    }

    /**
     * Returns the current transfer limit.
     */
    public synchronized long getLimit()
    {
        return _limit;
    }

    /**
     * Sets the current transfer limit.
     *
     * The <code>addBlock</code> method will block as soon as
     * <code>getCompleted</code> would return something not smaller
     * than <code>limit</code>. The idea is that the limit will
     * throttle the transfer as soon as everything up to
     * <code>limit</code> has been received.
     *
     * Using Long.MAX_VALUE for the limit effectively disables
     * transfer throttling.
     *
     * @param limit the new limit
     */
    public synchronized void setLimit(long limit)
    {
        _limit = limit;
        notifyAll();
    }
}
