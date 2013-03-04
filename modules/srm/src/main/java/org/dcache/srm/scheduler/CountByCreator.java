package org.dcache.srm.scheduler;

import java.util.HashMap;
import java.util.Map;

/**
 * This class has the ability to count per job creator. A counter can
 * never be negative.
 *
 * All operations on instances of this class are thread safe.
 */
public class CountByCreator
{
    private final Map<String,Integer> _counters =
        new HashMap<>();
    private int _total;

    public CountByCreator()
    {

    }

    /** Increment the counter for <code>creator</code>. */
    public synchronized void increment(String creatorId)
    {
        _total++;
        Integer value = _counters.get(creatorId);
        if (value == null) {
            _counters.put(creatorId, 1);
        } else {
            _counters.put(creatorId, value + 1);
        }
    }

    /**
     * Decrement the counter for <code>creator</code>. If the counter
     * is zero, then the counter is left unmodified.
     */
    public synchronized void decrement(String creatorId)
    {
        Integer value = _counters.get(creatorId);
        if (value != null) {
            _total--;
            if (value > 1) {
                _counters.put(creatorId, value - 1);
            } else {
                _counters.remove(creatorId);
            }
        }
    }

    /**
     * Returns the value of the counter for <code>creator</code>.
     */
    public synchronized int getValue(String creator)
    {
        Integer value = _counters.get(creator);
        return value == null ? 0 : value;
    }

    /**
     * Returns the sum of all counters.
     */
    public synchronized int getTotal()
    {
        return _total;
    }
}
