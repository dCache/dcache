package dmg.util;

/**
 * Utility class to generate a monotonically increasing sequence of
 * values.
 *
 * Under the right conditions, this sequence has a high likelihood to
 * also be monotonically increasing between restarts. This is the case
 * when
 *
 * - Time goes forward.
 *
 * - The frequency at which values are generated between two restarts
 *   A and B is lower than 1000 Hz.
 *
 * The main reason for using time as a component is to avoid having to
 * maintain persistent state.
 */
public class TimebasedCounter
{
    private long _last;

    public synchronized long next()
    {
        return (_last = Math.max(_last + 1, System.currentTimeMillis()));
    }
}
