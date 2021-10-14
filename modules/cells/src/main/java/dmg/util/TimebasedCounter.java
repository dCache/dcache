package dmg.util;

/**
 * Utility class to generate a monotonically increasing sequence of values.
 * <p>
 * Under the right conditions, this sequence has a high likelihood to also be monotonically
 * increasing between restarts. This is the case when
 * <p>
 * - Time goes forward.
 * <p>
 * - The frequency at which values are generated before a restart does not exceed 1 MHz.
 * <p>
 * The main reason for using time as a component is to avoid having to maintain persistent state.
 */
public class TimebasedCounter {

    private long _last;

    public synchronized long next() {
        return (_last = Math.max(_last + 1, System.currentTimeMillis() * 1_000));
    }
}
