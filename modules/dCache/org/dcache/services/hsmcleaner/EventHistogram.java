package org.dcache.services.hsmcleaner;

import java.io.PrintWriter;
import java.util.Arrays;

public class EventHistogram
{
    private long _period;
    private int[] _counters;
    private long _time;
    private final String _format;

    public EventHistogram(int slots, long period)
    {
        this(slots, period, "%1$tD %1$tR %2$-50s [ %3$d events ]%n");
    }

    public EventHistogram(int slots, long period, String format)
    {
        _period = period;
        _counters = new int[slots];
        _time = System.currentTimeMillis() - slots * period;
        _format = format;
    }

    /**
     * Shifts the elements of <code>_counters</code> such that the
     * current time falls within the first slot.
     */
    private void shift()
    {
        int slots = _counters.length;
        double d = System.currentTimeMillis() - (_time + slots * _period);

        int shift = Math.max(0, (int)Math.ceil(d / _period));

        System.arraycopy(_counters, 0,
                         _counters, Math.min(shift, slots),
                         Math.max(0, slots - shift));
        Arrays.fill(_counters, 0, shift, 0);

        _time += shift * _period;
    }

    /**
     * Returns the largest value in the <code>_counters</code> array.
     */
    private int getMax()
    {
        int value = 0;
        for (int counter : _counters) {
            value = Math.max(value, counter);
        }
        return value;
    }

    /**
     * Returns the string with <i>c</i> repeated <i>count</i> times.
     */
    private String repeat(char c, int count)
    {
        StringBuilder builder = new StringBuilder(count);
        for (int i = 0; i < count; i++) {
            builder.append(c);
        }
        return builder.toString();
    }

    /**
     * Writes the histogram to <i>out</i>.
     */
    public void write(PrintWriter out)
    {
        shift();

        int max = Math.max(1, getMax());
        int slots = _counters.length;
        for (int i = 0; i < slots; i++) {
            int events = _counters[slots - i - 1];
            out.printf(_format, _time + i * _period,
                       repeat('#', events / max), events);
        }
    }

    /**
     * Add an event.
     */
    public void add()
    {
        shift();
        _counters[0]++;
    }
}