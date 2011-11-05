package org.dcache.util;

import java.io.Serializable;

/**
 * Immutable class for representing intervals.
 */
public class Interval implements Serializable
{
    static final long serialVersionUID = 4755266838804807324L;

    public final long lower;
    public final long upper;

    public Interval(long lower, long upper)
    {
        this.lower = lower;
        this.upper = upper;
    }

    public boolean contains(long value)
    {
        return lower <= value && value <= upper;
    }

    public long getLower()
    {
        return lower;
    }

    public long getUpper()
    {
        return upper;
    }

    /**
     * Parse an interval in the format N..M. If N or M is omitted,
     * then the interval will be open-ended (represented by
     * Long.MIN_VALUE and Long.MAX_VALUE, respectively).
     *
     * @throws IllegalArgumentException in case of syntax errors.
     */
    public static Interval parseInterval(String s)
        throws IllegalArgumentException
    {
        long lower, upper;
        String[] bounds = s.split("\\.\\.", 2);
        switch (bounds.length) {
        case 1:
            lower = upper = Long.parseLong(bounds[0]);
            break;

        case 2:
            lower =
                (bounds[0].length() == 0)
                ? Long.MIN_VALUE
                : Long.parseLong(bounds[0]);
            upper =
                (bounds[1].length() == 0)
                ? Long.MAX_VALUE
                : Long.parseLong(bounds[1]);
            break;

        default:
            throw new IllegalArgumentException(s + ": Invalid interval");
        }
        return new Interval(lower, upper);
    }

    @Override
    public String toString()
    {
        return String.format("[%d,%d]", lower, upper);
    }
}