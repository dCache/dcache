package diskCacheV111.util;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Immutable quantity of disk space.
 *
 * Essentially just an integer, but can parse suffixes for kibibytes, mebibytes,
 * gibibytes, and tebibytes.
 *
 * The quantity can be unspecified, with the string representation being a dash.
 * This is represented as Long.MAX_VALUE bytes, the quantity larger than any other
 * disk space.
 *
 * Disk space is always positive.
 */
public class DiskSpace
{
    public static final DiskSpace UNSPECIFIED = new DiskSpace(Long.MAX_VALUE);

    private static final long TEBI = (1L << 40);
    private static final long GIBI = (1L << 30);
    private static final long MEBI = (1L << 20);
    private static final long KIBI = (1L << 10);

    private final long _value;

    public DiskSpace(long value)
    {
        checkArgument(value >= 0, "Negative value is not allowed");
        _value = value;
    }

    public DiskSpace(String s)
    {
        this(parseUnitLong(s));
    }

    private static long parseUnitLong(String s)
    {
        checkArgument(!s.isEmpty(), "Argument must not be empty");

        if (s.equals("Infinity") || s.equals("-")) {
            return Long.MAX_VALUE;
        }

        String num;
        long multi;
        switch (s.charAt(s.length() - 1)) {
        case 'k':
        case 'K':
            multi = KIBI;
            num = s.substring(0, s.length() - 1);
            break;
        case 'm':
        case 'M':
            multi = MEBI;
            num = s.substring(0, s.length() - 1);
            break;
        case 'g':
        case 'G':
            multi = GIBI;
            num = s.substring(0, s.length() - 1);
            break;
        case 't':
        case 'T':
            multi = TEBI;
            num = s.substring(0, s.length() - 1);
            break;
        default:
            multi = 1;
            num = s;
        }
        return Long.parseLong(num) * multi;
    }

    public String toString()
    {
        return toUnitString();
    }

    public String toUnitString()
    {
        return toUnitString(_value);
    }

    public static String toUnitString(long value)
    {
        long tmp;
        if (value == Long.MAX_VALUE) {
            return "-";
        }
        if (((tmp = (value / TEBI)) > 0) && ((value % TEBI) == 0)) {
            return Long.toString(tmp) + "T";
        }
        if (((tmp = (value / GIBI)) > 0) && ((value % GIBI) == 0)) {
            return Long.toString(tmp) + "G";
        }
        if (((tmp = (value / MEBI)) > 0) && ((value % MEBI) == 0)) {
            return Long.toString(tmp) + "M";
        }
        if (((tmp = (value / KIBI)) > 0) && ((value % KIBI) == 0)) {
            return Long.toString(tmp) + "k";
        }
        return Long.toString(value);
    }

    public long longValue()
    {
        return _value;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DiskSpace that = (DiskSpace) o;
        return _value == that._value;

    }

    @Override
    public int hashCode()
    {
        return (int) (_value ^ (_value >>> 32));
    }

    public boolean isLargerThan(long value)
    {
        return value < _value && _value < Long.MAX_VALUE;
    }

    public boolean isLessThan(long value)
    {
        return _value < value;
    }

    public boolean isSpecified()
    {
        return _value != Long.MAX_VALUE;
    }

    public long orElse(long other)
    {
        return isSpecified() ? _value : other;
    }

    public DiskSpace orElse(DiskSpace other)
    {
        return isSpecified() ? this : other;
    }
}

