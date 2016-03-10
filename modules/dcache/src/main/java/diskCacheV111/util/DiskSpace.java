package diskCacheV111.util;

import java.util.Arrays;

import org.dcache.util.ByteUnit;
import org.dcache.util.ByteUnits.JedecPrefix;
import org.dcache.util.ByteUnits.Representation;

import static com.google.common.base.Preconditions.checkArgument;
import static org.dcache.util.ByteUnit.BYTES;
import static org.dcache.util.ByteUnit.KiB;
import static org.dcache.util.ByteUnit.Type.BINARY;
import static org.dcache.util.ByteUnits.jedecPrefix;

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
    private static final Representation JEDEC_WITH_LOWER_K = new JedecPrefixLowerKRepresentation();

    private final long _value;

    private static class JedecPrefixLowerKRepresentation extends JedecPrefix
    {
        @Override
        public String of(ByteUnit unit)
        {
            return (unit == KiB) ? "k" : super.of(unit);
        }
    }

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

        String lastChar = s.substring(s.length()-1).toUpperCase();
        ByteUnit units = Arrays.stream(ByteUnit.values())
                .skip(1)
                .filter(u -> u.hasType(BINARY) && lastChar.equals(jedecPrefix().of(u)))
                .findFirst()
                .orElse(BYTES);

        String number = units == BYTES ? s : s.substring(0, s.length() -1);
        return units.toBytes(Long.parseLong(number));
    }

    @Override
    public String toString()
    {
        return toUnitString(_value);
    }

    public static String toUnitString(long value)
    {
        if (value == Long.MAX_VALUE) {
            return "-";
        }
        ByteUnit units = BINARY.exactUnitsOf(value);
        return Long.toString(units.convert(value, BYTES)) + JEDEC_WITH_LOWER_K.of(units);
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

