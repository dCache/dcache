package org.dcache.webdav;

import org.dcache.util.ByteUnit;

import static org.dcache.util.ByteUnit.BYTES;
import static org.dcache.util.ByteUnit.Type.BINARY;
import static org.dcache.util.ByteUnits.isoSymbol;

/**
 * Class to hold information about a file's size.  In particular, it
 * allows the StringTemplate language to access the file's size in different
 * formats.
 */
public class SizeWrapper
{
    private final long _size;
    private final String _humanFriendly;

    public SizeWrapper(long size)
    {
        _size = size;
        _humanFriendly = asReadableString(size);
    }

    private static String asReadableString(long size)
    {
        if (size == 0) {
            return "Empty";
        }

        StringBuilder sb = new StringBuilder();
        ByteUnit units = BINARY.unitsOf(size, 0.8);
        if (units == BYTES) {
            sb.append(size);
        } else {
            double val = units.convert((double)size, BYTES);
            String fmt;
            if (val >= 99.5) {
                fmt = "%.0f";
            } else if (val >= 9.95) {
                fmt = "%.1f";
            } else {
                fmt = "%.2f";
            }
            sb.append(String.format(fmt, val));
        }

        return sb.append(' ').append(isoSymbol().of(units)).toString();
    }

    @Override
    public String toString()
    {
        return String.valueOf(_size);
    }

    public String getHumanFriendly()
    {
        return _humanFriendly;
    }
}
