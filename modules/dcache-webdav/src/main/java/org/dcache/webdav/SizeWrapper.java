package org.dcache.webdav;

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

        if (size < 0.8*(1L<<10)) {
            return String.valueOf(size) + " B";
        }

        double val;
        String units;
        if (size < 0.8*(1L<<20)) {
            val = (double) size / (1L<<10);
            units = "kiB";
        } else if (size < 0.8*(1L<<30)) {
            val = (double) size / (1L<<20);
            units = "MiB";
        } else if (size < 0.8*(1L<<40)) {
            val = (double) size / (1L<<30);
            units = "GiB";
        } else if (size < 0.8*(1L<<50)) {
            val = (double) size / (1L<<40);
            units = "TiB";
        } else {
            val = (double) size / (1L<<50);
            units = "PiB";
        }
        String fmt;
        if (val >= 99.5) {
            fmt = "%.0f";
        } else if (val >= 9.95) {
            fmt = "%.1f";
        } else {
            fmt = "%.2f";
        }
        return String.format(fmt, val) + " " + units;
    };

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
