package dmg.util;

import com.google.common.collect.EvictingQueue;
import com.google.common.collect.Iterables;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.util.Queue;

public class Pinboard
{
    private static final ThreadLocal<DateFormat> TIMESTAMP_FORMAT =
            new ThreadLocal<DateFormat>()
            {
                @Override
                protected DateFormat initialValue()
                {
                    return DateFormat.getTimeInstance();
                }
            };

    private final Queue<PinEntry> _entries;

    public Pinboard(int size)
    {
        _entries = EvictingQueue.create(size);
    }

    public synchronized void pin(String note)
    {
        _entries.add(new PinEntry(note));
    }

    public synchronized void dump(StringBuilder sb)
    {
        dump(sb, _entries);
    }

    public synchronized void dump(StringBuilder sb, int last)
    {
        dump(sb, Iterables.skip(_entries, _entries.size() - last));
    }

    public synchronized void dump(File file) throws IOException
    {
        dump(file, _entries);
    }

    public synchronized void dump(File file, int last) throws IOException
    {
        dump(file, Iterables.skip(_entries, _entries.size() - last));
    }

    private void dump(StringBuilder sb, Iterable<PinEntry> entries)
    {
        DateFormat format = TIMESTAMP_FORMAT.get();
        for (PinEntry entry : entries) {
            sb.append(format.format(entry.timestamp)).append(' ').append(entry.message).append('\n');
        }
    }

    private void dump(File file, Iterable<PinEntry> entries) throws IOException
    {
        DateFormat format = TIMESTAMP_FORMAT.get();
        try (PrintWriter pw = new PrintWriter(new FileWriter(file))) {
            for (PinEntry entry : entries) {
                pw.append(format.format(entry.timestamp)).append(' ').println(entry.message);
            }
        }
    }

    private static class PinEntry
    {
        final String message;
        final long timestamp;

        public PinEntry(String message)
        {
            this.message = message;
            this.timestamp = System.currentTimeMillis();
        }
    }
}
