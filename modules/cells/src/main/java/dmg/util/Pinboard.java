package dmg.util;

import com.google.common.collect.EvictingQueue;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import java.util.Queue;

import static java.lang.Math.max;

public class Pinboard
{
    private static final DateTimeFormatter TIMESTAMP_FORMAT =
            DateTimeFormatter.ofLocalizedTime(FormatStyle.MEDIUM).withZone(ZoneId.systemDefault());

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
        _entries.forEach(e -> sb.append(e).append('\n'));
    }

    public synchronized void dump(StringBuilder sb, int last)
    {
        _entries.stream().skip(max(0, _entries.size() - last)).forEach(e -> sb.append(e).append('\n'));
    }

    public synchronized void dump(File file) throws IOException
    {
        try (PrintWriter pw = new PrintWriter(new FileWriter(file))) {
            _entries.forEach(pw::println);
        }
    }

    public synchronized void dump(File file, int last) throws IOException
    {
        try (PrintWriter pw = new PrintWriter(new FileWriter(file))) {
            _entries.stream().skip(max(0, _entries.size() - last)).forEach(pw::println);
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

        @Override
        public String toString()
        {
            return TIMESTAMP_FORMAT.format(Instant.ofEpochMilli(timestamp)) + ' ' + message;
        }
    }
}
