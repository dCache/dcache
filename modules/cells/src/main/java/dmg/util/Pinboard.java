package dmg.util;

import com.google.common.collect.EvictingQueue;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Queue;

import static java.lang.Math.max;

public class Pinboard
{
    private final Queue<String> _entries;

    public Pinboard(int size)
    {
        _entries = EvictingQueue.create(size);
    }

    public synchronized void pin(String note)
    {
        _entries.add(note);
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
}
