package org.dcache.acl.util.thread;

import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * @author David Melkumyan, DESY Zeuthen
 *
 */
public class StreamReaderThread extends Thread {
    StringBuilder _out;

    InputStreamReader _in;

    public StreamReaderThread(InputStream in, StringBuilder out) {
        _out = out;
        _in = new InputStreamReader(in);
    }

    public void run() {
        int ch;
        try {
            while (-1 != (ch = _in.read()))
                _out.append((char) ch);

        } catch (Exception e) {
            _out.append("\nRead error:" + e.getMessage());
        }
    }
}