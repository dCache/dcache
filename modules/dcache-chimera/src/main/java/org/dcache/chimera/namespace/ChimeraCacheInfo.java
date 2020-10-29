/*
 * $Id: ChimeraCacheInfo.java,v 1.1 2007-06-19 10:06:33 tigran Exp $
 */
package org.dcache.chimera.namespace;

import java.io.BufferedReader;
import java.io.CharArrayReader;
import java.io.CharArrayWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.FileNotFoundHimeraFsException;
import org.dcache.chimera.FsInode;
import org.dcache.chimera.posix.Stat;

public class ChimeraCacheInfo implements Serializable {

    private static final long serialVersionUID = -2625909340883039244L;
    private CacheFlags _cacheFlags;

    public static class CacheFlags implements Serializable {

        private static final long serialVersionUID = -5709322056371005585L;

        private final Map<String, String> _hash = new HashMap<>();

        private final StringBuilder _inputLine = new StringBuilder();

        private void addLine(String line) {
            _inputLine.append(line);
        }

        private void commit() {
            StringTokenizer st = new StringTokenizer(_inputLine.toString(), ";");
            while (st.hasMoreTokens()) {
                String t = st.nextToken();
                int l = t.length();
                if (l == 0) {
                    continue;
                }
                int i = t.indexOf('=');
                if (i < 0) {
                    _hash.put(t, "");
                } else if (i == (l - 1)) {
                    _hash.put(t.substring(0, i), "");
                } else if (i > 0) {
                    _hash.put(t.substring(0, i), t.substring(i + 1));
                }
            }
        }

        public String get(String key) {
            return _hash.get(key);
        }

        public String remove(String key) {
            return _hash.remove(key);
        }

        public void put(String key, String value) {
            _hash.put(key, value);
        }

        public Set<Map.Entry<String, String>> entrySet() {
            return _hash.entrySet();
        }

        private String toPnfsString() {
            StringBuilder sb = new StringBuilder();
            int l = 0;
            sb.append(":");
            for (Map.Entry<String, String> entry : _hash.entrySet()) {
                sb.append(entry.getKey()).append("=").append(entry.getValue())
                        .append(";");
                if ((sb.length() - l) > 70) {
                    l = sb.length();
                    sb.append("\n:");
                }
            }
            return sb.toString();
        }

        public String toString() {
            return toPnfsString();
        }
    }

    public CacheFlags getFlags() {
        return _cacheFlags;
    }

    private void readCacheInfo(BufferedReader file) throws IOException {

        _cacheFlags = new CacheFlags();

        // First line is cache statistics, obsolete.
        String line = file.readLine();
        if (line == null) {
            return;
        }

        // accept only lines in form ':key=value;'
        while ((line = file.readLine()) != null) {
            if (line.length() == 0) {
                continue;
            }
            if (line.charAt(0) == ':') {
                if (line.length() > 1) {
                    _cacheFlags.addLine(line.substring(1));
                }
            }
        }
        _cacheFlags.commit();
    }

    public void writeCacheInfo(FsInode inode) throws ChimeraFsException {

        //
        // currently we accept 1 and 2 but we only write 2.
        //
        CharArrayWriter cw = new CharArrayWriter();
        try (PrintWriter pw = new PrintWriter(cw)) {
            pw.println(); // place holder of cache statistics. Obsolete.
            pw.println(_cacheFlags.toPnfsString());
        }

        byte[] buff = cw.toString().getBytes();
        inode.write(0, buff, 0, buff.length);
    }

    public String toString() {
        return "";
    }

    public ChimeraCacheInfo(FsInode inode) throws ChimeraFsException, IOException {

        byte[] buff;
        int len = 0;
        try {
            Stat stat = inode.stat();
            buff = new byte[(int) stat.getSize()];
            len = inode.read(0, buff, 0, buff.length);
        } catch (FileNotFoundHimeraFsException fnf) {
            buff = new byte[0];
        }

        CharArrayReader ca = new CharArrayReader(new String(buff, 0, len).toCharArray());
        try (BufferedReader br = new BufferedReader(ca)) {
            readCacheInfo(br);
        }
    }
}
