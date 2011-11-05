// $Id: CacheInfo.java,v 1.8 2007-05-24 13:51:05 tigran Exp $

package diskCacheV111.vehicles;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import diskCacheV111.util.PnfsFile;

public class CacheInfo implements Serializable {
    //
    // The level in PNFS dedicated to the cache system
    //
    private static final int __level = 2;
    private CacheStatistics _cacheStatistics;
    private final List<String> _cacheLocations = new ArrayList<String>();
    private CacheFlags _cacheFlags = null;

    static final long serialVersionUID = -7449372587084685726L;

    public static class CacheFlags implements Serializable
    {
        static final long serialVersionUID = 6894258376947344044L;

        private Map<String, String> _hash = new HashMap<String, String>();
        private StringBuilder _inputLine = new StringBuilder();

        private void addLine(String line) {
            _inputLine.append(line);
        }

        private void commit() {
            StringTokenizer st = new StringTokenizer(_inputLine.toString(), ";");
            while (st.hasMoreTokens()) {
                String t = st.nextToken();
                int l = t.length();
                if (l == 0)
                    continue;
                int i = t.indexOf('=');
                if ((i < 0) || (i == (l - 1))) {
                    _hash.put(t, "");
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
        //
        // First line is cache statistics
        //
        _cacheLocations.clear();
        _cacheFlags = new CacheFlags();

        String line = file.readLine();

        if (line == null) {
            _cacheStatistics = new CacheStatistics();
            return;
        }

        _cacheStatistics = new CacheStatistics(line);

        while ((line = file.readLine()) != null) {
            if (line.length() == 0)
                continue;
            if (line.charAt(0) == ':') {
                if (line.length() > 1)
                    _cacheFlags.addLine(line.substring(1));
            } else {
                _cacheLocations.add(line);
            }
        }
        _cacheFlags.commit();
    }

    public void writeCacheInfo(PnfsFile pnfsFile) throws IOException {
        File f = pnfsFile.getLevelFile(__level);

        //
        // currently we accept 1 and 2 but we only write 2.
        //
        _cacheStatistics.setVersion(2);

        PrintWriter pw = new PrintWriter(new FileWriter(f));

        try {

            pw.println(_cacheStatistics.toPnfsString());
            pw.println(_cacheFlags.toPnfsString());

            for (String location : _cacheLocations) {
                pw.println(location);
            }

        } finally {
            pw.close();
        }
    }

    public void addCacheLocation(String location) {
        _cacheLocations.add(location);
    }

    public boolean clearCacheLocation(String location) {
        // Returns true if location was actually in the list
        return _cacheLocations.remove(location);
    }

    public List<String> getCacheLocations() {
        return _cacheLocations;
    }

    public CacheStatistics getCacheStatistics() {
        return _cacheStatistics;
    }

    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(_cacheStatistics.toString());

        for (String location : _cacheLocations) {
            sb.append(" ").append(location);
        }

        return sb.toString();
    }

    // XXX should the update method move from CacheStatistics to here?
    public void setCacheStatistics(CacheStatistics cs) {
        _cacheStatistics = cs;
    }

    public CacheInfo(PnfsFile pnfsFile) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(pnfsFile
                .getLevelFile(__level)));
        try {
            readCacheInfo(br);
        } finally {
            br.close();
        }
    }
}
