package org.dcache.gplazma.util;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableMultimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;

public class GridMapFile
{
    private static final Logger _log =
        LoggerFactory.getLogger(GridMapFile.class);

    private File _file;
    private long _loaded;
    private ImmutableMultimap<String,String> _map = ImmutableMultimap.of();

    public GridMapFile(File file)
    {
        _file = file;
        refresh();
    }

    public GridMapFile(String filename)
    {
        this(new File(filename));
    }

    public synchronized void refresh()
    {
        try {
            long now = System.currentTimeMillis();
            boolean readable = _file.canRead() || _loaded == 0;
            if (!readable) {
                _log.error("WARNING: Could not read grid-mapfile. Will use cached copy.");
            } else if (_loaded < _file.lastModified()) {
                _log.debug("GridMapFileHandler reading {}", _file);
                try (BufferedReader reader = new BufferedReader(new FileReader(_file))) {
                    _map = read(reader);
                    _loaded = now;
                }

            }
        } catch (IOException e) {
            _log.error("Failed to load grid-mapfile: " + e.getMessage());
        }
    }

    private static ImmutableMultimap<String,String> read(BufferedReader reader)
	throws IOException
    {
        ImmutableMultimap.Builder<String,String> map =
            ImmutableMultimap.builder();
        String line;
        while ((line = reader.readLine()) != null) {
            line = line.trim();
            if (line.length()==0 || line.charAt(0) != '\"') {
                continue;
            }
            line = line.substring(1);
            int last_quote = line.lastIndexOf('\"');
            if (last_quote == -1) {
                continue;
            }
            String dn = line.substring(0, last_quote);
            String user = line.substring(last_quote + 1).trim();
            if (user != null && user.length() > 0) {
                map.put(dn.trim(), removeQuotes(user.trim()));
            }
        }
        return map.build();
    }

    private static String removeQuotes(String quotedString)
    {
        if (quotedString != null) {
            quotedString = quotedString.replace('\"',' ').trim();
        }
        return quotedString;
    }

    public String getMappedUsername(String dn)
    {
        ImmutableCollection<String> names = _map.get(dn);
        return names.isEmpty() ? null : names.asList().get(0);
    }

    public Collection<String> getMappedUsernames(String dn)
    {
        return _map.get(dn);
    }
}
