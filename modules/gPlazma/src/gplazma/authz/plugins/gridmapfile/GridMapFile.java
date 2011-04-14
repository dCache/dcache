package gplazma.authz.plugins.gridmapfile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.Collection;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableMultimap;

/**
 *
 *  @author Abhishek Singh Rana
 */
public class GridMapFile
{
    private static final Logger log =
        LoggerFactory.getLogger(GridMapFile.class);

    private static final String GRIDMAP_FILENAME = "grid-mapfile";
    private static ImmutableMultimap<String,String> cache =
        ImmutableMultimap.of();
    private static long previousRefreshTime;

    private ImmutableMultimap<String,String> gridMap;

    public GridMapFile(File file)
        throws IOException
    {
        if (!file.getName().equals(GRIDMAP_FILENAME)) {
            log.warn("The grid-mapfile filename {} is not as expected.", file);
            log.warn("WARNING: Possible security violation.");
        }
        log.debug("GridMapFileHandler reading {}", file);
        refresh(file);
        gridMap = cache;
    }

    public GridMapFile(String filename)
        throws IOException
    {
        this(new File(filename));
    }

    private static synchronized void refresh(File file) throws IOException
    {
        long now = System.currentTimeMillis();
        boolean readable = file.canRead() || previousRefreshTime==0;
        if (!readable) {
            log.error("WARNING: Could not read grid-mapfile. Will use cached copy.");
        }
        if (readable && previousRefreshTime < file.lastModified()) {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            try {
                cache = read(reader);
                previousRefreshTime = now;
            } finally {
                reader.close();
            }
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
        ImmutableCollection<String> names = gridMap.get(dn);
        return names.isEmpty() ? null : names.asList().get(0);
    }

    public Collection<String> getMappedUsernames(String dn)
    {
        return gridMap.get(dn);
    }
}
