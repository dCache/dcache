package org.dcache.gplazma.util;

import com.google.common.collect.ImmutableMultimap;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GridMapFile {

    private static final Logger LOGGER =
          LoggerFactory.getLogger(GridMapFile.class);

    private File _file;
    private long _loaded;

    /**
     * DNs are expected in openssl format: "/KEY1=value1/KEY2=value2"
     */

    private static final Pattern KEY_PATTERN = Pattern.compile("/[^=]+=");

    private static class DnUsername {

        private String dn;
        private String username;

        DnUsername(String dn, String username) {
            this.dn = dn;
            this.username = username;
        }

        String getDn() {
            return dn;
        }

        String getUsername() {
            return username;
        }

        public String toString() {
            return "(" + dn + "," + username + ")";
        }

    }

    /**
     * map that stores DN with attribute names in upper case as a key and pair (original dn,
     * username) as value. This is needed to perform case insensitive match when comparing user
     * DNs.
     */

    private ImmutableMultimap<String, DnUsername> _map = ImmutableMultimap.of();

    public GridMapFile(File file) {
        _file = file;
        refresh();
    }

    public GridMapFile(String filename) {
        this(new File(filename));
    }

    public synchronized void refresh() {
        try {
            long now = System.currentTimeMillis();
            boolean readable = _file.canRead() || _loaded == 0;
            if (!readable) {
                LOGGER.error("WARNING: Could not read grid-mapfile. Will use cached copy.");
            } else if (_loaded < _file.lastModified()) {
                LOGGER.debug("GridMapFileHandler reading {}", _file);
                try (BufferedReader reader = new BufferedReader(new FileReader(_file))) {
                    _map = read(reader);
                    _loaded = now;
                }

            }
        } catch (IOException e) {
            LOGGER.error("Failed to load grid-mapfile: {}", e.getMessage());
        }
    }

    private static ImmutableMultimap<String, DnUsername> read(BufferedReader reader)
          throws IOException {
        ImmutableMultimap.Builder<String, DnUsername> map =
              ImmutableMultimap.builder();
        String line;
        while ((line = reader.readLine()) != null) {
            line = line.trim();
            if (line.length() == 0 || line.charAt(0) != '\"') {
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
                map.put(capitalizeLabels(dn.trim()),
                      new DnUsername(dn.trim(), removeQuotes(user.trim())));
            }
        }
        return map.build();
    }

    private static String removeQuotes(String quotedString) {
        if (quotedString != null) {
            quotedString = quotedString.replace('\"', ' ').trim();
        }
        return quotedString;
    }

    public String getMappedUsername(String dn) {
        Collection<String> names = getMappedUsernames(dn);
        return names.isEmpty() ? null : names.iterator().next();
    }

    public Collection<String> getMappedUsernames(String dn) {
        /**
         * input DN string needs to me massaged to capitalize attribute names
         */
        Collection<DnUsername> dnNamePairs = _map.get(capitalizeLabels(dn));
        if (!dnNamePairs.isEmpty() && LOGGER.isDebugEnabled()) {
            LOGGER.debug("User DN \"{}\" matched to the following (DN, Username) pairs:\"{}",
                  dn,
                  dnNamePairs.stream()
                        .map(i -> i.toString())
                        .collect(Collectors.joining(",", "[", "]")));
        }
        Collection<String> names = dnNamePairs.stream()
              .map(i -> i.getUsername())
              .collect(Collectors.toList());
        return names;
    }

    private static String capitalizeLabels(String dn) {
        Matcher m = KEY_PATTERN.matcher(dn);
        StringBuilder output = new StringBuilder();
        int i = 0;
        while (m.find()) {
            output.append(dn.substring(i, m.start()));
            String label = m.group().toUpperCase();
            output.append(label);
            i = m.end();
        }
        output.append(dn.substring(i, dn.length()));
        return output.toString();
    }
}
