package org.dcache.gplazma.plugins;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A LineBasedParser that reads gridmap files.
 */
public class GridMapFileParser extends PredicateMapParser<String,String> {

    /**
     * DNs are expected in openssl format: "/KEY1=value1/KEY2=value2"
     */

    private static final Pattern KEY_PATTERN = Pattern.compile("/[^=]+=");

    @Override
    public void accept(String line) {
        line = line.trim();
        if (line.length() == 0 || line.charAt(0) != '\"') {
            return;
        }
        line = line.substring(1);
        int last_quote = line.lastIndexOf('\"');
        if (last_quote == -1) {
            return;
        }
        String dn = line.substring(0, last_quote);
        var normalDn = normalise(dn);
        String user = line.substring(last_quote + 1).trim();
        if (user != null && user.length() > 0) {
            accept(normalDn::equals, user);
        }
    }

    /**
     * input DN string needs to me massaged to capitalize attribute names
     * @param rawDn The DN to be normalised.
     * @return The canonical version of that DN.
     */
    @Override
    String normalise(String rawDn) {
        String dn = super.normalise(rawDn);
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
