package org.dcache.gplazma.plugins;

import gplazma.authz.util.NameRolePair;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.dcache.auth.FQAN;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

/**
 * Parser for vorolemap files ("<DN>" "<FQAN>" "<USERNAME>") mapping dn/fqan pairs to a username.
 * @author karsten
 */
class VOMapLineParser implements LineParser<VOMapLineParser.DNFQANPredicate, String> {

    private static final Logger _log = LoggerFactory.getLogger(VOMapLineParser.class);

    private static final String SOME_WS = "\\s+";
    private static final String DN_WILDCARD = "\\*";
    private static final String QUOTED_TERM = "\"[^\"]*\"";
    private static final String UNQUOTED_DN = "(?:/[\\w\\d\\s,;:@\\-\\*\\.=]+)+";
    private static final String DN = DN_WILDCARD +"|(?:"+ UNQUOTED_DN +")|(?:"+ QUOTED_TERM + ")";
    private static final String UNQUOTED_FQAN = "(?:/[\\w\\d,;:@\\-\\*\\.]+)+(?:/[\\w\\d\\s,;:@\\-\\*=]+)*";
    private static final String FQAN = "(?:" + UNQUOTED_FQAN +")|(?:"+ QUOTED_TERM + ")";
    private static final String USERNAME = "[\\w\\d]+";

    private static final Pattern ROLE_MAP_FILE_LINE_PATTERN = Pattern.compile("(?:"+SOME_WS+")?" + "("+ DN +")"+ "(?:" + SOME_WS + "("+ FQAN +"))?" + SOME_WS + "("+ USERNAME +")");
    // assembles to:(?:\s+)?(\*|(?:(?:/[\w\d\s,;:@\*\.=]+)+)|(?:"[^"]*"))(?:\s+((?:(?:/[\w\d,;:@\*\.]+)+(?:/[\w\d\s,;:@\*=]+)*)|(?:"[^"]*")))?\s+([\w\d]+)

    private static final int RM_DN_GROUP = 1;
    private static final int RM_FQAN_GROUP = 2;
    private static final int RM_KEY_GROUP = 3;

    @Override
    public Map.Entry<DNFQANPredicate, String> accept(String line) {
        if (Strings.isNullOrEmpty(line.trim()) || line.startsWith("#")) return null;

        Matcher matcher = ROLE_MAP_FILE_LINE_PATTERN.matcher(line);
        if (matcher.lookingAt()) {
            String dn = matcher.group(RM_DN_GROUP).replace("\"", "");
            String vorole = matcher.group(RM_FQAN_GROUP) == null? "":matcher.group(RM_FQAN_GROUP).replace("\"", "");

            FQAN fqan = new FQAN(vorole);

            return new DNFQANStringEntry(new DNFQANPredicate(dn, fqan.toString()), matcher.group(RM_KEY_GROUP));
        }
        _log.warn("Ignored malformed line in VORoleMap-File: '{}'", line);
        return null;
    }

    static class DNFQANPredicate implements MapPredicate<NameRolePair> {

        private final Pattern _dnPattern;
        private final Pattern _fqanPattern;

        public DNFQANPredicate(String dnPattern, String fqanPattern) {
            _dnPattern = Pattern.compile(dnPattern.replaceAll("\\*", ".*"));
            _fqanPattern = Pattern.compile(fqanPattern.replaceAll("\\*",".*"));
        }

        @Override
        public boolean matches(NameRolePair dnfqan) {
            return _dnPattern.matcher(dnfqan.getName()).matches() && _fqanPattern.matcher(dnfqan.getRole()).lookingAt();
        }
    }

    private final class DNFQANStringEntry implements Map.Entry<DNFQANPredicate, String> {

        private final DNFQANPredicate _key;
        private String _value;

        public DNFQANStringEntry(DNFQANPredicate key, String value) {
            _key = key;
            _value = value;
        }

        @Override
        public DNFQANPredicate getKey() {
            return _key;
        }

        @Override
        public String getValue() {
            return _value;
        }

        @Override
        public String setValue(String value) {
            return _value = value;
        }

    }
}
