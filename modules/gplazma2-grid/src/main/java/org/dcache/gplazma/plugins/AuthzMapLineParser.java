package org.dcache.gplazma.plugins;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Arrays;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author karsten
 *
 */
class AuthzMapLineParser implements LineParser<AuthzMapLineParser.StringPredicate, AuthzMapLineParser.UserAuthzInformation> {

    private static final Logger _log = LoggerFactory.getLogger(AuthzMapLineParser.class);

    private static final String SOME_WS = "\\s+";
    private static final String AUTHORIZE = "[Aa][Uu][Tt][Hh][Oo][Rr][Ii][Zz][Ee]";
    private static final String USERNAME = "[\\w.][\\w.\\-]*";
    private static final String ACCESS = "(?:[Rr][Ee][Aa][Dd]-[Oo][Nn][Ll][Yy])|(?:[Rr][Ee][Aa][Dd]-[Ww][Rr][Ii][Tt][Ee])";
    private static final String UID = "\\d+";
    private static final String GID = "\\d+(?:,\\d+)*";
    private static final String PATH = "(?:\"[^\\00]+\")|(?:(?:(?:\\\\\\s)|[^\\s\\00])+)";
    private static final Pattern USER_MAP_FILE_LINE_PATTERN = Pattern.compile("(?:"+SOME_WS+")?" + AUTHORIZE + SOME_WS + "("+ USERNAME +")" + SOME_WS + "("+ ACCESS +")"+ SOME_WS + "("+ UID +")"+ SOME_WS + "("+ GID +")" + SOME_WS + "("+ PATH +")"+ SOME_WS + "("+ PATH +")"+"(?:"+ SOME_WS +"("+ PATH+"))?" );
    // assembles to: (?:\s+)?authorize\s+([\w\d]+)\s+((?:read-only)|(?:read-write))\s+(\d+)\s+(\d+)\s+((?:"[^\00]+")|(?:(?:(?:\\\s)|[^\s\00])+))\s+((?:"[^\00]+")|(?:(?:(?:\\\s)|[^\s\00])+))(?:\s+((?:"[^\00]+")|(?:(?:(?:\\\s)|[^\s\00])+)))?
    private static final int UM_KEY_GROUP = 1;
    private static final int UM_ACCESS_GROUP = 2;
    private static final int UM_UID_GROUP = 3;
    private static final int UM_GID_GROUP = 4;
    private static final int UM_HOME_GROUP = 5;
    private static final int UM_ROOT_GROUP = 6;
    private static final int UM_FS_ROOT_GROUP = 7;

    private static long[] toLongs(String[] s)
    {
        long[] longs = new long[s.length];
        for (int i = 0; i < s.length; i++) {
            longs[i] = Long.parseLong(s[i]);
        }
        return longs;
    }

    private static String stripQuotes(String s)
    {
        if (s != null && s.startsWith("\"") && s.endsWith("\"") && s.length() > 1) {
            return s.substring(1, s.length() - 1);
        } else {
            return s;
        }
    }

    @Override
    public Map.Entry<StringPredicate,UserAuthzInformation> accept(String line) {
        line = line.trim();
        if (line.isEmpty() || line.startsWith("#") || line.startsWith("version 2.")) {
            return null;
        }

        Matcher matcher = USER_MAP_FILE_LINE_PATTERN.matcher(line);
        if (matcher.lookingAt()) {
            final String key = matcher.group(UM_KEY_GROUP);
            final String access = matcher.group(UM_ACCESS_GROUP);
            final String uid = matcher.group(UM_UID_GROUP);
            final long[] gids = toLongs(matcher.group(UM_GID_GROUP).split(","));
            final String home = stripQuotes(matcher.group(UM_HOME_GROUP));
            final String root = stripQuotes(matcher.group(UM_ROOT_GROUP));
            final String fsroot = stripQuotes(matcher.group(UM_FS_ROOT_GROUP));

            return new SimpleImmutableEntry<>(new StringPredicate(key), new UserAuthzInformation(key, access, Long.parseLong(uid), gids, home, root, fsroot));
        }
        _log.warn("Ignored malformed line in AuthzDB-File: '{}'", line);
        return null;
    }

    public static class StringPredicate implements MapPredicate<String> {

        private final String _string;

        public StringPredicate(String string) {
            _string = string;
        }

        @Override
        public boolean matches(String object) {
            return _string.equals(object);
        }
    }

    public static class UserAuthzInformation
    {
        private final String _username;
        private final String _access;
        private final long _uid;
        private final long[] _gids;
        private final String _home;
        private final String _root;
        private final String _fsroot;

        public UserAuthzInformation(String username, String access, long uid,
                long[] gids, String home, String root, String fsroot)
        {
            _username = username;
            _access = access;
            _uid = uid;
            _gids = gids;
            _home = home;
            _root = root;
            _fsroot = fsroot;
        }

        public String getUsername() {
            return _username;
        }

        public String getAccess() {
            return _access;
        }

        public boolean isReadOnly() {
            return !Objects.equal(_access, "read-write");
        }

        public long getUid() {
            return _uid;
        }

        public long[] getGids() {
            return _gids;
        }

        public String getHome() {
            return _home;
        }

        public String getRoot() {
            return _root;
        }

        public String getFsroot() {
            return _fsroot;
        }

        @Override
        public int hashCode() {
            return _username.hashCode() ^ _access.hashCode()
                   ^ Arrays.hashCode(_gids)
                   ^ _home.hashCode() ^ _root.hashCode() ^ _fsroot.hashCode()
                   ^ Objects.hashCode(_uid);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }

            if (other instanceof UserAuthzInformation) {
                UserAuthzInformation otherInfo = (UserAuthzInformation) other;

                return Objects.equal(_username, otherInfo._username)
                        && Objects.equal(_access, otherInfo._access)
                        && (_uid == otherInfo._uid)
                        && Arrays.equals(_gids, otherInfo._gids)
                        && Objects.equal(_home, otherInfo._home)
                        && Objects.equal(_root, otherInfo._root)
                        && Objects.equal(_fsroot, otherInfo._fsroot);
            }

            return false;
        }

        @Override
        public String toString()
        {
            return MoreObjects.toStringHelper(this)
                    .add("username", _username)
                    .add("access", _access)
                    .add("uid", _uid)
                    .add("gids", _gids)
                    .add("home", _home)
                    .add("root", _root)
                    .add("fsroot", _fsroot)
                    .toString();
        }
    }
}
