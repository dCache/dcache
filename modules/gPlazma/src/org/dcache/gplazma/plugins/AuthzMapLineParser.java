package org.dcache.gplazma.plugins;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

/**
 * @author karsten
 *
 */
class AuthzMapLineParser implements LineParser<AuthzMapLineParser.StringPredicate, AuthzMapLineParser.UserAuthzInformation> {

    private static final Logger _log = LoggerFactory.getLogger(GPlazmaVORolePlugin.class);

    private static final String SOME_WS = "\\s+";
    private static final String AUTHORIZE = "[Aa][Uu][Tt][Hh][Oo][Rr][Ii][Zz][Ee]";
    private static final String USERNAME = "[\\w\\d]+";
    private static final String ACCESS = "(?:[Rr][Ee][Aa][Dd]-[Oo][Nn][Ll][Yy])|(?:[Rr][Ee][Aa][Dd]-[Ww][Rr][Ii][Tt][Ee])";
    private static final String UID = "\\d+";
    private static final String GID = UID;
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

    private static String stripQuotes(String s)
    {
        if (s.startsWith("\"") && s.endsWith("\"") && s.length() > 1) {
            return s.substring(1, s.length() - 1);
        } else {
            return s;
        }
    }

    @Override
    public AuthzMapEntry accept(String line) {
        line = line.trim();
        if (Strings.isNullOrEmpty(line)
                || line.startsWith("#")
                || line.startsWith("version 2."))
            return null;

        Matcher matcher = USER_MAP_FILE_LINE_PATTERN.matcher(line);
        if (matcher.lookingAt()) {
            final String key = matcher.group(UM_KEY_GROUP);
            final String access = matcher.group(UM_ACCESS_GROUP);
            final String uid = matcher.group(UM_UID_GROUP);
            final String gid = matcher.group(UM_GID_GROUP);
            final String home = stripQuotes(matcher.group(UM_HOME_GROUP));
            final String root = stripQuotes(matcher.group(UM_ROOT_GROUP));
            final String fsroot = stripQuotes(matcher.group(UM_FS_ROOT_GROUP));

            return new AuthzMapEntry(new StringPredicate(key), new UserAuthzInformation( key, access, Integer.parseInt(uid), Integer.parseInt(gid), home, root, fsroot ) );
        }
        _log.warn("Ignored malformed line in AuthzDB-File: '{}'", line);
        return null;
    }

    class StringPredicate implements MapPredicate<String> {

        private final String _string;

        public StringPredicate(String string) {
            _string = string;
        }

        @Override
        public boolean matches(String object) {
            return _string.equals(object);
        }
    }

    class UserAuthzInformation {
        private final String _username;
        private final String _access;
        private final int _uid;
        private final int _gid;
        private final String _home;
        private final String _root;
        private final String _fsroot;

        public UserAuthzInformation(String username, String access, int uid,
                int gid, String home, String root, String fsroot) {
            super();
            this._username = username;
            this._access = access;
            this._uid = uid;
            this._gid = gid;
            this._home = home;
            this._root = root;
            this._fsroot = fsroot;
        }

        public String getUsername() {
            return _username;
        }

        public String getAccess() {
            return _access;
        }

        public int getUid() {
            return _uid;
        }

        public int getGid() {
            return _gid;
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
        public boolean equals(Object other) {
            if (this == other) return true;
            if (!(other.getClass().equals(UserAuthzInformation.class))) return false;
            UserAuthzInformation otherInfo = (UserAuthzInformation)other;

            return ((_username == null) || (otherInfo._username == null) || (_username.equals(otherInfo._username))) &&
                   ((_access == null) || (otherInfo._access == null) || (_access.equals(otherInfo._access))) &&
                   ((_uid < 0) || (otherInfo._uid < 0) || _uid == otherInfo._uid) &&
                   ((_gid < 0) || (otherInfo._gid < 0) || _gid == otherInfo._gid) &&
                   ((_home == null) || (otherInfo._home == null) || _home.equals(otherInfo._home)) &&
                   ((_root == null) || (otherInfo._root == null) || _root.equals(otherInfo._root)) &&
                   ((_fsroot == null) || (otherInfo._fsroot == null) || _fsroot.equals(otherInfo._fsroot));
        }
    }

    private class AuthzMapEntry implements Map.Entry<StringPredicate, UserAuthzInformation> {

        private final StringPredicate _key;
        private UserAuthzInformation _value;

        public AuthzMapEntry(StringPredicate key, UserAuthzInformation value) {
            _key = key;
            _value = value;
        }

        @Override
        public StringPredicate getKey() {
            return _key;
        }

        @Override
        public UserAuthzInformation getValue() {
            return _value;
        }

        @Override
        public UserAuthzInformation setValue(UserAuthzInformation value) {
            return (_value = value);
        }

    }

}
