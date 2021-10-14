package org.dcache.auth.attributes;

import static java.util.Objects.requireNonNull;

import diskCacheV111.util.FsPath;
import java.io.Serializable;

/**
 * Encapsulation of a user's home. Used as session data of a LoginReply.
 */
public class HomeDirectory implements LoginAttribute, Serializable {

    private static final long serialVersionUID = -1502727254247340036L;

    private final String _home;

    public HomeDirectory(String home) {
        requireNonNull(home);
        _home = home;
    }

    public HomeDirectory(FsPath home) {
        this(home.toString());
    }

    public String getHome() {
        return _home;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof HomeDirectory)) {
            return false;
        }
        HomeDirectory other = (HomeDirectory) obj;
        return _home.equals(other._home);
    }

    @Override
    public int hashCode() {
        return _home.hashCode();
    }

    @Override
    public String toString() {
        return "HomeDirectory[" + _home + ']';
    }
}
