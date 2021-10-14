package org.dcache.auth.attributes;

import static java.util.Objects.requireNonNull;

import diskCacheV111.util.FsPath;
import java.io.Serializable;

/**
 * Immutable encapsulation of a user's root directory. Used as session data of a LoginReply.
 */
public class RootDirectory implements LoginAttribute, Serializable {

    private static final long serialVersionUID = 3092313442092927909L;

    private final String _root;

    public RootDirectory(String root) {
        requireNonNull(root);
        _root = root;
    }

    public RootDirectory(FsPath root) {
        this(root.toString());
    }

    public String getRoot() {
        return _root;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof RootDirectory)) {
            return false;
        }
        RootDirectory other = (RootDirectory) obj;
        return _root.equals(other._root);
    }

    @Override
    public int hashCode() {
        return _root.hashCode();
    }

    @Override
    public String toString() {
        return "RootDirectory[" + _root + ']';
    }
}
