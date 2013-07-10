package org.dcache.pool.repository.v3;

import diskCacheV111.util.CacheException;

public class RepositoryException extends CacheException
{
    private static final long serialVersionUID = -3613396690222652485L;

    public RepositoryException(int rc, String msg, Throwable cause) {
        super(rc, msg, cause);
    }

    public RepositoryException(int rc, String msg) {
        super(rc, msg);
    }

    public RepositoryException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public RepositoryException(String msg) {
        super(msg);
    }
}
