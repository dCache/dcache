package org.dcache.pool.repository.v3;

import diskCacheV111.util.CacheException;

public class RepositoryException extends CacheException {

	public RepositoryException(int rc, String msg) {
		super(rc, msg);
	}

	public RepositoryException(String msg) {
		super(msg);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = -3613396690222652485L;

}
