package org.dcache.pool.repository.v3;

public class RepositoryMissmatchException extends RepositoryException {

	public RepositoryMissmatchException(String msg) {
		super(msg);
	}

	public RepositoryMissmatchException(int rc, String msg) {
		super(rc, msg);
	}
	/**
	 * 
	 */
	private static final long serialVersionUID = 1559639977210589429L;

}
