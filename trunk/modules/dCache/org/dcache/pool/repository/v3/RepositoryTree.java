package org.dcache.pool.repository.v3;

import java.io.File;
import java.util.List;

/**
 *
 * @since 1.7.1
 *
 */
public interface RepositoryTree {


	/**
	 * @param name - file id (pnfsid)
	 * @return path to data file
	 */
	public File getDataFile(String name);

	/**
	 * @param name - file id (pnfsid)
	 * @return path to control file
	 */
	public File getControlFile(String name);

	/**
	 * @param name - file id (pnfsid)
	 * @return path to sorageInfo file
	 */
	public File getSiFile(String name);

	/**
	 * remove data, control and storageInfo files for the record
	 * @param name - file id (pnfsid)
	 * @return true if entry destoyed
	 */
	public boolean destroy(String name);

	/**
	 *
	 * @return list of all records ( pnfsid's)
	 */
	public List<String> list();

	/*
	 * with java6 we will have more functionality here
	 */
	public long getFreeSpace();
	public long getTotalSpace();

}
