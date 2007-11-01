package org.dcache.pool.repository.v3;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class FlatDirectoryTree implements RepositoryTree {

	private final File _baseDir;
	private final File _controlDir;
	private final File _dataDir;

	public FlatDirectoryTree(File baseDir) throws IOException {

		_baseDir = baseDir;

		if( !_baseDir.exists() ) {
			throw new IOException (_baseDir + " does not exist.");
		}

		if( !_baseDir.isDirectory() ) {
			throw new IOException (_baseDir + " Not a directory.");
		}

		_controlDir = new File(_baseDir, "control");
		_dataDir    = new File(_baseDir, "data");

		if( !_controlDir.isDirectory() ) {
			throw new IOException (_controlDir + " does not exist or not a directory.");
		}

		if( !_dataDir.isDirectory() ) {
			throw new IOException (_dataDir + " does not exist or not a directory.");
		}

	}

	public File getControlFile(String name) {
		return new File(_controlDir, name);
	}

	public File getDataFile(String name) {
		return new File(_dataDir, name);
	}

	public File getSiFile(String name) {
		return new File(_controlDir, "SI-"+name);
	}

	public List<String> list() {
		return Arrays.asList(_dataDir.list());
	}

	/*
	 * will work with java6
	 */
	public long getFreeSpace() {
		return 0;// return _baseDir.getFreeSpace();
	}

	/*
	 * will work with java6
	 */
	public long getTotalSpace() {
		return 0;// return _baseDir.getTotalSpace();
	}

	public boolean destroy(String name) {

		/*
		 * the order is important to prevent control and/or SI files
		 * without data file.
		 */
		getControlFile(name).delete();
		getSiFile(name).delete();
		getDataFile(name).delete();


		return true;
	}

}
