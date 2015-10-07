package org.dcache.ftp.client;

import java.io.IOException;

/**
 * The MlsxEntryWriter provides a callback interface for writing 
 * individual MlsxEntry items from a long directory listing (for
 * example, using the MLSR command).
 *
 */
public interface MlsxEntryWriter {
	/**
	 * Writes a single entry from the stream.  
	 * 
	 * @return true to continue, false to stop writer
	 * @param entry the file/directory entry
	 */
	public void write(MlsxEntry entry) throws IOException;
	
	/**
	 * Notifies the writer that the stream of entries has ended.
	 *
	 */
	public void close();
}
