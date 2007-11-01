package org.dcache.xrootd.util;

/**
 * 
 * This class encapsulates status information about a file.
 * It is compatible with the result of TSystem::GetPathInfo() as it is found 
 * in the ROOT framework. 
 * 
 * @author Martin Radicke
 *
 */
public class FileStatus {

	private String path, id, size, modtime;
	private int flags = 0;
	private StringBuffer info = null;
	private boolean isWrite;
	
	public FileStatus(String path) {
		this.path = path;
	}
	
	public FileStatus() {
		this("");
	}
	
//	set file handle
	public void setID(long id) {
		this.id = new Long(id).toString();
	}
	
	public void setSize(long size) {
		this.size = new Long(size).toString();
	}
	
	/**
	 * Set the flags for this file. 0 is regular file, bit 0 set executable,
     * bit 1 set directory, bit 2 set special file  (socket, fifo, pipe, etc.)
	 * 
	 * @param flags the flags to set to this file
	 */
	public void setFlags(int flags) {
		this.flags = flags;
	}
	
	public void setModtime(long modtime) {
		this.modtime = new Long(modtime).toString();
	}
	
	public String toString() {		
		if (info == null) {
			assembleInfoStringBuffer();
		}
		
		return info.toString();			
	}
	
	public int getInfoLength() {
		
		if (info == null)
			assembleInfoStringBuffer();
		
		return info.length();
	}
	
	public void setPath(String path) {
		this.path = path;
	}
	
	public String getPath() {
		return path;
	}
	
	public void setWrite(boolean b) {
		isWrite = b;
	}
	
	public boolean isWrite() {
		return isWrite;
	}
	
	private void assembleInfoStringBuffer() {
		
		info = new StringBuffer();
		
		info.append(id);
		info.append(" ");
		info.append(size);
		info.append(" ");
		info.append(flags);
		info.append(" ");
		info.append(modtime);
		info.append('\0');
	}
	
	public long getID() {
		return Long.parseLong(id);
	}
	
	public long getFileHandle() {
		return getID();
	}
}
