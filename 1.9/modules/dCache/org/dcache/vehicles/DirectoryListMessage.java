package org.dcache.vehicles;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PnfsMessage;

public class DirectoryListMessage extends PnfsMessage {
	

	/**
	 * 
	 */
	private static final long serialVersionUID = 0L;

	/**
	 * set by server to indicate that list is complete
	 */
	private boolean _eof = false; 

	/**
	 * the starting point in the list. zero means 
	 * starting from beginning
	 */
	private long _cookie = 0;
	
	/**
	 * maximal number of entries to be returned by server.
	 * -1 indicated unlimited
	 */
	private int _maxcount = -1;
	
	/**
	 * maximal number of bytes returned by server excluding message overhead.
	 * -1 indicated unlimited
	 */
	private int _maxbyte = -1;
	
	/**
	 * list of directory, starting  from requested cookie.
	 */
	private String[] _list = null;
	
	
	/**
	 * construct a new directory lookup request with default values
	 * @param dir pnfs id of directory to list
	 */
	public DirectoryListMessage(PnfsId dir) {
		this(dir, 0, -1, -1);
	}
	
	/**
	 * construct a new directory lookup request
	 * 
	 * @param dir pnfs id of directory to list
	 * @param cookie the list starting point
	 * @param maxcount maximal number of entries to return count
	 * @param maxbutes maximal bytes to return excluding message overhead
	 */
	public DirectoryListMessage(PnfsId dir, long cookei, int maxcount, int maxbytes ) {
		super(dir);
		super.setReplyRequired(true);
		_cookie = cookei;
		_maxcount = maxcount;
		_maxbyte = maxbytes;
	}
	
	/**
	 * maximal number of bytes returned by server excluding message overhead.
	 * -1 indicated unlimited.
	 * @return maxcount
	 */		
	public int maxCount() {
		return _maxcount;
	}
 	
	/**
	 * maximal number of bytes returned by server excluding message overhead.
	 * -1 indicated unlimited
	 * @return maxbyte
	 */	
	public int maxBytes() {
		return _maxbyte;
	}
	
	/**
	 * true if list is compete stating from defined cookie
	 * @return eof
	 */
	public boolean eof() {
		return _eof;
	}
	
	/**
	 * set by client indicates beginning of lookup operation.
	 * set by server indicated current 'position' at server.
	 * @return current cookie
	 */
	public long cookie() {
		return _cookie;
	}
	
	/**
	 * return directory listing starting from <strong>cookie</strong>. The <strong>eof</strong> set to true if
	 * listing is complete. The <strong>cookie</strong> is changed to actual value. The LookupPool is free to 
	 * return less entries or less bytes than requested.
	 * 
	 * @return An array of strings naming the files and directories in the directory denoted by this pnfsid.
     *         The array will be empty if the directory is empty or if no names were accepted by the filter.
     *         Returns null if this pnfsid does not denote a directory, or if an I/O error occurs.
	 */
	public String[] list() {
		return _list;
	}
	
	/**
	 * set new cookie
	 * @param newCookie
	 */
	public void cookie(long newCookie) {
		_cookie = newCookie;
	}
	
	/**
	 * set end of list indication
	 * @param eof
	 */
	public void eof(boolean eof) {
		_eof = eof;
	}
	
	/**
	 * 
	 * @param newList
	 */
	public void list(String[] newList) {
		_list = newList;
	}
}
