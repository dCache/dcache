package org.dcache.services.info.base;

/**
 * A generic exception, indicating that there was some problem with the
 * StatePath.
 * 
 * @author Paul Millar <paul.millar@desy.de>
 */
public class BadStatePathException extends Exception {
	
	static final long serialVersionUID = 1;
	
	private static final String _defaultMsg = "Unknown error with a StatePath";

	private String _msg;
	
	public BadStatePathException() {
		_msg = _defaultMsg;
	}
	
	public BadStatePathException( String msg) {
		_msg = msg;
	}
	
	protected void updateMessage( String msg) {
		_msg = msg;
	}
	
	@Override
	public String toString(){
		return _msg;
	}
}
