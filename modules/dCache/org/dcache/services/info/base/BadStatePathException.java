package org.dcache.services.info.base;

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
	
	public String toString(){
		return _msg;
	}
}
