package org.dcache.services.info.base;

/**
 * Indicates that a StatePath refers to something that simply doesn't exist.
 * 
 * @author Paul Millar <paul.millar@desy.de>
 */
public class NoMetricStatePathException extends BadStatePathException {

	static final long serialVersionUID = 1;

	public NoMetricStatePathException( String path) {
		StringBuilder sb = new StringBuilder();

		sb.append("path does not exist: ");
		sb.append(path);

		updateMessage( sb.toString());
	}	
}
