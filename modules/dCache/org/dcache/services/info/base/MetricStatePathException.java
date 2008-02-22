package org.dcache.services.info.base;


/**
 * An exception thrown when a StatePath refers to an element that should be
 * a StateComponsite (i.e., a branch), but is a StateValue (a leaf).
 * 
 * @author Paul Millar <paul.millar@desy.de>
 */
public class MetricStatePathException extends BadStatePathException {

	static final long serialVersionUID = 1;

	public MetricStatePathException( String path) {
		
		StringBuffer sb = new StringBuffer();
		sb.append("path element is a metric instead of a branch: ");
		sb.append(path);
		
		updateMessage( sb.toString());
	}	
}
