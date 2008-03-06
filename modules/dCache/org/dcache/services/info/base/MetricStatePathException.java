package org.dcache.services.info.base;


/**
 * An exception thrown when a StatePath refers to an element that should be
 * a StateComponsite (i.e., a branch), but is a StateValue (a leaf).
 * 
 * @author Paul Millar <paul.millar@desy.de>
 */
public class MetricStatePathException extends BadStatePathException {

	static private final String DEFAULT_PREFIX = "path element is a metric instead of a branch: ";  

	static final long serialVersionUID = 1;
	

	public MetricStatePathException( String path) {
		StringBuffer sb = new StringBuffer();
		sb.append( DEFAULT_PREFIX);
		sb.append(path);
		
		updateMessage( sb.toString());
	}
	
	
	/**
	 * Create a new MetricStatePathException when the child (childName) is a metric-node
	 * instead of a branch-node.
	 * @param pathToComposite the StatePath to this StateComposite
	 * @param childName the name of the child that should have been a StateComposite.
	 */
	public MetricStatePathException( StatePath pathToComposite, String childName) {
		StringBuffer sb = new StringBuffer();
		sb.append( DEFAULT_PREFIX);
		sb.append( pathToComposite.newChild(childName).toString());
		
		updateMessage( sb.toString());		
	}
}
