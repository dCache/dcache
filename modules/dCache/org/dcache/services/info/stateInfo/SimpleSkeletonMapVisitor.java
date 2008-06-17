package org.dcache.services.info.stateInfo;

import java.util.Map;

import org.dcache.services.info.base.BooleanStateValue;
import org.dcache.services.info.base.FloatingPointStateValue;
import org.dcache.services.info.base.IntegerStateValue;
import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.base.StateVisitor;
import org.dcache.services.info.base.StringStateValue;

/**
 * The SimpleSkeletonMapVisitor provides a simple framework for building a map between
 * a list of items and a particular metric within that list.  Although not abstract, this
 * Class should not be instantiated as it won't do anything useful!  Instead, a Class
 * should extend this class for each possible type of StateValue (the "metric").  This
 * is to allow the metric type to be reflected in the Map declaration.
 * <p>
 * To achieve this, the super-Class should override one of the visit*() methods, use
 * _pathToMetric to test whether the metric is to be selected and _thisKey as the key
 * in the corresponding Map. 
 * <p>
 * The super-Class should also implement the getMap() method that should return a 
 * Map of the correct type for the StateValue super-Class (metric type) under
 * consideration.
 * 
 * @author Paul Millar <paul.millar@desy.de>
 */
public class SimpleSkeletonMapVisitor implements StateVisitor {

	final private StatePath _pathToList;
	final private StatePath _relativePathToMetric;
	
	/** The key of the current branch */
	protected String _thisKey;
	protected StatePath _pathToMetric;

	/**
	 * Build a Map between a list of items and a metric value within that list.  To do this, two
	 * paths must be specified.  The first path locates the parent StateComposite (branch) that
	 * the list items share as their common parent: the keys in the resulting Map will be the names
	 * of the StateComposites that are immediate children of the StateComposite given by the first path.
	 * <p>
	 * The second path gives the relative path from a child of the parent StateComposite to the metric under
	 * consideration.  If this is missing, no mapping is created.
	 * <p>
	 * For example, if the tree has String metrics like:
	 * <pre>
	 *   aa.bb.item1.cc.dd.value1
	 *   aa.bb.item2.cc.dd.value2
	 *   aa.bb.item3.cc.dd.value3
	 * </pre>
	 * Then supplying the paths aa.bb and cc.dd will result in a mapping with the following
	 * Map.Entries:
	 * <pre>  item1 --> value1
	 *  item2 --> value2
	 *  item3 --> value3
	 * </pre>
	 * Classes that extend this Class should implement a <code>getMap()</code> method.  This should
	 * return a mapping between the key and the value and have the correct type.
     */
	public SimpleSkeletonMapVisitor( StatePath pathToList, StatePath pathToMetric) {
		_pathToList = pathToList;
		_relativePathToMetric = pathToMetric;
	}
	
	/**
	 * The super-Class should override one of the following four methods
	 */
	public void visitBoolean(StatePath path, BooleanStateValue value) {}
	public void visitFloatingPoint(StatePath path, FloatingPointStateValue value) {}
	public void visitInteger(StatePath path, IntegerStateValue value) {}
	public void visitString(StatePath path, StringStateValue value) {}

	public void visitCompositePostDescend(StatePath path, Map<String, String> metadata) {}
	public void visitCompositePreSkipDescend(StatePath path, Map<String, String> metadata) {}
	public void visitCompositePostSkipDescend(StatePath path, Map<String, String> metadata) {}
	public void visitCompositePreLastDescend(StatePath path, Map<String, String> metadata) {}

	public void visitCompositePreDescend(StatePath path, Map<String, String> metadata) {
		if( _pathToList.isParentOf( path)) {
			_thisKey = path.getLastElement();
			_pathToMetric = path.newChild( _relativePathToMetric);
		}
	}	
}
