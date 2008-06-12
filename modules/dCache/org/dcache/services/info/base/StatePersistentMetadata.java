package org.dcache.services.info.base;

import java.util.HashMap;
import java.util.Map;


/**
 * StatePersistentMetadataContainer is a hierarchical storage for information
 * that should persist independently from whether the corresponding StateComponent
 * object(s)exist, yet StateComponents may have a corresponding
 * StatePersistentMetadata.
 * <p>
 * The stored metadata is a simple keyword-value pairs, with both Strings.
 * 
 * @author Paul Millar <paul.millar@desy.de>
 */
public class StatePersistentMetadata {

	private final Map<String,StatePersistentMetadata> _children = new HashMap<String,StatePersistentMetadata>();
	private final Map<String,String> _payload = new HashMap<String,String>();
	private StatePersistentMetadata _wildcard = null;
	
	protected StatePersistentMetadata() {} // Reduce visibility of our constructor
		
	
	/**
	 * Look up a child reference given by the String.  If one doesn't exist, a new child
	 * object is created and this is returned.  The special wildcard value "*" is treated
	 * differently. 
	 * @param reference the label for this child object
	 * @return a child StatePersistentMetadataContainer object
	 */
	private StatePersistentMetadata getOrCreateChild( String reference) {
		
		/**
		 *  Deal with the wildcard special case.
		 */
		if( reference.equals("*")) {
			if( _wildcard == null)
				_wildcard = new StatePersistentMetadata();
			return _wildcard;
		}
		
		/**
		 * More generally, look up in our hash-table.
		 */
		
		StatePersistentMetadata child = _children.get( reference);
		
		if( child == null) {
			child = new StatePersistentMetadata();
			_children.put(reference, child);
		}
		
		return child;
	}
	
	/**
	 * Look for the best-match child StatePersistentMetadata object for the named
	 * child.  Will return null if not suitable reference is available.
	 * @param name the child object in the State hierarchy
	 * @return child persistent metadata object, if one is available, or null.
	 */
	protected StatePersistentMetadata getChild( String name) {
		StatePersistentMetadata child = _children.get( name);
		
		if( child == null)
			child = _wildcard;

		return child;
	}
	
	/**
	 * Add some metadata at a particular point in the hierarchy.  Extra nodes are
	 * created as necessary.
	 * @param path the path to the node that should be updated.
	 * @param update the set of updates to administer.
	 */
	void add( StatePath path, Map<String,String> update) {
		
		/** Catch bad input */
		if( update == null)
			return;
		
		/** If we still have more path to traverse, do so */
		if( path != null) {
			String pathElement = path.getFirstElement();
			getOrCreateChild( pathElement).add( path.childPath(), update);
			return;
		}
	
		/** Update our metadata */
		_payload.putAll(update);
	}
	

	/**
	 * Return this node's payload: a Map of metadata information.
	 * @return
	 */
	Map<String,String> getMetadata() {
		return _payload;
	}
	
	
	/**
	 *  Add a default (hard-coded) set of persistent metadata.
	 */
	protected void addDefault() {
		this.add( StatePath.parsePath("pools.*"), branchMetadata( "pool", "name"));
		this.add( StatePath.parsePath("pools.*.poolgroups.*"), branchMetadata( "poolgroupref", "name"));
		this.add( StatePath.parsePath("pools.*.queues.*"), branchMetadata( "queue", "type"));
		this.add( StatePath.parsePath("pools.*.queues.named-queues.*"), branchMetadata( "queue", "name"));
		
		this.add( StatePath.parsePath("poolgroups.*"), branchMetadata( "poolgroup", "name"));
		this.add( StatePath.parsePath("poolgroups.*.links.*"), branchMetadata( "linkref", "name"));
		this.add( StatePath.parsePath("poolgroups.*.pools.*"), branchMetadata( "poolref", "name"));

		this.add( StatePath.parsePath("links.*"), branchMetadata( "link", "name"));
		this.add( StatePath.parsePath("links.*.poolgroups.*"), branchMetadata( "poolgroupref", "name"));
		this.add( StatePath.parsePath("links.*.pools.*"), branchMetadata( "poolref", "name"));
		this.add( StatePath.parsePath("links.*.unitgroups.*"), branchMetadata( "unitgroupref", "name"));
		this.add( StatePath.parsePath("links.*.units.protocol.*"), branchMetadata( "unitref", "name"));
		this.add( StatePath.parsePath("links.*.units.net.*"), branchMetadata( "unitref", "name"));
		this.add( StatePath.parsePath("links.*.units.store.*"), branchMetadata( "unitref", "name"));
		this.add( StatePath.parsePath("links.*.units.dcache.*"), branchMetadata( "unitref", "name"));
		
		this.add( StatePath.parsePath("linkgroups.*"), branchMetadata("linkgroup", "lgid"));
		this.add( StatePath.parsePath("linkgroups.*.authorisation.*"), branchMetadata("authorised", "name"));
		
		this.add( StatePath.parsePath("srm.spaces.*"), branchMetadata("space", "space-id"));
		
		this.add( StatePath.parsePath("units.*"), branchMetadata( "unit", "name"));
		this.add( StatePath.parsePath("units.*.unitgroups.*"), branchMetadata( "unitgroupref", "ref"));

		this.add( StatePath.parsePath("unitgroups.*"), branchMetadata( "unitgroup", "name"));
		this.add( StatePath.parsePath("unitgroups.*.units.*"), branchMetadata( "unitref", "name"));
		this.add( StatePath.parsePath("unitgroups.*.links.*"), branchMetadata( "linkref", "name"));

		this.add( StatePath.parsePath("doors.*"), branchMetadata( "door", "name"));
		this.add( StatePath.parsePath("doors.*.interfaces.*"), branchMetadata( "interface", "name"));

		this.add( StatePath.parsePath("domains.*"), branchMetadata( "domain", "name"));
		this.add( StatePath.parsePath("domains.*.cells.*"), branchMetadata( "cell", "name"));
	}
	
	
	/**
	 * Prepare a Map that updates the persistent metadata hierarchy to include
	 * a Class and IdName values, these values are indexed by
	 * <tt>METADATA_BRANCH_CLASS_KEY</tt> and <tt>METADATA_BRANCH_IDNAME_KEY</tt>
	 * respectively.
	 * @param branchClass the name of this list's classification.
	 * @param branchIdName how to refer to the general class of individual item's unique id.
	 * @return a new Map entry, ready to be passed to StatePersistentMetadata.add().
	 */
	private Map<String,String> branchMetadata( String branchClass, String branchIdName) {
		Map<String,String> metadataUpdate = new HashMap<String,String>();
		
		metadataUpdate.put( State.METADATA_BRANCH_CLASS_KEY, branchClass);
		metadataUpdate.put( State.METADATA_BRANCH_IDNAME_KEY, branchIdName);
		
		return metadataUpdate;
	}		

}
