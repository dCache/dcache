package org.dcache.services.info.base;

import java.util.*;

/**
 * A StateComposite is an aggregation of zero or more StateComponents.  StateComposites
 * form the branch nodes within the dCache state tree.
 * <p>
 * A StateComposite has a minimum lifetime when created, which may be infinite.  If finite,
 * the expiry date will be adjusted to match any added children: the branch will always 
 * persist whilst it contains any children.
 * 
 * @author Paul Millar <paul.millar@desy.de>
 */
public class StateComposite implements StateComponent {

	/** Minimum lifetime for on-the-fly created StateComposites */  
	private static final long DEFAULT_LIFETIME = 10;
	
	private Map<String, StateComponent> _children = new HashMap<String, StateComponent>();
	private StatePersistentMetadata _metadataRef;
	private Date _earliestChildExpiry = null;
	private Date _whenIShouldExpire;
	
	/**
	 * The constructor for public use: a StateComposite with a finite lifetime.
	 * 
	 * @param lifetime the minimum duration, in seconds, that this StateComposite
	 * should persist.
	 */
	public StateComposite( long lifetime) {
		
		/**
		 * We rely on the add() method of parent Composite to update set the _metadataRef
		 * correctly.
		 */
		_metadataRef = null;
		buildSelfExpiry( lifetime);
	}

	/**
	 * Our private usage below: build a new StateComposite with a link to persistentMetadata.
	 * 
	 * @param ref the corresponding StatePersistentMetadata object.
	 * @param lifetime the minimum lifetime of this object, in seconds.
	 */
	private StateComposite( StatePersistentMetadata persistentMetadata, long lifetime) {
		_metadataRef = persistentMetadata;
		buildSelfExpiry( lifetime);
	}
	
	/**
	 * Build an immortal composite with specific metadata link.  This should only be used
	 * by the State singleton. 
	 * @param persistentMetadata the top-level metadata.
	 */
	protected StateComposite( StatePersistentMetadata persistentMetadata) {
		_metadataRef = persistentMetadata;		
		_whenIShouldExpire = null;
	}

	/**
	 * Initialise our expiry time.
	 * @param lifetime the time, in seconds, that this object should guarantee to persist.
	 */
	private void buildSelfExpiry( long lifetime) {
		_whenIShouldExpire = new Date( System.currentTimeMillis() + lifetime * 1000);		
	}

	
	/**
	 * Return a meaningful string for this branch
	 */
	public String toString() {
		StringBuffer sb_list = new StringBuffer();

		for (Iterator<String> itr = _children.keySet().iterator(); itr
				.hasNext();) {
			if (sb_list.length() > 0)
				sb_list.append(", ");
			sb_list.append(itr.next());
		}

		StringBuffer sb = new StringBuffer();
		sb.append("{");
		sb.append(sb_list);
		sb.append("}");

		return sb.toString();
	}

	/**
	 * When we should expire.
	 */
	public Date getExpiryDate() {
		return _whenIShouldExpire;
	}
	
	/**
	 * @return the time when the earliest child will expire, or null if no child is expiring.
	 */
	protected Date getEarliestChildExpiryDate() {
		return _earliestChildExpiry;
	}
	
	/**
	 *  This function checks whether our parent should expung us.  We also use it to expung child
	 *  entries.
	 */
	public boolean hasExpired() {
		Date now = new Date();
		
		if( _earliestChildExpiry != null && now.after( _earliestChildExpiry)) {
			Date newEarliestExp = null;

			for (Iterator<StateComponent> itr = _children.values().iterator(); itr.hasNext();) {
				StateComponent child = itr.next();

				if( child.hasExpired())  // trigger cascade...
					itr.remove();
				else {
					Date childExpDate = child.getExpiryDate();
					
					if( childExpDate == null)
						continue;
					
					if( newEarliestExp == null || childExpDate.before( newEarliestExp))
						newEarliestExp = childExpDate;
				}
			}
			
			_earliestChildExpiry = newEarliestExp;
		}
		
		return _whenIShouldExpire != null ? now.after(_whenIShouldExpire) : false;
	}


	/**
	 * Apply the visitor pattern over our children.
	 * <p>
	 * Interesting aspects:
	 * <ul>
	 * <li> Visiting over all child only happens once the <tt>start</tt> path has been
	 * exhausted
	 * <li> There are five StateVisitor call-backs from this StateComponent
	 * </ul>
	 * 
	 * The standard callbacks are:
	 * <ul>
	 * <li>visitCompositePreDescend() called before visiting children.
	 * <li>visitCompositePreLastDescend() called before visiting the last child.
	 * <li>visitCompositePostDescend() called after visiting children.
	 * </ul>
	 * 
	 * The <tt>start</tt> path allows the client to specify a point within the State tree
	 * to start visiting.  Iterations down to that level call a different set of Visitor
	 * call-backs: visitCompositePreSkipDescend() and visitCompositePostSkipDescend().
	 * These are equivalent to the non-<tt>Skip</tt> versions and allow the StateVisitor
	 * to represent the skipping down to the starting point, or not.
	 *
	 * @param path the path to the current position in the State.
	 * @param start the residual path to skip.
	 * @param visitor the object that implements the StateVisitor class.
	 */
	public void acceptVisitor(StatePath path, StatePath start, StateVisitor visitor) {
		
		Map<String,String> childMetadata = null;

		/** If start is not yet null, iterate down directly */
		if( start != null) {
			String childName = start.getFirstElement();
			StateComponent child = _children.get( childName);
			if( child != null) {
				if( _metadataRef != null)
					childMetadata = _metadataRef.getMetadata();

				visitor.visitCompositePreSkipDescend(path, childMetadata);
				child.acceptVisitor(path.newChild(childName), start.childPath(), visitor);
				visitor.visitCompositePostSkipDescend(path, childMetadata);
			} else {
				
				/**
				 * No data is available at this level.  This is because the
				 * client has specified a path that (currently) doesn't exist.
				 * The path may be "correct" and will be filled with data "soon",
				 * or may be "incorrect" and data will never appear here.
				 * <p>
				 * Since we cannot distinguish between the two, we silently fail. 
				 */
			}
			return;
		}
		
		if( _metadataRef != null)
			childMetadata = _metadataRef.getMetadata();

		visitor.visitCompositePreDescend(path, childMetadata);

		for (Iterator<Map.Entry<String, StateComponent>> itr = _children
				.entrySet().iterator(); itr.hasNext();) {

			Map.Entry<String, StateComponent> entry = itr.next();

			StatePath childPath = path.newChild(entry.getKey());
			StateComponent sc = entry.getValue();

			if (!itr.hasNext())
				visitor.visitCompositePreLastDescend(path, childMetadata);

			sc.acceptVisitor(childPath, null, visitor);
		}

		visitor.visitCompositePostDescend(path, childMetadata);
	}
	
	

	/**
	 * Attempt to add a new StateComponent underneath this object.  Branches
	 * (StateComposites) will be generated on-the-fly to satisfy this.  However,
	 * if part of the StatePath points to a metric, then the value cannot be
	 * added and a BadStatePathException will be thrown.
	 * @param path the path under which the StateComponent should be stored.
	 * @param newChild the StateComponent that should be stored.
	 * @exception BadStatePathException the path is impossible to satisfy.
	 */
	public void add(StatePath path, StateComponent newChild) throws BadStatePathException {

		Date expiryDate = newChild.getExpiryDate();

		// Ensure that we don't expire before any of our children (or children's children, or...)
		if( _whenIShouldExpire != null && expiryDate != null && expiryDate.after( _whenIShouldExpire))
			_whenIShouldExpire = expiryDate;
		
		// Update our view of our children (and children's children, and ...)
		Date newChildExpDate = newChild.getExpiryDate();		
		if( _earliestChildExpiry == null || newChildExpDate.before( _earliestChildExpiry))
			_earliestChildExpiry = newChildExpDate; 

		String ourReference = path.getFirstElement();

		if (path.isSimplePath())
			addComponent( ourReference, newChild);	 // We are last StateComposite, so add directly
		else {
			StateComponent child = getOrCreateChild( ourReference);
			child.add(path.childPath(), newChild);	// Iterate down to next level.
		}
		
	}
	
	
	/**
	 * Look up persistent metadata reference for child and return it.  If none is
	 * available, null is returned.
	 * @param childName the name of the child.
	 * @return a StatePersistentMetadata entry, or null if none is appropriate.
	 */
	private StatePersistentMetadata getChildMetadata( String childName) {
		StatePersistentMetadata childMetadata = null;
		
		if( _metadataRef != null)
			childMetadata = _metadataRef.getChild( childName);
		
		return childMetadata;
	}

	

	/**
	 * Add a new component to our list of children.
	 * <p>
	 * If the new StateComponent is a StateComposite, then it <i>must</i> contain no
	 * children. 
	 * <p>
	 * @param name the name under which this item should be recorded
	 * @param newChild the StateComponent to be stored.
	 */
	private void addComponent(String name, StateComponent newChild) {

		StateComponent existingChild = _children.get( name);

		/**
		 * If we're added a StateComposite, we must be more careful!
		 */
		if( newChild instanceof StateComposite) {
			StateComposite newComposite = (StateComposite) newChild;

			// make sure it's metadataRef is up-to-date.  NB this assumes newStateComp has no children.
			newComposite._metadataRef = getChildMetadata( name);
			
			/**
			 *   If a child with this name already exists (and is a StateComposite)
			 *   merge existing element into new element and inherit from the existing 
			 *   _earliestChildExpiry value.
			 */			
			if( existingChild != null && existingChild instanceof StateComposite) {
				StateComposite existingComposite = (StateComposite) existingChild;
				
				// Copy across children
				for( Map.Entry<String,StateComponent> entry : existingComposite._children.entrySet())						
					if( !newComposite._children.containsKey(entry.getKey()))
						newComposite._children.put(entry.getKey(), entry.getValue());
				
				// ... and details of their earliest expiry date
				newComposite._earliestChildExpiry = existingComposite._earliestChildExpiry;
			}			
		}

		/**
		 *  If our new child name colides with an existing child who's expiry date matches
		 *  the earliestChildExpiry Date, trigger a rescan of the earliestChildExp.
		 */
		boolean shouldReScan = false;
		if( existingChild != null) {
			Date newChildExpDate = newChild.getExpiryDate();
			shouldReScan = newChildExpDate != null && newChildExpDate.equals( _earliestChildExpiry);		
		}
		
		_children.put(name, newChild);
		
		if( shouldReScan) {
			_earliestChildExpiry = null;
			
			for( StateComponent thisChild : _children.values()) {				
				Date thisChildExpDate = thisChild.getExpiryDate();

				if( thisChildExpDate == null)
					continue;
				
				if( _earliestChildExpiry == null || thisChildExpDate.before(_earliestChildExpiry))
					_earliestChildExpiry = thisChildExpDate;
			}
		}
	}

	
	
	/**
	 * Search for a child entry.  If one doesn't exist, create a new StateComposite with the
	 * supplied name.
	 * @param name the name of the StateComponent
	 * @param expTime the minimum number of seconds any created StateComponent should exist.
	 * @return a corresponding StateComponent.
	 */
	private StateComponent getOrCreateChild(String name) {

		StateComponent child = _children.get( name);
		
		if( child == null) {
			/**
			 *  NB DEFAULT_LIFETIME is largely irrelevant: add()ing new StateComponents will
			 *  extend this new StateComposite's expiry date.
			 */
			child = new StateComposite( getChildMetadata( name), DEFAULT_LIFETIME);
			_children.put( name, child);
		}
		
		return _children.get(name);
	}
}
