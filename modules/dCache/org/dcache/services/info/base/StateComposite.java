package org.dcache.services.info.base;

import java.util.*;

import org.apache.log4j.Logger;

/**
 * A StateComposite is an aggregation of zero or more StateComponents.  StateComposites
 * form the branch nodes within the dCache state tree.
 * <p>
 * A Mortal StateComposite has a minimum lifetime when created.  The expiry date will be
 * adjusted to match any added Mortal children: the branch will always persist whilst it
 * contains any children.
 * <p>
 * An Ephemeral StateComposite has no lifetime: it will persist without having fixed
 * lifetime.  However an Ephemeral StateComposite will not prevent an ancestor StateComposite
 * that is Mortal from expiring.  In general, a StateComposite that contains <i>only</i>
 * Ephemeral children should also be Ephemeral; all other StateComposites should be Mortal.  
 * <p>
 * A StateComposite also maintains a record of the earliest any of its children (or children of
 * children) will expire.  This is an optimisation, allowing a quick determination when
 * a tree should next be purged and, with any subtree, whether it is necessary to purge that
 * subtree.
 * 
 * @author Paul Millar <paul.millar@desy.de>
 */
public class StateComposite implements StateComponent {

	private static final Logger _log = Logger.getLogger(StateComposite.class);
	
	private static final int MILLIS_IN_SECOND = 1000;

	
	/** Minimum lifetime for on-the-fly created StateComposites */  
	private static final long DEFAULT_LIFETIME = 10;
	
	private final Map<String, StateComponent> _children = new HashMap<String, StateComponent>();
	private StatePersistentMetadata _metadataRef;
	private Date _earliestChildExpiry = null;
	private Date _whenIShouldExpire;
	private boolean _isEphemeral;
	
	/**
	 * The constructor for public use: a StateComposite with a finite lifetime.
	 * 
	 * @param lifetime the minimum duration, in seconds, that this StateComposite
	 * should persist.
	 */
	public StateComposite( long lifetime) {
		becomeMortal( lifetime);
		_metadataRef = null;  // Set when added to state tree
	}
	
	
	/**
	 * Create an Ephemerial StateComposite.  These should <i>only</i> be used
	 * when they are to contain only Ephemerial children.  Normally StateComposites
	 * should be created Mortal.
	 */
	public StateComposite() {
		becomeEphemeral();
		_metadataRef = null; // Set when added to state tree
	}

	/**
	 * Our private usage below: build a new Mortal StateComposite with a
	 * link to persistentMetadata.
	 * 
	 * @param ref the corresponding StatePersistentMetadata object.
	 * @param lifetime the minimum lifetime of this object, in seconds.
	 */
	private StateComposite( StatePersistentMetadata persistentMetadata, long lifetime) {
		becomeMortal( lifetime);
		_metadataRef = persistentMetadata;
	}
	
	/**
	 * Build an Immortal StateComposite with specific metadata link.
	 * This should only be used by the State singleton. 
	 * @param persistentMetadata the top-level metadata.
	 */
	protected StateComposite( StatePersistentMetadata persistentMetadata) {
		becomeImmortal();
		_metadataRef = persistentMetadata;		
	}

	/**
	 * Possibly update our belief of the earliest time that a Mortal child StateComponent
	 * will expire.  It is safe to call this method with all child Dates: it will
	 * update the _earliestChildExpiry Date correctly.
	 * @param newDate  the expiry Date of a Mortal child StateComponent
	 */
	private void updateEarliestChildExpiryDate( Date newDate) {
		if( newDate == null)
			return;
		
		if( _earliestChildExpiry == null || newDate.before( _earliestChildExpiry))
			_earliestChildExpiry = newDate;
	}
	
	/**
	 * @return the time when the earliest child will expire, or null if we have
	 * no Mortal children.
	 */
	public Date getEarliestChildExpiryDate() {
		return _earliestChildExpiry;
	}
	

	/**
	 * Update our whenIShouldExpire date.  If the new date is before the existing
	 * one it is ignored.
	 * @param newDate  the new whenIShouldExpire date
	 */
	private void updateWhenIShouldExpireDate( Date newDate) {
		if( newDate == null)
			return;
		
		if( _whenIShouldExpire == null || newDate.after( _whenIShouldExpire))
			_whenIShouldExpire = newDate;
	}
	



	
	/**
	 * Return a cryptic string describing this StateComposite.
	 */
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("StateComposite <");
		sb.append( isMortal() ? "+" : isEphemeral() ? "*" : "+" );
		sb.append("> {");
		sb.append( _children.size());
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
	 *  This function checks whether our parent should expung us.
	 */
	public boolean hasExpired() {
		Date now = new Date();
		
		return _whenIShouldExpire != null ? now.after(_whenIShouldExpire) : false;
	}
	
	/**
	 * Make sure we never expire.
	 */
	private void becomeImmortal() {		
		_isEphemeral = false;
		_whenIShouldExpire = null; 
	}
	
	/**
	 * Switch behaviour to be Ephemeral.  That is, don't expire automatically but
	 * don't prevent Mortal parent(s) from expiring. 
	 */
	private void becomeEphemeral() {
		_isEphemeral = true;
		_whenIShouldExpire = null; 		
	}

	/**
	 * Initialise our expiry time to some point in the future.
	 * @param lifetime the time, in seconds.
	 */
	private void becomeMortal( long lifetime) {
		_whenIShouldExpire = new Date( System.currentTimeMillis() + lifetime * MILLIS_IN_SECOND);
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
		acceptVisitor( null, path, start, visitor);
	}
	
	
	/**
	 * Simulate the effects of the StateTransition, so allowing the StateVisitor to visit the dCache
	 * State after the transition has taken effect.
	 */
	public void acceptVisitor( StateTransition transition, StatePath ourPath, StatePath startPath, StateVisitor visitor) {

		if( _log.isDebugEnabled())
			_log.debug( "acceptVisitor( " + (transition != null ? "not null" : "(null)")+ ", " + (ourPath != null ? ourPath : "(null)") + ", " + (startPath != null ? startPath : "(null)") + " )");

		Map<String,String> branchMetadata = getMetadataInfo();
		

		/** If start is not yet null, iterate down directly */
		if( startPath != null) {
			String childName = startPath.getFirstElement();
			
			if( haveChild( ourPath, transition, childName)) {
				visitor.visitCompositePreSkipDescend(ourPath, branchMetadata);
				visitNamedChild( visitor, ourPath, childName, startPath, transition);
				visitor.visitCompositePostSkipDescend(ourPath, branchMetadata);
			} else {
				
				if( _log.isDebugEnabled())
					_log.debug( this.toString() + " no children when accepting for " + startPath);

				/**
				 * No data is available at this level.  This is because the
				 * client has specified a start-path that (currently) doesn't exist.
				 * The path may be "correct" and will be filled with data "soon",
				 * or may be "incorrect" and data will never appear here.
				 * <p>
				 * Since we cannot distinguish between the two, we silently fail. 
				 */
			}
			return;
		}

		visitor.visitCompositePreDescend(ourPath, branchMetadata);

		Set<String> children = buildChildrenList( ourPath, transition);
		
		// Iterate over them
		for(Iterator<String> itr = children.iterator(); itr.hasNext();) {
			String childName = itr.next();

			if (!itr.hasNext())
				visitor.visitCompositePreLastDescend(ourPath, branchMetadata);

			visitNamedChild( visitor, ourPath, childName, null, transition);
		}

		visitor.visitCompositePostDescend(ourPath, branchMetadata);
	}

	/**
	 * Visit a specific child.  This is somewhat tricky as we must be careful, when there is
	 * a transition, which child we visit.
	 * @param visitor the StateVisitor we are applying 
	 * @param ourPath the StatePath of this StateComposite
	 * @param childName the name of the child
	 * @param startPath the skip path, or null if none
	 * @param transition the StateTransition to consider, or null if dealing with the current state.
	 */
	private void visitNamedChild( StateVisitor visitor, StatePath ourPath, String childName, StatePath startPath, StateTransition transition) {

		StateComponent child = _children.get( childName);
		
		if( transition != null) {
			StateComponent freshChild = transition.getFreshChildValue( ourPath, childName);
			
			if( freshChild != null)
				child = freshChild;
			else {
				if( transition.childIsRemoved( ourPath, childName)) {
					_log.error( "Tried to visit deleted child " + childName + " of " + ourPath != null ? ourPath : "(top)");
					return;
				}
			}
		}
		
		if( child == null) {
			_log.error("Tried to visit null child in " + ourPath != null ? ourPath : "(top)");
			return;
		}
		
		StatePath childStartPath = startPath != null ? startPath.childPath() : null;
		child.acceptVisitor( transition, buildChildPath( ourPath, childName), childStartPath, visitor);
	}
	
	/**
	 * Obtain a child based on StateTransition.  If transition is null, only get this
	 * child from local store, otherwise StateTransition will be considered.
	 * @param ourPath the StatePath for this StateComposite
	 * @param transition the transition under consideration, or null
	 * @param childName the name of the child StateComponent
	 * @return the value of the child after transition.
	 */
	private boolean haveChild( StatePath ourPath, StateTransition transition, String childName) {

		if( transition != null) {
			if( transition.childIsRemoved( ourPath, childName))
				return false;

			if( transition.childIsNew( ourPath, childName))
				return true;
		}
		
		return _children.containsKey( childName);
	}
		

	/**
	 * Build a Set of our children, potentially taking into account a StateTransition.
	 * The transition may be null, in which case this is equivalent to <code>_children.keySet()</code>  
	 * @param ourPath the StatePath of this StateComposite
	 * @param transition the transition to consider, or null if none.
	 * @return a list of child names to iterate over.
	 */
	private Set<String> buildChildrenList( StatePath ourPath, StateTransition transition) {
		if( transition == null)
			return _children.keySet();

		Set<String> allChildren = new HashSet<String>();
		
		Set<String> newChildren = transition.getNewChildren(ourPath);
			
		if( newChildren != null)
			allChildren.addAll( newChildren);
			
		allChildren.addAll( _children.keySet());
		
		Set<String> removedChildren = transition.getRemovedChildren(ourPath);

		if( removedChildren != null)
			allChildren.removeAll( removedChildren);
		
		return allChildren;
	}
	
	
	/**
	 * Apply a transition to our current state.  Children are added, updated or removed based on
	 * the supplied transition.
	 * @param ourPath the path to ourself within dCache tree, or null for top-most StateComposite
	 * @param transition the StateTransition to apply
	 */
	public void applyTransition( StatePath ourPath, StateTransition transition) {
		Set<String> removedChildren = new HashSet<String>();
		Date newExpDate = transition.getWhenIShouldExpireDate(ourPath);
		updateWhenIShouldExpireDate( newExpDate);
		if( newExpDate == null)
			_log.debug("getWhenIShouldExpireDate() returned null: no Mortal children?");
		
		if( transition.haveImmortalChild( ourPath))
			becomeImmortal(); // this is currently irreversable

		// First, remove those children we should remove.
		for( String childName : transition.getRemovedChildren( ourPath)) {
			if( _log.isDebugEnabled())
				_log.debug("removing child " + childName);
			_children.remove( childName);
			removedChildren.add( childName);
		}

		// Then update our existing children.
		for( String childName : transition.getUpdatedChildren( ourPath)) {
			StateComponent updatedChildValue = transition.getUpdatedChildValue(ourPath, childName);

			if( updatedChildValue == null) {
				_log.error( "Attempting to update " + childName + " in " + ourPath + ", but value is null; wilfully ignoring this.");
				continue;
			}
			
			if( _log.isDebugEnabled())
				_log.debug("updating child " + childName + ", updated value " + updatedChildValue.toString());
			
			addComponent( childName, updatedChildValue);
		}


		// Finally, add all new children.
		for( String childName : transition.getNewChildren( ourPath)) {
			StateComponent newChildValue = transition.getNewChildValue( ourPath, childName);

			if( _log.isDebugEnabled())
				_log.debug("adding new child " + childName + ", new value " + newChildValue.toString());
			
			addComponent( childName, newChildValue);
			
			if( newChildValue.isImmortal())
				_log.warn("Adding an immortal child: this is not well supported.");			
		}
		
		// Now, which children should we iterate into?
		for( String childName : transition.getItrChildren( ourPath)) {
			StateComponent child = _children.get( childName);
			
			if( child == null) {
				if( !removedChildren.contains( childName))
					_log.error( "Whilst in "+ourPath+", avoided attempting to applyTransition() on missing child " + childName);
				continue;
			}
			
			child.applyTransition( buildChildPath(ourPath, childName), transition);
		}
		
		recalcEarliestChildExpiry();
	}
	
	
	/**
	 * Recalculate _earliestChildExpiryDate() by asking our children for their earliest expiring
	 * child.
	 */
	private void recalcEarliestChildExpiry() {
		
		_earliestChildExpiry = null; // A forceful reset
		
		for( StateComponent child : _children.values()) {
			
			Date earliestExpires = child.getEarliestChildExpiryDate();

			if( earliestExpires != null)
				updateEarliestChildExpiryDate( earliestExpires);
			
			if( child.isMortal())
				updateEarliestChildExpiryDate( child.getExpiryDate());			
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
	 * @return our metadata info, if there is any, otherwise null.
	 */
	private Map<String,String> getMetadataInfo() {
		Map<String,String> ourMetadataInfo = null;

		if( _metadataRef != null)
			ourMetadataInfo = _metadataRef.getMetadata();

		return ourMetadataInfo;
	}
	
	
	/**
	 * Add a new component to our list of children.
	 * <p>
	 * @param childName the name under which this item should be recorded
	 * @param newChild the StateComponent to be stored.
	 */
	private void addComponent(String childName, StateComponent newChild) {

		StateComponent existingChild = _children.get( childName);
		
		/**
		 *  If we're added a StateComposite, we must be a little more careful!
		 */
		if( newChild instanceof StateComposite) {
			StateComposite newComposite = (StateComposite) newChild;
			
			/**
			 * Copy across all existing children that don't clash.  Those with
			 * the same name are updates for those children, so we want to go with
			 * the values under the newComposite.
			 */
			if( existingChild instanceof StateComposite) {
				StateComposite existingComposite = (StateComposite) existingChild;
				
				// Copy across the existingComposite's children over to the newComposite
				for( Map.Entry<String,StateComponent> entry : existingComposite._children.entrySet())						
					if( !newComposite._children.containsKey(entry.getKey()))
						newComposite._children.put(entry.getKey(), entry.getValue());
				
				// ... and details of the dates...
				newComposite.updateEarliestChildExpiryDate( existingComposite.getEarliestChildExpiryDate());
				newComposite.updateWhenIShouldExpireDate( existingComposite.getExpiryDate());
			}
		}

		_children.put( childName, newChild);
		
		if( _log.isDebugEnabled())
			_log.debug( "Child "+ childName + " now " + _children.get(childName).toString());
	}

	
	
	/**
	 * Update a StateTransition object so a new StateComponent will be added to dCache's state.  The
	 * changes are recorded in StateTransition so they can be applied later.
	 * @param ourPath the StatePath to this StateComposite.
	 * @param newComponentPath the StatePath to this StateComponent, relative to this StateComposition 
	 * @param newComponent the StateComponent to add.
	 * @param transition the StateTransition in which we will record these changes
	 */
	public void buildTransition( StatePath ourPath, StatePath newComponentPath, StateComponent newComponent, StateTransition transition) throws MetricStatePathException {

		String childName = newComponentPath.getFirstElement();
		
		/* If we are mortal and the new child is too, check we don't expire too soon */
		if( this.isMortal() && newComponent.isMortal()) {
			Date newComponentExpiryDate = newComponent.getExpiryDate();
			transition.recordNewWhenIShouldExpireDate( ourPath, newComponentExpiryDate);			
		}
		
		/**
		 * If newComponent is one of our children, process it directly.
		 */
		if( newComponentPath.isSimplePath()) {
			if( _children.containsKey(childName))
				transition.recordUpdatedChild( ourPath, childName, newComponent);
			else
				transition.recordNewChild( ourPath, childName, newComponent);
			
			if( newComponent instanceof StateComposite) {
				StateComposite newComposite = (StateComposite) newComponent;
				newComposite._metadataRef = getChildMetadata(childName);
			}
			
			// Parents of an Immortal Child should know not to expire.
			if( newComponent.isImmortal())
				transition.recordChildIsImmortal( ourPath);
			
			return;
		}

		/**
		 * Otherwise, iterate down; if possible, through the existing tree, otherwise through
		 * a new StateComposite.
		 */
		StateComponent child = _children.get(childName);
			
		if( child == null) {
			
			// Perhaps we're already adding a StateComposite with this transition?
			child = transition.getNewChildValue( ourPath, childName);
			
			if( child == null) {
				// No? OK, create a new NewComposite and record it.
				child = new StateComposite( getChildMetadata( childName), DEFAULT_LIFETIME);
				transition.recordNewChild( ourPath, childName, child);
			}
		}
		
		/**
		 * Even if we didn't change anything, record that we were here
		 */
		transition.recordChildItr( ourPath, childName);
		child.buildTransition( buildChildPath(ourPath, childName), newComponentPath.childPath(), newComponent, transition);
	}
	
	
	public boolean shouldTriggerWatcher( StateComponent newValue) {
		
		/**
		 * If the new value is a new StateComposite, nothing significant will change so 
		 * should not trigger a StateWatcher.  Replacing a StateComposite with anything else
		 * <i>is</i> significant.
		 */
		return !(newValue instanceof StateComposite);
	}

	
	/**
	 * Check whether the specified StatePathPredicate has been triggered by the given StateTransition.
	 * <p>
	 * If none of our children match the top-level element of the predicate, then the answer is definitely
	 * not triggered.
	 * <p>
	 * If a child matches but there are more elements to consider, iterate down: ask the child whether the
	 * predicate has been triggered with one less element in the predicate.
	 * <p>
	 * If the predicate is simple (that is, contains only a single element) then the predicate is
	 * triggered depending on the activity of our children under the StateTransition.
	 * @param ourPath  Our path
	 * @param predicate the predicate we are to check.
	 * @param transition the StateTransition to consider.
	 * @return true if the transition has triggered this predicate, false otherwise
	 */
	public boolean predicateHasBeenTriggered( StatePath ourPath, StatePathPredicate predicate, StateTransition transition) throws MetricStatePathException {

		if( _log.isDebugEnabled())
			_log.debug("entering ("+ (ourPath != null ? ourPath.toString() : "(null)") +", " + (predicate != null ? predicate.toString() : "(null)") + ")");
		
		// Scan through the list of new children first.
		Collection<String> newChildren = transition.getNewChildren(ourPath);
		
		if( newChildren != null) {
			for( String newChildName : newChildren) {

				if( !predicate.topElementMatches(newChildName)) // ignore unrelated children.
					continue;

				if( predicate.isSimplePath())
					return true; // a new child always triggers a predicate, if name matches

				/**
				 *  Ask this child whether the predicate is triggered.  If the child says "yes", we
				 *  concur.  If the answer is no, we continue searching.
				 */ 
				StateComponent child = transition.getNewChildValue( ourPath, newChildName);				
				if( child.predicateHasBeenTriggered( buildChildPath( ourPath, newChildName), predicate.childPath(), transition))
					return true;
			}
		}
		
		// Scan through our existing children
		for( String childName : _children.keySet()) {
			StateComponent child = _children.get( childName);
			
			// If we've done nothing, it can't have changed.
			if( !transition.hasChildChanged( ourPath, childName))
				continue;
			
			// ignore unrelated children
			if( !predicate.topElementMatches(childName))
				continue;

			/**
			 * If predicate's last element is one of our children... 
			 */
			if( predicate.isSimplePath()) {
				// Check various options:
				
				// Removed children always triggers a predicate.
				if( transition.childIsRemoved(ourPath, childName))
					return true; 
				
				// Has child changed "significantly" ?
				StateComponent updatedChildValue = transition.getUpdatedChildValue( ourPath, childName);				
				if( updatedChildValue != null && child.shouldTriggerWatcher(updatedChildValue))
					return true;
			} else {
				// ... otherwise, try iterating down.
				if( child.predicateHasBeenTriggered( buildChildPath( ourPath, childName), predicate.childPath(), transition))
					return true;
			}
		}
		
		return false;
	}

	
	public boolean isEphemeral() {
		return _whenIShouldExpire == null && _isEphemeral;
	}
	
	public boolean isImmortal() {
		return _whenIShouldExpire == null && !_isEphemeral;
	}
	
	public boolean isMortal() {
		return _whenIShouldExpire != null;
	}
	
	
	/**
	 * Build a child's StatePath, taking into account that a path may be null
	 * (this one-liner is repeated fairly often)
	 * @param ourPath our current path, or null if we are the root StateComposite
	 * @param childName the name of the child.
	 * @return
	 */
	private StatePath buildChildPath( StatePath ourPath, String childName) {
		return ourPath != null ? ourPath.newChild(childName) : new StatePath( childName);
	}
	
	/**
	 * Ostensibly, we iterate over all children to find Mortal children that should be
	 * removed.  In practise, cached knowlege of Mortal child expiry Dates means this
	 * iterates over only those StateComponents that contain children that have actually
	 * expired.
	 *  
	 * @param ourPath
	 * @param transition
	 * @param forced
	 */
	public void buildRemovalTransition( StatePath ourPath, StateTransition transition, boolean forced) {
		Date now = new Date();

		if( _log.isDebugEnabled())
			_log.debug("entering buildRemovalTransition( "+ourPath+", ..)");
				
		// Check each child in turn:
		for( Map.Entry<String, StateComponent>entry : _children.entrySet()) {
			
			boolean shouldRemove = forced;
			boolean shouldItr = forced;

			StateComponent childValue = entry.getValue();
			String childName = entry.getKey();

			// If *this* child has expired, we should mark it as To Be Removed.
			if( childValue.hasExpired()) {
				if( _log.isDebugEnabled())
					_log.debug("registering "+childName+" (in path "+ourPath+") for removal.");

				shouldRemove = shouldItr = true;
			}			

			// If *this* child has some child that has expired, iterate down.
			Date childExp = childValue.getEarliestChildExpiryDate();
			if( childExp != null && now.after( childExp))
				shouldItr = true;
			
			if( shouldItr) {
				if( shouldRemove)
					transition.recordRemovedChild( ourPath, childName);
					
				transition.recordChildItr( ourPath, childName);
				childValue.buildRemovalTransition( buildChildPath(ourPath, childName), transition, shouldRemove);				
			}
		}
	}
		
}
