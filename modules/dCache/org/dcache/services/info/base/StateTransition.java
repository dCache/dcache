package org.dcache.services.info.base;

import java.util.*;

/**
 * A StateTransition contains information about a pending alteration to the current
 * dCache state tree.  Unlike a StateUpdate, the StateTransition includes this information
 * in terms of Change-sets, each contains the changes for a specific StateComposites
 * (i.e., branches) instead of a collection of StatePaths.  Each change-set contains information
 * on new children, children that are to be updated, and those that are to be removed.
 * <p>
 * Providing infomation like this, the Visitor pattern can be applied, iterating over
 * proposed changes, with the StateTransition object working in colaboration with the
 * existing dCache state tree. 
 * @see StateUpdate
 * @author Paul Millar <paul.millar@desy.de>
 */
public class StateTransition {
	
	private static final Set<String> EMPTY_SET = new HashSet<String>();

	/**
	 * Information about a particular StateComposite's changes
	 * 	New StateComponent
	 * 	Renewed StateComponent
	 * 	Altered StateValue
	 * 	Removed StateComponent
	 * @author Paul Millar <paul.millar@desy.de>
	 */
	private static class CompositeChangeSet {
		final Map<String, StateComponent> _newChildren = new HashMap<String, StateComponent>();
		final Map<String, StateComponent> _updatedChildren = new HashMap<String, StateComponent>();
		final Set<String> _removedChildren = new HashSet<String>();
		final Set<String> _itrChildren = new HashSet<String>();
		Date _whenIShouldExpire = null;
		boolean _hasImmortalChildren = false;
	}
	
	final Map<StatePath, CompositeChangeSet> _allChanges = new HashMap<StatePath, CompositeChangeSet>();
	
	/**
	 * Record that a new child is to be added
	 * @param path the StatePath of the StateComposite
	 * @param name the name of the child StateComponent
	 * @param value the StateComponent that is to be added
	 */
	protected void recordNewChild( StatePath path, String name, StateComponent value) {
		CompositeChangeSet changeSet = getOrCreateChangeSet( path);
		changeSet._newChildren.put(name, value);
		changeSet._itrChildren.add(name);
	}

	/**
	 * Record that a child is to be updated.
	 * @param path the StatePath of the StateComposite that contains this child.
	 * @param name the name of the child.
	 * @param value the new value of this child.
	 */
	protected void recordUpdatedChild( StatePath path, String name, StateComponent value) {
		CompositeChangeSet changeSet = getOrCreateChangeSet( path);
		changeSet._updatedChildren.put(name, value);
		changeSet._itrChildren.add(name);
	}
	
	/**
	 * Record that a child is to be removed.
	 * @param path the StatePath of the StateComposite that contains this child.
	 * @param name the name of the child.
	 */
	protected void recordRemovedChild( StatePath path, String name) {
		CompositeChangeSet changeSet = getOrCreateChangeSet( path);
		changeSet._removedChildren.add(name);
	}
	
	/**
	 * Record that this StateTransition adds a child to this (presumably) StateComposite
	 * that is Immortal.
	 * @param path
	 */
	protected void recordChildIsImmortal( StatePath path) {
		CompositeChangeSet changeSet = getOrCreateChangeSet( path);
		changeSet._hasImmortalChildren = true;
	}

	
	
	/**
	 * Record that the _whenIShouldExpire Date should be changed.  If a new value is already
	 * set for this transition, it is only updated if the new value is sooner than the
	 * currently record value.
	 * @param childExpiryDate the new Date to be used.
	 */
	protected void recordNewWhenIShouldExpireDate( StatePath path, Date childExpiryDate) {

		if( childExpiryDate == null)
			return;
		
		CompositeChangeSet changeSet = getOrCreateChangeSet( path);
		
		if( changeSet._whenIShouldExpire == null || childExpiryDate.after(changeSet._whenIShouldExpire))
			changeSet._whenIShouldExpire = childExpiryDate;		
	}
	
	
	
	/**
	 * Discover the new whenIShouldExpire Date, if one is present.
	 * @param path  the StatePath of the StateComposite
	 * @return the new Data, if one exists, or null.
	 */
	protected Date getWhenIShouldExpireDate( StatePath path) {
		CompositeChangeSet changeSet = _allChanges.get(path);
		
		return (changeSet != null) ? changeSet._whenIShouldExpire : null;			
	}

	/**
	 * Record that the named child StateComponent has had some activity during a
	 * StateTransition.
	 * @param path the StateComposite that is iterating into a child.
	 * @param childName the name of the child.
	 */
	protected void recordChildItr( StatePath path, String childName) {
		CompositeChangeSet changeSet = getOrCreateChangeSet( path);

		changeSet._itrChildren.add( childName);
	}
	
	/**
	 * Return the Set of child names for children of path that have been iterated into
	 * when building the StateTransition.
	 * @param path the StatePath of the StateComposite
	 * @return a Set of child names, or null if this StateComposite was not updated.
	 */
	protected Set<String> getItrChildren( StatePath path) {
		CompositeChangeSet changeSet = _allChanges.get(path);

		return changeSet == null ? EMPTY_SET : changeSet._itrChildren;
	}
	
	/**
	 * Check whether a child has altered.
	 * @param path the StateComposite that is the parent of the child.
	 * @param childName the name of the child under question.
	 * @return true if this child is to be added, updated or removed. 
	 */
	protected boolean hasChildChanged( StatePath path, String childName) {
		CompositeChangeSet changeSet = _allChanges.get(path);
		return changeSet == null ? false : changeSet._itrChildren.contains( childName);
	}

	
	/**
	 * Return whether this Transition introduces an Immortal child
	 * that is a child of this StatePath
	 * @param path
	 * @return
	 */
	protected boolean haveImmortalChild( StatePath path) {
		CompositeChangeSet changeSet = _allChanges.get(path);
		return changeSet != null ? changeSet._hasImmortalChildren : false;
	}

	
	/**
	 * Returns whether a particular named child is to be removed.
	 * @param path The StatePath of the StateComposite
	 * @param name the child's name
	 * @return true if the child is to be remove, false otherwise.
	 */
	protected boolean childIsRemoved( StatePath path, String name) {
		CompositeChangeSet changeSet = _allChanges.get(path);
		
		return changeSet == null ? false : changeSet._removedChildren.contains(name);
	}
	
	/**
	 * Returns whether a particular named child is to be updated.
	 * @param path The StatePath of the StateComposite
	 * @param name the child's name
	 * @return true if this child is to be removed, false otherwise.
	 */
	protected boolean childIsUpdated( StatePath path, String name) {
		CompositeChangeSet changeSet = _allChanges.get(path);
		
		return changeSet == null ? false : changeSet._updatedChildren.containsKey(name);		
	}
	
	
	/**
	 * Returns whether a particular named child is to be added.
	 * @param path The StatePath of the StateComposite
	 * @param name the child's name
	 * @return true if this child is to be removed, false otherwise.
	 */
	protected boolean childIsNew( StatePath path, String name) {
		CompositeChangeSet changeSet = _allChanges.get(path);
		
		return changeSet == null ? false : changeSet._newChildren.containsKey(name);				
	}
	
		
	/**
	 * Return the fresh value for a child.  If the child is new or
	 * is to be updated then the new value is return.  If the child 
	 * is to be deleted or is unmodified, then null is returned. 
	 * @return the updated or new value for this child, or null.
	 */
	protected StateComponent getFreshChildValue( StatePath path, String childName) {
		CompositeChangeSet changeSet = _allChanges.get( path);
		StateComponent newValue;

		if( changeSet == null)
			return null;
		
		newValue = changeSet._newChildren.get(childName);
		
		return newValue != null ? newValue : changeSet._updatedChildren.get(childName);
	}
	
	/**
	 * Obtain a collection of all new Children of the StateComposite pointed to by path.
	 * @param path the path of the StateComposite
	 * @return a collection of new children.
	 */
	protected Set<String> getNewChildren( StatePath path) {
		CompositeChangeSet changeSet = _allChanges.get( path);
		return changeSet == null ? EMPTY_SET : changeSet._newChildren.keySet();
	}
	
	/**
	 * Obtain a collection of all children to be removed.
	 * @param path the StatePath of the StateComposite
	 * @return a collection of children's names.
	 */
	protected Set<String> getRemovedChildren( StatePath path) {
		CompositeChangeSet changeSet = _allChanges.get( path);
		return changeSet == null ? EMPTY_SET : changeSet._removedChildren;
	}
	
	/**
	 * Obtain a collection of all children that are to be update.
	 * @param path the StatePath of the StateComposite
	 * @return a collection of children's names.
	 */
	protected Collection<String> getUpdatedChildren( StatePath path) {
		CompositeChangeSet changeSet = _allChanges.get( path);
		return changeSet == null ? EMPTY_SET : changeSet._updatedChildren.keySet();
	}
	
	
	/**
	 * Obtain the updated value for a child of some StateComposite.  This is a value registered as
	 * an updated child value.  If this metric is not updated, null is returned.
	 * @param path
	 * @param childName
	 * @return
	 */
	protected StateComponent getUpdatedChildValue( StatePath path, String childName) {
		CompositeChangeSet changeSet = _allChanges.get( path);
		return changeSet == null ? null : changeSet._updatedChildren.get( childName);
	}

	
	/**
	 * Obtain the new value for a child of some StateComposite.  This is a value registered as
	 * a new child value.  If this metric is not new, null is returned.
	 * @param path
	 * @param childName
	 * @return
	 */
	protected StateComponent getNewChildValue( StatePath path, String childName) {
		CompositeChangeSet changeSet = _allChanges.get( path);
		return changeSet != null ? changeSet._newChildren.get( childName) : null;
	}
	
	/**
	 * Obtain the CompositeChangeSet for this path.  If one doesn't exist, an empty one is created
	 * and returned. 
	 * @param path  The StatePath for the composite.
	 * @return this StatePath's change-set
	 */
	private CompositeChangeSet getOrCreateChangeSet( StatePath path) {
		CompositeChangeSet changeSet;

		changeSet = _allChanges.get(path);
		
		if( changeSet == null) {
			changeSet = new CompositeChangeSet();
			_allChanges.put(path, changeSet);
		}

		return changeSet;
	}
	
	
	/**
	 * Dump our contents to a (quite verbose) String.
	 * @return
	 */
	protected String dumpContents() {
		StringBuffer sb = new StringBuffer();
		boolean isFirst = true;

		for( Map.Entry<StatePath,CompositeChangeSet> entry :  _allChanges.entrySet()) {
			CompositeChangeSet changeSet = entry.getValue();

			if( isFirst)
				isFirst = false;
			else
				sb.append( "\n");

			sb.append( "Path: ");
			sb.append( entry.getKey() != null ? entry.getKey() : "(null)");
			sb.append( "\n");
			
			sb.append( "  new:\n");
			for( Map.Entry<String, StateComponent> newEntry: changeSet._newChildren.entrySet()) {
				sb.append( "    ");
				sb.append( newEntry.getKey());
				sb.append( " --> ");
				sb.append( newEntry.getValue().toString());
				sb.append( "\n");
			}
			
			sb.append( "  update:\n");
			for( Map.Entry<String, StateComponent> updateEntry: changeSet._updatedChildren.entrySet()) {
				sb.append( "    ");
				sb.append( updateEntry.getKey());
				sb.append( " --> ");
				sb.append( updateEntry.getValue().toString());
				sb.append( "\n");
			}

			sb.append( "  remove:\n");
			for( String childName : changeSet._removedChildren) {
				sb.append( "    ");
				sb.append( childName);
				sb.append( "\n");
			}
		}
		
		return sb.toString();
	}
		
}
