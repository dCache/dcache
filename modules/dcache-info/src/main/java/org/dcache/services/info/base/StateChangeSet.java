package org.dcache.services.info.base;

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Information about all changes a particular StateComposite should undertake
 * as the result of transition.
 *
 * @author Paul Millar <paul.millar@desy.de>
 */
public class StateChangeSet
{
    final Map<String, StateComponent> _newChildren = new HashMap<>();
    final Map<String, StateComponent> _updatedChildren = new HashMap<>();
    final Set<String> _removedChildren = new HashSet<>();

    final Set<String> _itrChildren = new HashSet<>();

    Date _whenIShouldExpire;
    boolean _hasImmortalChildren;

    /**
     * Record that a new child is to be added to this StateComposite.
     *
     * @param childName
     *            the name of the child
     * @param value
     *            the StateComponent
     */
    protected void recordNewChild(String childName, StateComponent value)
    {
        purgeChildEntries(childName);
        _newChildren.put(childName, value);
    }

    /**
     * Record that a child is to be updated.
     *
     * @param path
     *            the StatePath of the StateComposite that contains this
     *            child.
     * @param childName
     *            the name of the child.
     * @param value
     *            the new value of this child.
     */
    protected void recordUpdatedChild(String childName, StateComponent value)
    {
        purgeChildEntries(childName);
        _updatedChildren.put(childName, value);
    }

    /**
     * Record that a child is to be removed.
     *
     * @param path
     *            the StatePath of the StateComposite that contains this
     *            child.
     * @param childName
     *            the name of the child.
     */
    protected void recordRemovedChild(String childName)
    {
        purgeChildEntries(childName);
        _removedChildren.add(childName);
    }

    /**
     * Record that this StateTransition adds a child to this (presumably)
     * StateComposite that is Immortal.
     *
     * @param path
     */
    protected void recordChildIsImmortal()
    {
        _hasImmortalChildren = true;
    }

    /**
     * Record that the _whenIShouldExpire Date should be changed. If a new
     * value is already set for this transition, it is only updated if the
     * new value will occur sooner than the currently record value.
     *
     * @param childExpiryDate
     *            the new Date to be used.
     */
    protected void recordNewWhenIShouldExpireDate(Date childExpiryDate)
    {
        if (childExpiryDate == null) {
            return;
        }

        if (_whenIShouldExpire == null ||
            childExpiryDate.after(_whenIShouldExpire)) {
            _whenIShouldExpire = childExpiryDate;
        }
    }

    /**
     * Discover the new whenIShouldExpire Date, if one is present.
     *
     * @param path
     *            the StatePath of the StateComposite
     * @return the new Data, if one exists, or null.
     */
    protected Date getWhenIShouldExpireDate()
    {
        return _whenIShouldExpire;
    }

    /**
     * Record that the named child StateComponent has had some activity
     * during a StateTransition.
     *
     * @param path
     *            the StateComposite that is iterating into a child.
     * @param childName
     *            the name of the child.
     */
    protected void recordChildItr(String childName)
    {
        _itrChildren.add(childName);
    }

    /**
     * Return the Set of child names for children of path that have been
     * iterated into when building the StateTransition.
     *
     * @param path
     *            the StatePath of the StateComposite
     * @return a Set of child names, or null if this StateComposite was not
     *         updated.
     */
    protected Set<String> getItrChildren()
    {
        return _itrChildren;
    }

    /**
     * Check whether a child has altered.
     *
     * @param path
     *            the StateComposite that is the parent of the child.
     * @param childName
     *            the name of the child under question.
     * @return true if this child is to be added, updated or removed.
     */
    protected boolean hasChildChanged(String childName)
    {
        return _itrChildren.contains(childName) ||
               _newChildren.containsKey(childName) ||
               _updatedChildren.containsKey(childName) ||
               _removedChildren.contains(childName);
    }

    /**
     * Return whether this Transition introduces an Immortal child that is a
     * child of this StatePath
     *
     * @param path
     * @return
     */
    protected boolean haveImmortalChild()
    {
        return _hasImmortalChildren;
    }

    /**
     * Returns whether a particular named child is to be removed.
     *
     * @param path
     *            The StatePath of the StateComposite
     * @param name
     *            the child's name
     * @return true if the child is to be remove, false otherwise.
     */
    protected boolean childIsRemoved(String name)
    {
        return _removedChildren.contains(name);
    }

    /**
     * Returns whether a particular named child is to be updated.
     *
     * @param path
     *            The StatePath of the StateComposite
     * @param name
     *            the child's name
     * @return true if this child is to be removed, false otherwise.
     */
    protected boolean childIsUpdated(String name)
    {
        return _updatedChildren.containsKey(name);
    }

    /**
     * Remove the named child from the list of those to be removed. If the
     * named child isn't to be removed then this method has no effect.
     *
     * @param childName
     */
    protected void ensureChildNotRemoved(String childName)
    {
        _removedChildren.remove(childName);
    }

    /**
     * Returns whether a particular named child is to be added.
     *
     * @param path
     *            The StatePath of the StateComposite
     * @param name
     *            the child's name
     * @return true if this child is to be removed, false otherwise.
     */
    protected boolean childIsNew(String name)
    {
        return _newChildren.containsKey(name);
    }

    /**
     * Return the fresh value for a child; that is, the value of this child
     * after the transition, provided it has been altered.
     * <p>
     * If the child is new or is to be updated then the new value is return.
     * If the child is to be deleted or is unmodified, then null is returned.
     *
     * @param path
     *            the StatePath of the containing StateComposite
     * @param childName
     *            the name of the child.
     * @return the updated or new value for this child, or null.
     */
    protected StateComponent getFreshChildValue(String childName)
    {
        StateComponent newValue;

        newValue = _newChildren.get(childName);

        return newValue != null ? newValue : _updatedChildren.get(childName);
    }

    /**
     * Obtain a collection of all new Children of the StateComposite pointed
     * to by path.
     *
     * @param path
     *            the path of the StateComposite
     * @return a collection of new children.
     */
    protected Set<String> getNewChildren()
    {
        return _newChildren.keySet();
    }

    /**
     * Obtain a collection of all children to be removed.
     *
     * @param path
     *            the StatePath of the StateComposite
     * @return a collection of children's names.
     */
    protected Set<String> getRemovedChildren()
    {
        return _removedChildren;
    }

    /**
     * Obtain a collection of all children that are to be update.
     *
     * @param path
     *            the StatePath of the StateComposite
     * @return a collection of children's names.
     */
    protected Collection<String> getUpdatedChildren()
    {
        return _updatedChildren.keySet();
    }

    /**
     * Obtain the updated value for a child of some StateComposite. This
     * method only returns a value if the child is to be updated. If this
     * child is:
     * <ul>
     * <li>not updated,
     * <li>a new Component,
     * <li>to be removed
     * </ul>
     * then null is returned.
     *
     * @param path
     * @param childName
     * @return
     */
    protected StateComponent getUpdatedChildValue(String childName)
    {
        return _updatedChildren.get(childName);
    }

    /**
     * Obtain the new value for a child of some StateComposite. This is a
     * value registered as a new child value. If this metric is not new, null
     * is returned.
     *
     * @param path
     * @param childName
     * @return
     */
    protected StateComponent getNewChildValue(String childName)
    {
        return _newChildren.get(childName);
    }

    /**
     * Dump our contents to a (quite verbose) String.
     *
     * @return
     */
    protected String dumpContents()
    {
        StringBuilder sb = new StringBuilder();

        sb.append("  new:\n");
        for (Map.Entry<String, StateComponent> newEntry : _newChildren.entrySet()) {
            sb.append("    ");
            sb.append(newEntry.getKey());
            sb.append(" --> ");
            sb.append(newEntry.getValue().toString());
            sb.append("\n");
        }

        sb.append("  update:\n");
        for (Map.Entry<String, StateComponent> updateEntry : _updatedChildren.entrySet()) {
            sb.append("    ");
            sb.append(updateEntry.getKey());
            sb.append(" --> ");
            sb.append(updateEntry.getValue().toString());
            sb.append("\n");
        }

        sb.append("  remove:\n");
        for (String childName : _removedChildren) {
            sb.append("    ");
            sb.append(childName);
            sb.append("\n");
        }

        return sb.toString();
    }

    /**
     * Remove any reference to the named child in the new, updated or removed
     * children. The list of those child entries to iterate down into is not
     * affected.
     * <p>
     * It is intended this is done before adding a child entry.
     *
     * @param childName
     */
    private void purgeChildEntries(String childName)
    {
        _newChildren.remove(childName);
        _updatedChildren.remove(childName);
        _removedChildren.remove(childName);
    }
}
