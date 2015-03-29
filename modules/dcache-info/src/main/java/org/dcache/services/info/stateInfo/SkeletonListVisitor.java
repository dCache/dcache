package org.dcache.services.info.stateInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import org.dcache.services.info.base.BooleanStateValue;
import org.dcache.services.info.base.FloatingPointStateValue;
import org.dcache.services.info.base.IntegerStateValue;
import org.dcache.services.info.base.StateGuide;
import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.base.StateVisitor;
import org.dcache.services.info.base.StringStateValue;
import org.dcache.services.info.base.guides.SubtreeStateGuide;

/**
 * A simple skeleton class that provides a StateVisitor that iterates over items in a list.
 * The constructor should be called with the StatePath of the parent StateComposite.  Each
 * child of this StateComposite is considered a item within the list and newListItem() will be
 * called for each such list.  Subclasses may overload that method.
 *
 * The method getKey() will return the last item, as recorded by the subclass calling
 * super.newListItem(key), typically done within an overloaded method newListitem.
 *
 * @author Paul Millar <paul.millar@desy.de>
 */
public class SkeletonListVisitor implements StateVisitor
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SkeletonListVisitor.class);

    final private StatePath _pathToList;

    /** The key of the current branch */
    private String _thisKey;
    private final StateGuide _guide;

    /**
     * Instantiate the list over the items underneath pathToList.
     * @param pathToList the StatePath representing the parent object for this list.
     */
    protected SkeletonListVisitor(StatePath pathToList)
    {
        LOGGER.trace("Searching on path {}", pathToList);
        _pathToList = pathToList;
        _guide = new SubtreeStateGuide(pathToList);
    }

    @Override
    public boolean isVisitable(StatePath path)
    {
        return _guide.isVisitable(path);
    }

    /**
     * The super-Class should override one of the following four methods
     */
    @Override
    public void visitBoolean(StatePath path, BooleanStateValue value)
    {
    }

    @Override
    public void visitFloatingPoint(StatePath path, FloatingPointStateValue value)
    {
    }

    @Override
    public void visitInteger(StatePath path, IntegerStateValue value)
    {
    }

    @Override
    public void visitString(StatePath path, StringStateValue value)
    {
    }

    @Override
    public void visitCompositePreDescend(StatePath path, Map<String, String> metadata)
    {
        if (_pathToList.isParentOf(path)) {
            LOGGER.trace("Entering {}", path);
            newListItem(path.getLastElement());
        }
    }

    @Override
    public void visitCompositePostDescend(StatePath path, Map<String, String> metadata)
    {
        if (_pathToList.isParentOf(path)) {
            LOGGER.trace("Leaving {}", path);
            exitingListItem(path.getLastElement());
        }
    }

    /**
     * Method called whenever a new list item is visited.
     * @param listItemName the name of the list item to record.
     * @see the getKey() method.
     */
    protected void newListItem(String listItemName)
    {
        LOGGER.trace("Assigning _thisKey to {}", listItemName);
        _thisKey = listItemName;
    }

    /**
     * Method called whenever the visitor is leaving a list item.
     * @param listItemName the name of the list item that is being left.
     */
    protected void exitingListItem(String listItemName)
    {
        LOGGER.trace("Resetting _thisKey to null on leaving {}", listItemName);
        _thisKey = null;
    }

    /**
     * Obtain the StatePath to the parent object for all list items.
     */
    protected StatePath getPathToList()
    {
        return _pathToList;
    }

    /**
     * @return the name of the last item in the list, or null if not currently within a list item.
     */
    protected String getKey()
    {
        return _thisKey;
    }

    /**
     * Check whether the visitor is within (underneath) the list item.  If so,
     * then getKey() will return the valid key.
     * @return true if visitor is within a list item, false otherwise.
     */
    protected boolean isInListItem()
    {
        return _thisKey != null;
    }
}
