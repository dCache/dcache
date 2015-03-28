package org.dcache.services.info.stateInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

import org.dcache.services.info.base.StateExhibitor;
import org.dcache.services.info.base.StatePath;


/**
 * A very simple StateVisitor class.  This visitor builds a list of the names of immediate
 * children of a StateComposite.  The parent StateComposite is described by the StatePath.
 * @author Paul Millar <paul.millar@desy.de>
 */
public class ListVisitor extends SkeletonListVisitor
{
    private static Logger _log = LoggerFactory.getLogger(ListVisitor.class);

    /**
     * Obtain the set of items below a certain path within the dCache state.
     * @param path the StatePath that is the parent to the required items.
     * @return the Set of all items that have the path as their parent.
     */
    static public Set<String> getDetails(StateExhibitor exhibitor, StatePath path)
    {
        if (_log.isDebugEnabled()) {
            _log.debug("Gathering current status for path " + path);
        }

        ListVisitor visitor = new ListVisitor(path);
        exhibitor.visitState(visitor);
        return visitor.getItems();
    }

    private final Set<String> _listItems;

    public ListVisitor(StatePath parent)
    {
        super(parent);
        _listItems = new HashSet<>();
    }

    @Override
    protected void newListItem(String name)
    {
        super.newListItem(name);
        _listItems.add(name);
    }

    public Set<String> getItems()
    {
        return _listItems;
    }
}
