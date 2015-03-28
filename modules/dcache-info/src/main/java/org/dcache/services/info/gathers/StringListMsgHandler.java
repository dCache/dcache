package org.dcache.services.info.gathers;

import dmg.cells.nucleus.UOID;

import org.dcache.services.info.base.StateComposite;
import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.base.StateUpdate;
import org.dcache.services.info.base.StateUpdateManager;

/**
 * A generic routine for processing an incoming CellMessage.  The message is expected to
 * be an array of Strings that should be inserted into dCache state at a specific
 * point in the state.
 * @author Paul Millar <paul.millar@desy.de>
 */
public class StringListMsgHandler extends CellMessageHandlerSkel
{
    final private StatePath _path;

    /**
     * Create a new generic String-list message handler.
     * @param path a String representation of the path under which incoming elements
     * will be added
     */
    public StringListMsgHandler(StateUpdateManager sum,
            MessageMetadataRepository<UOID> msgMetaRepo, String path)
    {
        super(sum, msgMetaRepo);
        _path = new StatePath(path);
    }

    @Override
    public void process(Object msgPayload, long metricLifetime)
    {
        Object array[] = (Object []) msgPayload;

        if (array.length == 0) {
            return;
        }

        StateUpdate update = new StateUpdate();

        for (Object element : array) {
            String listItem = (String) element;
            update.appendUpdate(_path
                    .newChild(listItem), new StateComposite(metricLifetime));
        }

        applyUpdates(update);
    }
}
