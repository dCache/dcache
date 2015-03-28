package org.dcache.services.info.gathers;

import dmg.cells.nucleus.CellMessageAnswerable;
import dmg.cells.nucleus.CellPath;

import org.dcache.services.info.base.StateExhibitor;
import org.dcache.services.info.base.StatePath;


/**
 * This class sends a series of messages based on the current state tree.
 *   It uses a visitor to
 * extract all entries below a certain point in the state tree and constructs a message for
 * each entry; for example, if the path is "dCache.pools" and the state contains entries for
 * "dCache.pools.fandango_1" and "dCache.pools.fandango_2" then two CellMessages are sent.
 * <p>
 * @author Paul Millar <paul.millar@desy.de>
 */
public class ListBasedMessageDga extends SkelListBasedActivity {

    private final CellPath _cellPath;
    private final String _messagePrefix;
    private final CellMessageAnswerable _handler;

    private final MessageSender _sender;

    /* The following two are only needed for toString() */
    private final String _cellName;
    private final String _parentPath;


    /**
     * Create a new list-based data-gathering activity
     * @param parent the StatePath that points to the list's parent item
     * @param cellName the name of the cell to contact
     * @param message the message to send.
     * @param handler the cell handler for the return msg payload.
     */
    public ListBasedMessageDga(StateExhibitor exhibitor, MessageSender sender, StatePath parent, String cellName, String message, CellMessageAnswerable handler) {

        super(exhibitor, parent);

        _cellName = cellName;
        _parentPath = parent.toString();
        _messagePrefix = message;
        _handler = handler;

        _cellPath = new CellPath(cellName);
        _sender = sender;
    }

    /**
     * Triggered every-so-often, under control of SkelListBasedActivity.
     */
    @Override
    public void trigger() {

        super.trigger();

        String item = getNextItem();

        // Only null if there's nothing under _parentPath in dCache.
        if (item == null) {
            return;
        }

        StringBuilder sb = new StringBuilder();
        sb.append(_messagePrefix);
        sb.append(" ");
        sb.append(item);

        _sender.sendMessage(getMetricLifetime(), _handler, _cellPath, sb.toString());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append(this.getClass().getSimpleName());
        sb.append("[");
        sb.append(_cellName);
        sb.append(", ");
        sb.append(_parentPath);
        sb.append(", ");
        sb.append(_messagePrefix);
        sb.append("]");

        return sb.toString();
    }
}
