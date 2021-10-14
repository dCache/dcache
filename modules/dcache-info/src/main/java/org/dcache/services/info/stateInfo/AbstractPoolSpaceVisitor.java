package org.dcache.services.info.stateInfo;

import org.dcache.services.info.base.IntegerStateValue;
import org.dcache.services.info.base.StatePath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides an abstract interface for discovering pool space information.
 * <p>
 * Classes that extend this class must implement the newPool() method.  This method is called once
 * per pool recorded in the dCache state.  The pool's name and a summary of the pool's space-usage
 * are passed as parameters.
 *
 * @author Paul Millar <paul.millar@desy.de>
 */
public abstract class AbstractPoolSpaceVisitor extends SkeletonListVisitor {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractPoolSpaceVisitor.class);
    private static final StatePath POOLS_PATH = new StatePath("pools");

    private SpaceInfo _currentPoolSpaceInfo;
    private StatePath _currentPoolSpacePath;

    private static final String METRIC_NAME_FREE = "free";
    private static final String METRIC_NAME_PRECIOUS = "precious";
    private static final String METRIC_NAME_TOTAL = "total";
    private static final String METRIC_NAME_REMOVABLE = "removable";
    private static final String METRIC_NAME_USED = "used";

    protected AbstractPoolSpaceVisitor() {
        super(POOLS_PATH);
    }

    /**
     * Called once per pool in dCache state
     */
    protected abstract void newPool(String poolName, SpaceInfo space);

    @Override
    protected void newListItem(String itemName) {
        LOGGER.trace("Found pool {}", itemName);
        super.newListItem(itemName);

        _currentPoolSpaceInfo = new SpaceInfo();
        _currentPoolSpacePath = getPathToList().newChild(itemName).newChild("space");
    }

    @Override
    protected void exitingListItem(String itemName) {
        super.exitingListItem(itemName);

        newPool(itemName, _currentPoolSpaceInfo);
    }


    @Override
    public void visitInteger(StatePath path, IntegerStateValue value) {
        if (_currentPoolSpacePath == null || !_currentPoolSpacePath.isParentOf(path)) {
            return;
        }

        String metricName = path.getLastElement();

        LOGGER.trace("Found metric {} = {}", metricName, value.getValue());

        switch (metricName) {
            case METRIC_NAME_REMOVABLE:
                _currentPoolSpaceInfo.setRemovable(value.getValue());
                break;
            case METRIC_NAME_FREE:
                _currentPoolSpaceInfo.setFree(value.getValue());
                break;
            case METRIC_NAME_TOTAL:
                _currentPoolSpaceInfo.setTotal(value.getValue());
                break;
            case METRIC_NAME_PRECIOUS:
                _currentPoolSpaceInfo.setPrecious(value.getValue());
                break;
            case METRIC_NAME_USED:
                _currentPoolSpaceInfo.setUsed(value.getValue());
                break;
        }
    }
}
