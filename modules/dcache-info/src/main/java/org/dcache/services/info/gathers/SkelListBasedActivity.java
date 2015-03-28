package org.dcache.services.info.gathers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Set;
import java.util.Stack;

import org.dcache.services.info.base.StateExhibitor;
import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.stateInfo.ListVisitor;


/**
 * Provide generic support for building a list of items, taken from some part of the current
 * dCache state and triggering some activity based on this list.  Most likely this is
 * sending off messages, but it could (in principle) be anything.
 * <p>
 * Events will be triggered for each list item before the current state is re-evaluated and a
 * fresh list generated.  The method getNextItem() treats the List as a stack and pops the next
 * item off the list.  It is assumed (although not required) that subclasses will call getNextItem()
 * once per trigger.
 * <p>
 * The time between successive trigger()s is carefully controlled.  If there are items on the
 * list, then SUCCESSIVE_MSG_DELAY milliseconds is used.  Once the list is exhausted, it is
 * automatically refreshed when triggered() is next called.  If the list is empty, then the next
 * trigger may be subject to an additional delay.  This is to ensure that the list is generated
 * no more than once every MINIMUM_LIST_REFRESH_PERIOD milliseconds, which is to ensure that no
 * element is called more than once in this period.
 *
 * @author Paul Millar <paul.millar@desy.de>
 */
abstract public class SkelListBasedActivity implements Schedulable {

    private static final Logger _log = LoggerFactory.getLogger(SkelListBasedActivity.class);


    /** Minimum time between fetching a fresh list (or querying the same list-item), in milliseconds */
    private static final int MINIMUM_LIST_REFRESH_PERIOD = 60000;

    /** Time between sending successive messages, in milliseconds */
    private static final int SUCCESSIVE_MSG_DELAY = 100;

    /** For how long should the resulting metrics live? (in seconds) */
    private long _metricLifetime;

    /** When it is acceptable to refresh the list, based on the MINIMUM_LIST_REFRESH_PERIOD */
    private Date _whenListRefresh;

    /**  When we should next send a message */
    private Date _nextSendMsg = new Date();

    /** Our collection of to-be-done work */
    private final Stack<String> _outstandingWork = new Stack<>();

    /** The StatePath pointing to the parent who's children we want to iterate over */
    private final StatePath _parentPath;

    /** Minimum time between fetching a fresh list (or querying the same list-item), in milliseconds */
    private final int _minimumListRefreshPeriod;

    /** Time between sending successive messages, in milliseconds */
    private final int _successiveMsgDelay;

    private final StateExhibitor _exhibitor;

    /**
     * Create a new List-based activity, based on the list of items below parentPath in
     * dCache's state.
     * @param parentPath all list items must satisfy parentPath.isParentOf(item)
     */
    protected SkelListBasedActivity (StateExhibitor exhibitor, StatePath parentPath) {
        _parentPath = parentPath;
        _exhibitor = exhibitor;

        updateStack();  // Bring in initial work.

        _minimumListRefreshPeriod = MINIMUM_LIST_REFRESH_PERIOD;
        _successiveMsgDelay = SUCCESSIVE_MSG_DELAY;

        randomiseDelay(); // Randomise our initial offset.
    }

    /**
     * Create a new List-based activity that is based on a list of items below parentPath
     * in dCache's state.  The trigger() method is called periodically and getNextItem() method
     * provides the name of the item to be processed.
     * @param parentPath the path of the parent object we should iterate over
     * @param minimumListRefreshPeriod An enforced minimum time, in milliseconds, between the same item being called.
     * @param successiveMsgDelay The minimum time between triggering() successive items, in milliseconds.
     */
    protected SkelListBasedActivity (StateExhibitor exhibitor, StatePath parentPath, int minimumListRefreshPeriod, int successiveMsgDelay) {
        _parentPath = parentPath;
        _exhibitor = exhibitor;
        updateStack();  // Bring in initial work.

        _minimumListRefreshPeriod = minimumListRefreshPeriod;
        _successiveMsgDelay = successiveMsgDelay;

        randomiseDelay();  // Randomise our initial offset.
    }

    /**
     * Set when we are next to trigger an event to be a random fraction of
     * the minimum refresh period.  This is most useful when starting up.
     */
    private void randomiseDelay() {
        _whenListRefresh = new Date(System.currentTimeMillis() + (long) (Math.random() * _minimumListRefreshPeriod));
    }


    /**
     *  When should we next be triggered?
     *  @returns the desired time we should next be triggered.
     */
    @Override
    public Date shouldNextBeTriggered() {
        return _outstandingWork.empty() ? _whenListRefresh : _nextSendMsg;
    }


    /**
     * We maintain our outstanding work List by (potentially) fetching a new list from the
     * current state.  Classes that extend this class should call super() so getNextItem()
     * continues to work.
     */
    @Override
    public void trigger() {
        Date now = new Date();

        _nextSendMsg = new Date(System.currentTimeMillis() + _successiveMsgDelay);

        if (!_outstandingWork.empty() || now.before(_whenListRefresh)) {
            return;
        }

        updateStack();

        /**
         *  Calculate the earliest we would like to do this again.
         */
        long timeToSendAllMsgs = (long) (_outstandingWork.size() * _successiveMsgDelay);
        long listRefreshPeriod = timeToSendAllMsgs < _minimumListRefreshPeriod ? _minimumListRefreshPeriod : timeToSendAllMsgs;
        _whenListRefresh = new Date(System.currentTimeMillis() + listRefreshPeriod);


        /**
         *  All metrics that are generated should have a lifetime based on when we expect
         *  to refresh the list and generate more metrics.
         *  The 2.5 factor allows for both 50% growth and a message being lost.
         */
        _metricLifetime = (long) (2.5 * listRefreshPeriod / 1000.0);
    }


    /**
     *  Query dCache's current state and add the new Set to to our _outstandingWork Stack.
     */
    private void updateStack() {
        Set<String> items = ListVisitor.getDetails(_exhibitor, _parentPath);

        for (String item : items) {
            _outstandingWork.add(item);
        }

        if (_log.isDebugEnabled()) {
                _log.debug("fresh to-do list obtained for " + this.getClass().getSimpleName());
                _log.debug("list now contains " + _outstandingWork.size() + " items");
        }
    }


    /**
     * @return the next item off the list if there is one, or null otherwise.
     */
    public String getNextItem() {
        if (_outstandingWork.empty()) {
            return null;
        }

        return _outstandingWork.pop();
    }


    /**
     * @return an appropriate lifetime for a metric, in seconds.
     */
    public long getMetricLifetime() {
        return _metricLifetime;
    }
}
