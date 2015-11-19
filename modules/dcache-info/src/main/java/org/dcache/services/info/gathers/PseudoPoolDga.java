package org.dcache.services.info.gathers;

import java.util.Date;
import java.util.Random;

import org.dcache.services.info.base.StateComposite;
import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.base.StateUpdate;
import org.dcache.services.info.base.StateUpdateManager;
import org.dcache.services.info.stateInfo.SpaceInfo;


/**
 * The PseudoPoolDga provides a stream of fake data about a fake pool.  It
 * is used to introduce load on the info state system and watchers.  This
 * DGA will also register itself as a member of the <code>default</code>
 * Poolgroup.
 * <p>
 * To use this class, add a line like:
 * <pre>
 *        // A pool that sends status information every 30s
 *        //     (called "pseudo_1" and containing 40 GiB)
 *        addActivity(new PseudoPoolDga("pseudo_1", 30000, 40));
 * </pre>
 * to the <code>DataGatheringScheduler.addDefaultActivity()</code> method.
 * <p>
 * This class provides a very rough-and-ready simulation of data activity,
 * with files being added and deleted at random intervals.
 *
 * @author Paul Millar <paul.millar@desy.de>
 */
public class PseudoPoolDga implements Schedulable
{
    /** Max number standard deviation to allow in delay */
    private static final int NUM_SD = 4;
    /** The safety margin we should ensure for metrics, in seconds */
    private static final int SAFETY_MARGIN = 2;
    private static final long BYTES_IN_GIBIBYTE = 1024 * 1024 * 1024;

    private static final StatePath DEFAULT_POOLGROUP_MEMBERSHIP = StatePath.parsePath("poolgroups.default.pools");

    static final double PRECIOUS_ADD_LIKELIHOOD = 0.5;
    /** Fraction of total space to add, on average */
    static final double PRECIOUS_ADD_DELTA = 0.01;
    /** Standard deviation of space added, as fraction of the average */
    static final double PRECIOUS_ADD_SPREAD = 0.01;
    static final double PRECIOUS_DEL_DELTA = 0.1;
    static final double PRECIOUS_DEL_SPREAD = 0.01;
    static final double PRECIOUS_DEL_SUPPRESS = 0.01;

    /** Likelihood of adding files each iteration */
    static final double REMOVABLE_ADD_LIKELIHOOD = 0.8;
    /** Fraction of total space that is max added in Zipf */
    static final double REMOVABLE_ADD_MAX_FRAC = 0.03;
    /** How fast the Zipf falls off, controls the spread of files */
    static final double REMOVABLE_ADD_EXP = 0.05;
    /** When removing, what fraction of removable is deleted*/
    static final double REMOVABLE_DEL_DELTA = 0.3;
    /** The standard deviation of the removed amount, from  */
    static final double REMOVABLE_DEL_SPREAD = 0.05;
    /** Suppress how often files are removed  */
    static final double REMOVABLE_DEL_SUPPRESS = 0.01;

    private final String _poolName;
    private final SpaceInfo _spaceInfo;
    private final StatePath _ourSpacePath;
    private StatePath _ourPgMembership;
    /** Average time between successive calls, in milliseconds */
    private final int _delay;
    /** Standard deviation for Normal spread of successive calls */
    private final int _spread;
    /** Lifetime of metrics, in seconds */
    private final long _metricLifetime;
    /** Maximum delay between successive queries, in milliseconds */
    private final long _maxDelay;
    private final Random _rnd = new Random();

    private final StateUpdateManager _sum;

    /**
     * Create a new fake pool, injecting information roughly periodically
     * (5% spread) and randomly increasing (and purging) space usage.
     * @param poolName the name of this pool
     * @param how often (on average) update metrics should be sent, in milliseconds
     * @param the total capacity of this pool, in GiB
     */
    public PseudoPoolDga(StateUpdateManager sum, String poolName, int updatePeriod,
            long capacity)
    {
        _poolName = poolName;
        _sum = sum;

        StatePath pools = new StatePath("pools");
        _ourSpacePath = pools.newChild(poolName).newChild("space");

        // Parameters for how often metrics are updated
        _delay = updatePeriod;
        _spread = (int) (0.05 * updatePeriod);

        _maxDelay = _delay + NUM_SD * _spread;
        _metricLifetime = _maxDelay/1000 + SAFETY_MARGIN;

        _ourPgMembership = DEFAULT_POOLGROUP_MEMBERSHIP.newChild(poolName);

        // Initially, completely empty.
        _spaceInfo = new SpaceInfo(capacity * BYTES_IN_GIBIBYTE);
    }

    // Roughly periodic: Normal with mean: _duration, SD: _spread
    @Override
    public Date shouldNextBeTriggered()
    {
        long thisDelay = _delay + (long) (_rnd.nextGaussian() * _spread);

        if (thisDelay > _maxDelay) {
            thisDelay = _maxDelay;
        }

        return new Date(System.currentTimeMillis() + thisDelay);
    }


    /**
     *  When we've been triggered.
     */
    @Override
    public void trigger()
    {
        updateSpace();

        StateUpdate update = new StateUpdate();

        // Renew our membership of default poolgroup
        update.appendUpdate(_ourPgMembership, new StateComposite(_metricLifetime));

        _spaceInfo.addMetrics(update, _ourSpacePath, _metricLifetime);

        _sum.enqueueUpdate(update);
    }

    /**
     * Update our space information.  We try to provide a half-way reasonable model of how
     * a pool behaves.
     */
    private void updateSpace()
    {
        // People add precious data with fixed likelihood that is mostly the same size.
        if (_rnd.nextFloat() < PRECIOUS_ADD_LIKELIHOOD) {
            _spaceInfo.updatePrecious((long) (_spaceInfo
                    .getTotal() * PRECIOUS_ADD_DELTA * (1 + _rnd
                    .nextGaussian() * PRECIOUS_ADD_SPREAD)));
        }

        // As pool becomes more full, there's increased likelihood that some files are deleted
        double freeFrac = _spaceInfo.getFree() / (double)_spaceInfo.getTotal();
        if (_rnd.nextDouble() > PRECIOUS_DEL_SUPPRESS*Math.pow(freeFrac, 5)) {
                long size = (long) (_spaceInfo.getPrecious() * PRECIOUS_DEL_DELTA * (1 + _rnd.nextGaussian() * PRECIOUS_DEL_SPREAD));
                _spaceInfo.updatePrecious(-size);
        }

        // Likewise with removable, add some data, taken from a Zipf-like distribution.
        if (_rnd.nextFloat() < REMOVABLE_ADD_LIKELIHOOD) {
                long size = (long) (_spaceInfo.getTotal()*REMOVABLE_ADD_MAX_FRAC*Math.exp(-_rnd.nextDouble()/REMOVABLE_ADD_EXP));
                _spaceInfo.updateRemovable(size);
        }

        // Again, people increasingly likely to delete files when space becomes full.
        if (_rnd.nextDouble() > REMOVABLE_DEL_SUPPRESS*Math.pow(freeFrac, 5)) {
                long size = (long) (_spaceInfo.getPrecious() * REMOVABLE_DEL_DELTA * (1 + _rnd.nextGaussian() * REMOVABLE_DEL_SPREAD));
                _spaceInfo.updatePrecious(-size);
        }
    }

    @Override
    public String toString()
    {
        return this.getClass().getSimpleName() + "[" + _poolName + "]";
    }
}
