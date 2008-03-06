package org.dcache.services.info.gathers;

import java.util.Random;
import org.dcache.services.info.stateInfo.*;
import org.dcache.services.info.base.*;
import java.util.Date;


/**
 * The PseudoPoolDga provides a stream of fake data about a fake pool.  It
 * is used to introduce load on the info state system and watchers.  This
 * DGA will also register itself as a member of the <code>default</code>
 * Poolgroup. 
 * <p>
 * To use this class, add a line like:
 * <pre>
 *        // Assuming 30s between pool update msgs, 100ms => 300 pools installed
 *        addActivity( new PseudoPoolDga("pseudo_1", 100, 40));
 * </pre>
 * to the <code>DataGatheringScheduler.addDefaultActivity()</code> method.
 * <p>
 * This class provides a very rough-and-ready simulation of data activity, 
 * with files being added and deleted at random intervals. 
 * 
 * @author Paul Millar <paul.millar@desy.de>
 */
public class PseudoPoolDga implements Schedulable {

	/** Max number std.dev. to allow in delay */
	static final private int NUM_SD = 4;
	/** The safety margin we should ensure for metrics, in seconds */  
	static final private int SAFETY_MARGIN = 2;
	static final private long BYTES_IN_GIBIBYTE = 1024*1024*1024;
	
	static final private StatePath DEFAULT_POOLGROUP_MEMBERSHIP = StatePath.parsePath( "poolgroups.default.pools");
		
	static final double PRECIOUS_ADD_LIKELIHOOD = 0.5;
	/** Fraction of total space to add, on average */
	static final double PRECIOUS_ADD_DELTA = 0.01;
	/** std. dev. of space added, as fraction of avr */
	static final double PRECIOUS_ADD_SPREAD = 0.01;
	static final double PRECIOUS_DEL_DELTA = 0.1;
	static final double PRECIOUS_DEL_SPREAD = 0.01;
	static final double PRECIOUS_DEL_SUPPRESS = 0.01;

	/** Likelihood of adding files each itr. */
	static final double REMOVABLE_ADD_LIKELIHOOD = 0.8;
	/** Frac. of total space that is max added in Zipf */
	static final double REMOVABLE_ADD_MAX_FRAC = 0.03;
	/** How fast the Zipf falls off, controls the spread of files */
	static final double REMOVABLE_ADD_EXP = 0.05;
	/** When removing, what fraction of removable is deleted*/
	static final double REMOVABLE_DEL_DELTA = 0.3;
	/** The std dev. of the removed amount, from  */
	static final double REMOVABLE_DEL_SPREAD = 0.05;
	/** Supress how often files are removed  */
	static final double REMOVABLE_DEL_SUPPRESS = 0.01;

	private String _poolName;
	private SpaceInfo _spaceInfo;
	private StatePath _ourSpacePath;
	private StatePath _ourPgMembership;
	/** Average time between successive calls, in milliseconds */	
	private int _delay; 
	/** Std dev. for Normal spread of successive calls */
	private int _spread;
	/** Lifetime of metrics, in seconds */
	private long _metricLifetime;
	/** Maximum delay between successive queries, in milliseconds */
	private long _maxDelay;
	private Random _rnd = new Random();
	
	/**
	 * Create a new fake pool, injecting information roughly periodically
	 * (5% spread) and a random walk in space usage.
	 * @param poolName the name of this pool
	 * @param updatePeriod the average update period, in milliseconds
	 * @param capacity of this pool, in GiB
	 */
	public PseudoPoolDga( String poolName, int updatePeriod, long capacity) {
		
		_poolName = poolName;
		
		StatePath pools = new StatePath( "pools");
		_ourSpacePath = pools.newChild(poolName).newChild("space");
		
		// Parameters for how often metrics are updated
		_delay = updatePeriod;
		_spread = (int) (0.05 * updatePeriod);
		
		_maxDelay = _delay + NUM_SD * _spread;		
		_metricLifetime = _maxDelay/1000 + SAFETY_MARGIN;
		
		_ourPgMembership = DEFAULT_POOLGROUP_MEMBERSHIP.newChild( poolName);
		
		// Initially, completely empty.
		_spaceInfo = new SpaceInfo(capacity * BYTES_IN_GIBIBYTE);
	}

	// Roughly periodic: Normal with mean: _duration, SD: _spread
	public Date shouldNextBeTriggered() {
		long thisDelay = _delay + (long) (_rnd.nextGaussian() * _spread);
		
		if( thisDelay > _maxDelay)
			thisDelay = _maxDelay;
		
		return new Date( System.currentTimeMillis() + thisDelay);
	}

	
	/**
	 *  When we've been triggered.
	 */
	public void trigger() {
		
		updateSpace();
		
		StateUpdate update = new StateUpdate();
		
		// Renew our membership of default poolgroup
		update.appendUpdate( _ourPgMembership, new StateComposite( _metricLifetime));
		
		_spaceInfo.addMetrics(update, _ourSpacePath, _metricLifetime);
		
		State.getInstance().updateState(update);
	}
	
	/**
	 * Update our space information.  We try to provide a half-way reasonable model of how
	 * a pool behaves.
	 */
	private void updateSpace() {
		
		// People add precious data with fixed likelihood that is mostly the same size.
		if( _rnd.nextFloat() < PRECIOUS_ADD_LIKELIHOOD)
			_spaceInfo.updatePrecious( (long) (_spaceInfo.getTotal() * PRECIOUS_ADD_DELTA * (1 + _rnd.nextGaussian() * PRECIOUS_ADD_SPREAD)));

		// As pool becomes more full, there's increased likelihood that some files are deleted
		double freeFrac = _spaceInfo.getFree() / (double)_spaceInfo.getTotal();
		if( _rnd.nextDouble() > PRECIOUS_DEL_SUPPRESS*Math.pow( freeFrac, 5)) {
			long size = (long) (_spaceInfo.getPrecious() * PRECIOUS_DEL_DELTA * (1 + _rnd.nextGaussian() * PRECIOUS_DEL_SPREAD));
			_spaceInfo.updatePrecious( -size);
		}

		// Likewise with removable, add some data, taken from a Zapf-like distribution.
		if( _rnd.nextFloat() < REMOVABLE_ADD_LIKELIHOOD) {
			long size = (long) (_spaceInfo.getTotal()*REMOVABLE_ADD_MAX_FRAC*Math.exp(-_rnd.nextDouble()/REMOVABLE_ADD_EXP));
			_spaceInfo.updateRemovable(size);
		}

		// Again, people increasingly likely to delete files when space becomes full.
		if( _rnd.nextDouble() > REMOVABLE_DEL_SUPPRESS*Math.pow( freeFrac, 5)) {
			long size = (long) (_spaceInfo.getPrecious() * REMOVABLE_DEL_DELTA * (1 + _rnd.nextGaussian() * REMOVABLE_DEL_SPREAD));
			_spaceInfo.updatePrecious( -size);
		}
	}
	

	public String toString()
	{
		return this.getClass().getSimpleName() + "[" + _poolName + "]";
	}


}
