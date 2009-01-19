package org.dcache.services.info.base;

import java.lang.Runnable;
import java.util.Date;

import org.apache.log4j.Logger;

/**
 * The StateMaintainer is a thread that maintains the state.  It wakes up when updates to
 * the dCache state are needed, based on the stack of pending StateUpdates.
 * 
 * @author Paul Millar <paul.millar@desy.de>
 */
public class StateMaintainer implements Runnable {
	
	private static final Logger _log = Logger.getLogger(StateMaintainer.class);
	
	private static StateMaintainer _instance = null;
	private static Thread _smThread;
	
	public static StateMaintainer getInstance() {
		if( _instance == null)
			createNewMaintainer();
		
		return _instance;
	}
	
	public static void createNewMaintainer() {
		_instance = new StateMaintainer();
		
		_smThread = new Thread( _instance, "StateMaintainer"); 
		_smThread.start();
	}

	volatile private boolean _isAwake = true;
	volatile private boolean _shouldQuit = false;
	
	public void run() {
		
		State state = State.getInstance();
		
		_log.info( "StateMaintainer thread has started");
	
		while( !_shouldQuit) {
			
			// check for delete data.
			_log.debug( "checking for expired metrics.");
			state.removeExpiredMetrics();

			// Process pending updates
			_log.debug( "checking for pending StateUpdates in update stack.");
			while( state.countPendingUpdates() > 0) {
				if( _log.isDebugEnabled())
					_log.debug( "Iterating, " + state.countPendingUpdates() + " remaining");
				state.processUpdateStack();
			}

			_log.debug( "Done ... going to sleep");
			
			sleep();
		}

	}
	

	/**
	 *   Wait to be woken up.  This wait may timeout if metrics are to
	 *   be deleted in the future.
	 */
	private void sleep() {
		long sleepTime;
		
		// calculate how long to sleep for
		Date nextExp = State.getInstance().getEarliestMetricExpiryDate();
		
		if( nextExp != null) {			
			Date now = new Date();
			
			if( nextExp.before(now))
				return;
		
			sleepTime = nextExp.getTime() - now.getTime(); 
		} else
			sleepTime = 0; // wait until notified.
		
		synchronized( this) {
			
			if( State.getInstance().countPendingUpdates() > 0) {
				_log.debug( "We have work to do, Batman, another " + State.getInstance().countPendingUpdates() +" updates");
				return;
			}

			if( _log.isDebugEnabled())
				_log.debug( "StateMaintainer is going to sleep for "+sleepTime+"ms");			
			
			_isAwake = false;
	
			try {
				this.wait( sleepTime);
			} catch( InterruptedException e) {
				_log.info( "StateMaintainer thread was interrupted.");
			}
			_isAwake = true;
			_log.info( "StateMaintainer awoke.");
		}
		
	}
	
	
	
	/**
	 *  Wakes up the StateMaintainer thread.  This forces the maintainer thread
	 *  to recalculate its activities, based on any new activity.
	 */
	protected void wakeUp() {
		
		synchronized( this) {
			if ( _isAwake) {
				_log.debug( "not waking StateMaintainer, already awake.");
			} else {
				_log.debug( "waking up StateMaintainer.");
				this.notify();
			}
		}
	}
	
	/**
	 * Instruct the StateMaintainer thread to shutdown.  Wait for this to happen.
	 */
	public void shutdown() {
		_shouldQuit = true;
		
		wakeUp();
		
		_log.debug( "waiting for StateMaintainer to finish...");
		
		try {
			_smThread.join();
		} catch ( InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}
	
}
