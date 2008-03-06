package org.dcache.services.info.base;

import org.apache.log4j.Logger;
import java.util.*;


/**
 * This singleton class provides a (best-effort) complete representation of
 * dCache instance's current state.
 * <p>
 * It receives fresh information through the updateState() method, which
 * accepts a StateUpdate object.  StateUpdate objects are created
 * by classes in the org.dcache.services.info.gathers package, either
 * synchronously (see DataGatheringScheduler), or by dCache messages received
 * asynchronously (see MessageHandlerChain).
 * <p>
 * There is preliminary support for triggering activity on state changes through
 * StateWatcher interface.  Some class may use this to publish aggregated (or otherwise,
 * derived) data in a timely fashion (see org.dcache.services.info.secondaryInfoProvider
 * package).  Other classes may use StateWatcher interface to trigger external events,
 * although future work may include adding support for asynchronous event-based monitoring
 * that would interface with, but remain seperate from this state.
 * <p>
 * The State object allows a visitor pattern, through the StateVisitor interface.  The
 * principle usage of this is to serialise the current content (see classes under
 * org.dcache.services.info.serialisation package), but some synchronous classes also
 * use this to build lists from dCache current state (e.g., to send a message requesting
 * data to each currently known pool).
 * 
 * @author Paul Millar <paul.millar@desy.de>
 */
public class State {

	/**
	 *  Constants used for persistent metadata
	 */
	public static final String METADATA_BRANCH_CLASS_KEY  = "branch";
	public static final String METADATA_BRANCH_IDNAME_KEY = "id";
	
	private static Logger _log = Logger.getLogger(State.class);
	
	/** The singleton instance of State */
	private static State _instance = null;

	/**
	 * Discover the single instance of State
	 * @return the State object.
	 */
	public static State getInstance() {
		if( _instance == null)
			_instance = new State();
		return _instance;
	}
	
	
	/**
	 *  Class member variables...
	 */

	/** Our current state */
	private StateComposite _state;
	
	/** All registered StateWatchers */
	private Collection<StateWatcher> _watchers = new LinkedList<StateWatcher>();

	/** Our read/write lock */
	private StateMonitor _monitor = new StateMonitor();
	
	/** Our list of pending Updates */
	private Stack<StateUpdate> _pendingUpdates;
	
	/**
	 *  The read/write monitor for this state. This controls access to the State
	 *  information.
	 * <p>
	 *  The monitor allows multiple concurrent readers, so reading state
	 *  will not stop other threads from reading the state concurrently.
	 *  However, a thread with a reader-lock will block any threads attempting
	 *  to update dCache state.  Such a thread should release the lock
	 *  as soon as it is no longer needed.
	 * <p>
	 *  This monitor prioritises writing activity over reading activity.
	 *  Specifically, the <code>startReading()</code> method will block
	 *  if a thread has a writer-lock (that is, it is currently writing
	 *  new values into dCache state) or if there are writers waiting to
	 *  update dCache state. the <code>startWriting()</code> method will
	 *  block while any thread holds a reading lock.
	 */
	private class StateMonitor {
		
		private int _readerCount = 0;
		private int _waitingWriterCount = 0;
		private Thread _activeWriter = null;
		
		/**
		 * Obtain a reader-lock.  A reader-lock guarentees that reading from
		 * dCache state will yield valid and consistant results whilst the
		 * lock is held.  When the lock is released, this guarentee is lost. 
		 * <p>
		 * The reader thread may be interrupted whilst it is waiting for
		 * a reader-lock.  If this happens, an InterruptedException is thrown.
		 * <p>
		 * A reader must <i>always</i> release the reader-lock.  One way of
		 * guarenteeing this is illustrated below:
		 * 
		 * <pre>
		 *     try {
		 *         startReading();
		 *         // read state...
		 *     } finally {
		 *         stopReading();
		 *     }
		 * </pre>
		 * @throws InterruptedException
		 */
		protected synchronized void startReading() throws InterruptedException {
			if( _log.isDebugEnabled()) {
				_log.debug("Thread " + Thread.currentThread().getName() + " requesting read lock.");
			}
			
			while( _activeWriter != null || _waitingWriterCount > 0)
				wait();
			
			_readerCount++;
			
			if( _log.isDebugEnabled()) {
				_log.debug("Thread " + Thread.currentThread().getName() + " has read lock.");
			}
		}
		
		/**
		 * Release a reader-lock.  The State allows multiple concurrent readers.
		 * However, reading blolcks writing, so this method must <i>always</i> be
		 * called after each <code>startReading()</code> to allow writing to occure.
		 * @throws InterruptedException
		 */
		protected synchronized void stopReading() {
			if( _log.isDebugEnabled()) {
				_log.debug("Thread " + Thread.currentThread().getName() + " releasing read lock.");
			}
			_readerCount--;
			
			if( _readerCount == 0)
				notifyAll();
		}
		
		/**
		 * Obtain a writing lock.  If there is reading underway, this method will
		 * block until reading activity has finished.  Writing is prioriatised over
		 * reading, so no new reading will be allowed until writing has completed.
		 * The writing lock prevents any reading from occuring.  Because of this
		 * caller must <i>always</i> call the <code>stopWriting()</code> method.
		 * @throws InterruptedException
		 */
		protected synchronized void startWriting() throws InterruptedException {
			
			if( _log.isDebugEnabled()) {
				_log.debug("Thread " + Thread.currentThread().getName() + " requesting write lock.");
			}

			if( _readerCount > 0) {
				_waitingWriterCount++;
				while( _readerCount > 0 || _activeWriter != null)
					wait();
				_waitingWriterCount--;
			}

			// Make a note of which thread owns the writing lock
			_activeWriter = Thread.currentThread();

			if( _log.isDebugEnabled()) {
				_log.debug("Thread " + Thread.currentThread().getName() + " obtained write lock.");
			}
		}
		
		/**
		 * Release the writing lock.  This allows further writing or (if there is
		 * none) subsequent reading activity to take place.
		 * @throws InterruptedException
		 */
		protected synchronized void stopWriting() {
			_activeWriter = null;
			notifyAll();

			if( _log.isDebugEnabled()) {
				_log.debug("Thread " + Thread.currentThread().getName() + " released write lock.");
			}
		}
		
		/**
		 * Check whether the current thread has already obtained a writing lock.
 		 * @return true if the current thread has the writing lock, false otherwise.
		 */
		protected synchronized boolean haveWritingLock() {
			return _activeWriter != null ? _activeWriter.equals( Thread.currentThread()) : false;
		}
	}

	
	/**
	 * Our private State constructor.
	 */
	private State() {

		// Build our persistent State metadata tree, with default contents
		StatePersistentMetadata metadata = new StatePersistentMetadata();		
		metadata.addDefault();

		// Build our top-level immortal StateComposite.
		_state = new StateComposite( metadata);
		
		// Our stack of pending updates ...
		_pendingUpdates = new Stack<StateUpdate>();		
	}

	
	/**
	 * Trigger the update of dCache state.
	 * @param update the set of changes that should be made.
	 */
	public void updateState( StateUpdate update) {
		_pendingUpdates.push(update);
		StateMaintainer.getInstance().wakeUp();
	}
	
	
	/**
	 * Discover when next to purge metrics from the dCache state.
	 * @return the Date when a metric or branch will next need to be expired.
	 */
	protected Date getEarliestMetricExpiryDate() {
		return _state.getEarliestChildExpiryDate();
	}
	
	
	
	/**
	 * Discover whether there are pending StateUpdates on the todo update stack.
	 * @return true if there are updates to be done, false otherwise.
	 */
	protected int countPendingUpdates() {
		return _pendingUpdates.size();
	}
	
	/**
	 * Update the currently stored state values with new primary
	 * values.  This may trigger Secondary Information Providers to update
	 * the information they provide.
	 * @return true if something was done, false if nothing needed doing.
	 */
	protected void processUpdateStack() {
		
		if( _pendingUpdates.empty())
			return;

		StateUpdate update = _pendingUpdates.pop();
		
		if( _log.isInfoEnabled())
			_log.info("Processing update with "+update.count()+" metric updates, "+_pendingUpdates.size()+" pending.");
		
		if( update.count() == 0) {
			_log.warn( "StateUpdate with zero updates encountered");
			return;
		}
		
		StateTransition transition = new StateTransition();

		try {
			_monitor.startReading();

			/**
			 *  Update our new StateTransition based on the StateUpdate.
			 */			
			try {
				update.updateTransition( _state, transition);
			} catch( BadStatePathException e) {
				_log.error("Error updating state:", e);
			}
			
			if( _log.isDebugEnabled())
				_log.debug(" Dump of pending StateTransition follows...\n\n" + transition.dumpContents());
			
			checkWatchers( transition);
			
		} catch( InterruptedException e) {
			Thread.currentThread().interrupt();
		} finally {
			_monitor.stopReading();
		}		

		applyTransition( transition);
	}
	

	/**
	 * Apply a StateTransition to dCache state.
	 * <p>
	 * This method will obtain a writer lock against dCache state.
	 * @param transition the StateTransition to apply.
	 */
	private void applyTransition( StateTransition transition) {
		try {
			_monitor.startWriting();

			_state.applyTransition(null, transition);
			
		} catch( InterruptedException e) {
			Thread.currentThread().interrupt();
		} finally {
			_monitor.stopWriting();
		}		
	}
	
	
	/**
	 * Given a StateTransition, check the registered StateWatchers and trigger those who's
	 * predicates have been triggered.  NB For consistency, the caller <i>must</i> hold a 
	 * reader-lock that was established when the StateTransition was obtained.  For this reason
	 * no locking is done within this method.
	 * @param transition The StateTransition to apply
	 */
	private void checkWatchers( StateTransition transition) {
		
		synchronized( _watchers) {
			for( StateWatcher thisWatcher : _watchers) {
				if( _log.isDebugEnabled())
					_log.debug( "checking watcher " + thisWatcher);
			
				boolean hasBeenTriggered = false;
				
				for( StatePathPredicate thisPredicate : thisWatcher.getPredicate()) {

					if( _log.isDebugEnabled())
						_log.debug( "checking predicate " + thisPredicate);

					try {
						hasBeenTriggered = _state.predicateHasBeenTriggered( null, thisPredicate, transition);
					}  catch( MetricStatePathException e) {
						_log.warn("problem querying trigger:", e);
					}
					
					if( hasBeenTriggered)
						break; // we only need one predicate to match, so quit early. 
				}

				if( hasBeenTriggered) {
					if( _log.isInfoEnabled())
						_log.info("triggering watcher " + thisWatcher);
					thisWatcher.trigger( transition);
					break;
				}
			}
		}		
	}
		
	
	/**
	 * Add a watcher to the Collection of Secondary Information
	 * Provider plug-ins.
	 * @param watcher: the Secondary Information Provider plug-in.
	 */
	public void addStateWatcher( StateWatcher watcher) {
		synchronized( _watchers) {
			_watchers.add(watcher);
		}
	}
	
	/**
	 * Remove a specific Secondary Information Provider.
	 * @param watcher: the Secondary Information Provider to remove.
	 */
	public void removeStateWatcher( StateWatcher watcher) {
		synchronized( _watchers) {
			_watchers.remove(watcher);
		}
	}
	
	/**
	 * Return a String containing a list of all state watchers.
	 * @param prefix a String to prefix each line of the output
	 * @return the list of watchers.
	 */
	public String[] listStateWatcher() {		
		String[] watchers;
		
		synchronized( _watchers) {
			watchers = new String[_watchers.size()];
			
			if( !_watchers.isEmpty()) {
				int i=0;
				for( StateWatcher thisWatcher : _watchers)
					watchers [i++] = thisWatcher.toString();
			}
		}
		
		return watchers;
	}
	
	
	/**
	 *   Check all StateValues within dCache State to see if any have elapsed.
	 */
	protected synchronized void removeExpiredMetrics() {
		
		// A quick check before obtaining the lock
		Date expDate = getEarliestMetricExpiryDate();
		
		if( expDate == null || expDate.after( new Date()))
			return;
		
		_log.info( "Building StateTransition for expired StateComponents");
		StateTransition transition = new StateTransition();
		
		try {
			_monitor.startReading();
			
			_state.buildRemovalTransition( null, transition, false);
			
			if( _log.isDebugEnabled())
				_log.debug(" Dump of pending StateTransition follows...\n\n" + transition.dumpContents());
			
			checkWatchers( transition);
			
		} catch( InterruptedException e) {
			
			Thread.currentThread().interrupt();
			
		} finally {
			_monitor.stopReading();
		}
		
		applyTransition( transition);
	}
	
	
	
	/**
	 * Allow an arbitrary algorithm to visit our state, but selecting a subset of
	 * all available values.
	 * @param visitor the algorithm that wishes to visit our current state
	 * @param predicate the selection criteria
	 */
	public void visitState( StateVisitor visitor, StatePath start) {
		try {
			_monitor.startReading();
			
			_state.acceptVisitor( null, start, visitor);
		} catch( InterruptedException e) {
			Thread.currentThread().interrupt();
		} finally {
			_monitor.stopReading();
		}					
	}
	
	
	/**
	 * Allow an arbitrary algorithm to visit what dCache state would look like <i>after</i> a
	 * StateTransition has been applied.
	 * @param transition The StateTransition to consider. 
	 * @param visitor the algorithm to apply
	 * @param start the "starting" point within the dCache state
	 */
	public void visitState( StateTransition transition, StateVisitor visitor, StatePath start) {
		try {
			_monitor.startReading();
			
			_state.acceptVisitor( transition, null, start, visitor);
		} catch( InterruptedException e) {
			Thread.currentThread().interrupt();
		} finally {
			_monitor.stopReading();
		}		
	}


}
