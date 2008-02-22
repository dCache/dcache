package org.dcache.services.info.base;

import java.util.*;

import org.dcache.services.info.*;


/**
 * This singleton class provides a (best-effort) complete representation of
 * dCache instance's current state.
 * 
 * It receives fresh information through the updateState() method, which
 * accepts a StateUpdate object.  StateUpdate objects are created
 * by classes in the org.dcache.services.info.gathers package, either
 * synchronously (see DataGatheringScheduler), or by dCache messages received
 * asynchronously (see MessageHandlerChain).
 * 
 * There is preliminary support for triggering activity on state changes through
 * StateWatcher interface.  Some class may use this to publish aggregated (or otherwise,
 * derived) data in a timely fashion (see org.dcache.services.info.secondaryInfoProvider
 * package).  Other classes may use StateWatcher interface to trigger external events,
 * although future work may include adding support for asynchronous event-based monitoring
 * that would interface with, but remain seperate from this state.
 * 
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
	
	/** The singleton instance of State */
	private static final StatePath _topStatePath = new StatePath( "dCache");
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
	 *  update dCache state.
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
			while( _activeWriter != null || _waitingWriterCount > 0)
				wait();
			
			_readerCount++;
		}
		
		/**
		 * Release a reader-lock.  The State allows multiple concurrent readers.
		 * However, reading blolcks writing, so this method must <i>always</i> be
		 * called after each <code>startReading()</code> to allow writing to occure.
		 * @throws InterruptedException
		 */
		protected synchronized void stopReading() {
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
			
			if( _readerCount > 0) {
				_waitingWriterCount++;
				while( _readerCount > 0 || _activeWriter != null)
					wait();
				_waitingWriterCount--;
			}

			// Make a note of which thread owns the writing lock
			_activeWriter = Thread.currentThread();
		}
		
		/**
		 * Release the writing lock.  This allows further writing or (if there is
		 * none) subsequent reading activity to take place.
		 * @throws InterruptedException
		 */
		protected synchronized void stopWriting() {
			_activeWriter = null;
			notifyAll();
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
	}

	
	/**
	 * Update the currently stored state values with new primary
	 * values.  This may trigger Secondary Information Providers to update
	 * the information they provide.
	 * @param update: the set of new state variables
	 */
	public void updateState( StateUpdate primaryUpdate) throws BadStatePathException {
		
		Map<StateWatcher,StateUpdate> interested = new HashMap<StateWatcher,StateUpdate>();
		BadStatePathException badStatePath = null;

		/**
		 *  Which StateWatchers, if any, are interested?
		 */
		for( StateWatcher thisWatcher : _watchers)
			if( primaryUpdate.watcherIsInterested( thisWatcher))
				interested.put(thisWatcher, null);
		
		/**
		 * Inform StateWatchers of update, solicit further StateUpdate information.  
		 */
		try {
			_monitor.startReading();
			
			for( StateWatcher thisWatcher : interested.keySet()) {			
				StateUpdate watcherUpdate = thisWatcher.evaluate( new StateUpdate( primaryUpdate));
				interested.put( thisWatcher, watcherUpdate);
			}
		} catch( InterruptedException e) {
			Thread.currentThread().interrupt();
		} finally {
			_monitor.stopReading();
		}

		try {
			_monitor.startWriting();
			
			/**
			 *  Write SIP updates, if any; we capture exceptions to allow the primary update to
			 *  proceed.
			 */			
			try {
				for( StateUpdate sipUpdate : interested.values())
					if( sipUpdate != null)
						updateStateValues( sipUpdate);
			} catch( BadStatePathException e) {
				badStatePath = e;
			}
			
			try {
				updateStateValues( primaryUpdate);
			} catch( BadStatePathException e) {
				badStatePath = e;
			}
			
			removeExpiredValues();
		} catch( InterruptedException e) {
			Thread.currentThread().interrupt();
		} finally {
			_monitor.stopWriting();
		}		
		
		if( badStatePath != null)
			throw badStatePath;
	}
	
	
	/**
	 * Update State values based on supplied StateUpdate information.
	 * @param update the StateUpdate information to apply.
	 */
	private synchronized void updateStateValues( StateUpdate update) throws BadStatePathException {		
		update.updateStateUnderComposite( _state);
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
	 * Check all StateValues within dCache State to see if any have elapsed.
	 */
	private synchronized void removeExpiredValues() {
		
		// A quick check before obtaining the writing lock
		Date expDate = _state.getEarliestChildExpiryDate();
		if( expDate == null || expDate.after( new Date()))
			return;
		
		boolean locallyObtainedLock = false;

		try {			
			if( !_monitor.haveWritingLock()) {
				_monitor.startWriting();
				locallyObtainedLock = true;
			}

			_state.hasExpired(); // Trigger cascade
			
		} catch( InterruptedException e) {
			Thread.currentThread().interrupt();
		} finally {
			if( locallyObtainedLock)
				_monitor.stopWriting();
		}
	}
	
	
	
	/**
	 * Allow an arbitrary algorithm to visit our state, but selecting a subset of
	 * all available values.
	 * @param visitor the algorithm that wishes to visit our current state
	 * @param predicate the selection criteria
	 */
	public void visitState( StateVisitor visitor, StatePath start) {
		removeExpiredValues();

		try {
			_monitor.startReading();
			
			_state.acceptVisitor( _topStatePath, start, visitor);
		} catch( InterruptedException e) {
			Thread.currentThread().interrupt();
		} finally {
			_monitor.stopReading();
		}					
	}
	

}
