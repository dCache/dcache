package org.dcache.pool.repository.v3.entry;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.dcache.pool.repository.v3.entry.state.Cached;
import org.dcache.pool.repository.v3.entry.state.ErrorState;
import org.dcache.pool.repository.v3.entry.state.FromClient;
import org.dcache.pool.repository.v3.entry.state.FromStore;
import org.dcache.pool.repository.v3.entry.state.Precious;
import org.dcache.pool.repository.v3.entry.state.Removed;
import org.dcache.pool.repository.v3.entry.state.Sticky;
import org.dcache.pool.repository.v3.entry.state.ToClient;
import org.dcache.pool.repository.v3.entry.state.ToStore;
import org.dcache.pool.repository.StickyRecord;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CacheRepositoryEntryState {


	// new logger concept
	private static Logger _logBussiness = Logger.getLogger("logger.org.dcache.repository");


	// format version
	private static final int FROMAT_VERSION_MAJOR = 3;
	private static final int FROMAT_VERSION_MINOR = 0;

	// possible states of entry in the repository

	private final Sticky     _sticky       = new Sticky();
	private final Precious   _precious     = new Precious(false);
	private final Cached     _cached       = new Cached(false);
	private final ToStore    _toStore      = new ToStore(false);
	private final ToClient   _toClient     = new ToClient(false);
	private final FromClient _fromClient   = new FromClient(false);
	private final FromStore  _fromStore    = new FromStore(false);
	private final ErrorState _error        = new ErrorState(false);
	private final Removed    _removed      = new Removed(false);


	// we are thread safe
	private final ReadWriteLock _stateLock = new ReentrantReadWriteLock();

	// data, control and SI- files locations
	private final File _controlFile;

	public CacheRepositoryEntryState(File controlFile) throws IOException {
		_controlFile = controlFile;

		// read state from file
		try {
			loadState();
		}catch( FileNotFoundException fnf) {
			/*
			 * it's not an error state.
			 */
		}

	}

	/**
	 * file is busy if there is a transfer in progress
	 * @return
	 */
	public boolean isBusy() {

		boolean ret = false;
		_stateLock.readLock().lock();
		try {
			ret =  _toStore.isSet() || _toClient.isSet() || _fromClient.isSet() || _fromStore.isSet() ;
		}finally{
			_stateLock.readLock().unlock();
		}

		return ret;
	}

	/**
	 *
	 * @return true if file not precious, not sticky and not used now
	 */

	public boolean canRemove() {

		boolean ret = false;
		_stateLock.readLock().lock();
		try {
			ret =  !( _precious.isSet() || isBusy() || _sticky.isSet() || _error.isSet() );
		}finally{
			_stateLock.readLock().unlock();
		}

		return ret;
	}

	/*
	 *
	 *  State transitions
	 *
	 */

	public void setSticky(String owner, long expire) throws IllegalStateException, IOException {


		_stateLock.writeLock().lock();
		try {
			if( _error.isSet() ) {
				throw new IllegalStateException("No state transition for files in error state");
			}

			// too late
			if( _removed.isSet() ) {
				throw new IllegalStateException("Entry in removed state");
			}

			// if sticky flag modified, make changes persistent
			if( _sticky.addRecord(owner, expire) ) {
				makeStatePercistent();
			}
		}finally{
			_stateLock.writeLock().unlock();
		}
	}


	public void cleanSticky(String owner, long expire) throws IllegalStateException, IOException {

		_stateLock.writeLock().lock();
		try {

			// too late
			if( _removed.isSet() ) {
				throw new IllegalStateException("Entry in removed state");
			}

			// if sticky flag modified, make changes persistent
			if( _sticky.removeRecord(owner, expire)) {
				makeStatePercistent();
			}
		}finally{
			_stateLock.writeLock().unlock();
		}
	}

	public void cleanBad() {

		_stateLock.writeLock().lock();
		try {

			// too late
			if( _removed.isSet() ) {
				throw new IllegalStateException("Entry in removed state");
			}

			_error.set(false);

		}finally{
			_stateLock.writeLock().unlock();
		}
	}

	public void setPrecious(boolean force) throws IllegalStateException, IOException {

		_stateLock.writeLock().lock();
		try {

			if( ! force && _error.isSet() ) {
				throw new IllegalStateException("No state transition for files in error state");
			}

			// too late
			if( _removed.isSet() ) {
				throw new IllegalStateException("Entry in removed state");
			}

			// precious file can't be in receiving state

			_fromClient.set(false);
			_fromStore.set(false);
			_precious.set(true);
			_cached.set(false);

			makeStatePercistent();
		}finally{
			_stateLock.writeLock().unlock();
		}

	}

	public void setCached() throws IllegalStateException, IOException {

		_stateLock.writeLock().lock();
		try {

			if( _error.isSet() ) {
				throw new IllegalStateException("No state transition for files in error state");
			}

			// too late
			if( _removed.isSet() ) {
				throw new IllegalStateException("Entry in removed state");
			}

			// cached file can't be in receiving state

			_fromClient.set(false);
			_fromStore.set(false);
			_cached.set(true);
			_precious.set(false);

			makeStatePercistent();
		}finally{
			_stateLock.writeLock().unlock();
		}
	}

	public void setFromClinet() throws IllegalStateException, IOException {

		_stateLock.writeLock().lock();
		try {

			if( _error.isSet() ) {
				throw new IllegalStateException("No state transition for files in error state");
			}

			// too late
			if( _removed.isSet() ) {
				throw new IllegalStateException("Entry in removed state");
			}

			// only 'clean' file allowed to be received
			if( _precious.isSet() || _cached.isSet() || _fromStore.isSet() ) {
				throw new IllegalStateException("File still transient");
			}

			_fromClient.set(true);
			makeStatePercistent();
		}finally{
			_stateLock.writeLock().unlock();
		}
	}


	public void setFromStore() throws IllegalStateException, IOException {

		_stateLock.writeLock().lock();
		try {

			if( _error.isSet() ) {
				throw new IllegalStateException("No state transition for files in error state");
			}

			// too late
			if( _removed.isSet() ) {
				throw new IllegalStateException("Entry in removed state");
			}

			// only 'clean' file allowed to be received
			if( _precious.isSet() || _cached.isSet() || _fromClient.isSet() ) {
				throw new IllegalStateException("File still transient");
			}

			_fromStore.set(true);
			makeStatePercistent();
		}finally{
			_stateLock.writeLock().unlock();
		}
	}



	public void setToClient() throws IllegalStateException, IOException {

		_stateLock.writeLock().lock();
		try {

			if( _error.isSet() ) {
				throw new IllegalStateException("No state transition for files in error state");
			}

			// too late
			if( _removed.isSet() ) {
				throw new IllegalStateException("Entry in removed state");
			}

			// only received files can be delivered to a client
			if( !(_precious.isSet() || _cached.isSet() ) ) {
				throw new IllegalStateException("File still transient");
			}

			_toClient.set(true);
		}finally{
			_stateLock.writeLock().unlock();
		}
	}

	public void setToStore() throws IllegalStateException, IOException {

		_stateLock.writeLock().lock();
		try {

			if( _error.isSet() ) {
				throw new IllegalStateException("No state transition for files in error state");
			}

			// too late
			if( _removed.isSet() ) {
				throw new IllegalStateException("Entry in removed state");
			}

			// only received precious files can be flushed to store
			if( !_precious.isSet() ) {
				throw new IllegalStateException("File still transient");
			}

			_toStore.set(true);
		}finally{
			_stateLock.writeLock().unlock();
		}
	}


	public void cleanToStore() throws IllegalStateException, IOException {
		_stateLock.writeLock().lock();
		try {

			// too late
			if( _removed.isSet() ) {
				throw new IllegalStateException("Entry in removed state");
			}

			_toStore.set(false);
		}finally{
			_stateLock.writeLock().unlock();
		}
	}


	public void setError() throws IllegalStateException, IOException {
		_stateLock.writeLock().lock();
		try {

			// too late
			if( _removed.isSet() ) {
				throw new IllegalStateException("Entry in removed state");
			}

			_error.set(true);
		}finally{
			_stateLock.writeLock().unlock();
		}
	}

	public void setRemoved() throws IllegalStateException, IOException {
		_stateLock.writeLock().lock();
		try {
			_removed.set(true);
		}finally{
			_stateLock.writeLock().unlock();
		}
	}

	/*
	 * State getters
	 */

	public boolean isError() {
		boolean ret = false;
		_stateLock.readLock().lock();
		try {
			ret = _error.isSet();
		}finally{
			_stateLock.readLock().unlock();
		}
		return ret;
	}

	public boolean isCached() {
		boolean ret = false;
		_stateLock.readLock().lock();
		try {
			ret = _cached.isSet();
		}finally{
			_stateLock.readLock().unlock();
		}
		return ret;

	}

	public boolean isPrecious() {
		boolean ret = false;
		_stateLock.readLock().lock();
		try {
			ret = _precious.isSet();
		}finally{
			_stateLock.readLock().unlock();
		}
		return ret;
	}


	/**
	 *
	 * @return true if file ready for clients (CACHED or PRECIOUS)
	 */
	public boolean isReady() {

		boolean ret = false;
		_stateLock.readLock().lock();
		try {
			ret = _precious.isSet() | _cached.isSet();
		}finally{
			_stateLock.readLock().unlock();
		}
		return ret;
	}

	public boolean isRecivingFromClient() {
		return _fromClient.isSet();
	}

	public boolean isRecivingFromStore() {
		boolean ret = false;
		_stateLock.readLock().lock();
		try {
			ret = _fromStore.isSet();
		}finally{
			_stateLock.readLock().unlock();
		}
		return ret;

	}

	public boolean isSendingToStore() {
		boolean ret = false;
		_stateLock.readLock().lock();
		try {
			ret = _toStore.isSet();
		}finally{
			_stateLock.readLock().unlock();
		}
		return ret;
	}


	public boolean isSticky() {
		boolean ret = false;
		_stateLock.readLock().lock();
		try {
			ret = _sticky.isSet();
		}finally{
			_stateLock.readLock().unlock();
		}
		return ret;
	}

	public boolean isRemoved() {
		boolean ret = false;
		_stateLock.readLock().lock();
		try {
			ret = _removed.isSet();
		}finally{
			_stateLock.readLock().unlock();
		}
		return ret;
	}
	/**
	 * store state in control file
	 * @throws IOException
	 */
	private void makeStatePercistent() throws IOException {

		//BufferedReader in = new BufferedReader( new FileReader(_controlFile) );
		BufferedWriter out = new BufferedWriter(new FileWriter(_controlFile, false) );
		try {

			// write repository version number

			out.write("# version 3.0"); out.newLine();
			String state = null;

			state = _precious.stringValue();
			if( state != null && state.length() > 0 ) {
				out.write(state); out.newLine();
			}


			state = _cached.stringValue();
			if( state != null && state.length() > 0 ) {
				out.write(state); out.newLine();
			}

			state = _fromClient.stringValue();
			if( state != null && state.length() > 0 ) {
				out.write(state); out.newLine();
			}

			state = _fromStore.stringValue();
			if( state != null && state.length() > 0 ) {
				out.write(state); out.newLine();
			}

			state = _sticky.stringValue();
			if( state != null && state.length() > 0 ) {
				out.write(state); out.newLine();
			}

			out.flush();

		}finally{
			out.close();
		}

	}


	private void loadState() throws IOException {


		BufferedReader in = null;

		List<String> lines = new ArrayList<String>();

		try {

			in = new BufferedReader( new FileReader(_controlFile) );

			boolean done = false;
			while(!done) {

				String line = in.readLine();
				if( line == null) {
					done = true;
					continue;
				}


				// ignore empty lines
				line = line.trim();
				if(line.length() == 0 ) {
					continue;
				}

				// a comment or version string

				if( line.startsWith("#") ) {

					Pattern p = Pattern.compile("#\\s+version\\s+[0-9]\\.[0-9]");
					Matcher m = p.matcher(line);

					// it's the version string
					if( m.matches() ) {
						String[] versionLine = line.split("\\s");
						String[] versionNumber = versionLine[2].split("\\.");

						int major = Integer.parseInt(versionNumber[0]);
						int minor = Integer.parseInt(versionNumber[1]);

						if( major > FROMAT_VERSION_MAJOR || minor != FROMAT_VERSION_MINOR ) {
							throw new IOException("control file format mismatch: supported <= "
									+ FROMAT_VERSION_MAJOR + "." + FROMAT_VERSION_MINOR + " found: " + versionLine[2]);
						}
					}

					continue;
				}

				lines.add(line);

			}

		}finally{
			if( in != null ) { in.close(); }
		}


		Iterator<String> stateIterator = lines.iterator();

		while( stateIterator.hasNext() ) {

			String state = stateIterator.next();

			if( state.equals("precious") ) {
				_precious.set(true);
				continue;
			}

			if( state.equals("cached") ) {
				_cached.set(true);
				continue;
			}

			if( state.equals("from_client") ) {
				_fromClient.set(true);
				continue;
			}

			if( state.equals("from_store") ) {
				_fromStore.set(true);
				continue;
			}

			/*
			 * backward compatibility
			 */

			if( state.equals("receiving.store") ) {
				_fromStore.set(true);
				continue;
			}

			if( state.equals("receiving.cient") ) {
				_fromClient.set(true);
				continue;
			}

			// in case of some one fixed the spelling
			if( state.equals("receiving.client") ) {
				_fromClient.set(true);
				continue;
			}

			// FORMAT: sticky:owner:exipire

			if( state.startsWith("sticky") ) {

				String[] stickyOptions = state.split(":");

				String owner = "repository";
				long expire = -1;

				switch ( stickyOptions.length ) {
				case 1:
					// old style
					owner = "system";
					expire = -1;
					break;
				case 2:
					// only owner defined
					owner = stickyOptions[1];
					expire = -1;
					break;
				case 3:
					owner = stickyOptions[1];
					try {
						expire = Long.parseLong(stickyOptions[2]);
					}catch(NumberFormatException nfe) {
						// bad number
						_error.set(true);
					}

					break;
				default:
					_logBussiness.info("Unknow number of arguments in " +_controlFile.getPath() + " [" +state+"]");
					_error.set(true);
				}

				_sticky.addRecord(owner, expire);

				continue;
			}


			// if none of knows states, then it's BAD state
			_logBussiness.error("Invalid state [" + state + "] for entry " + _controlFile);
			_error.set(true);
			break;

		}
	}


	public List<StickyRecord> stickyRecords() {

		_stateLock.readLock().lock();
		try{
			return _sticky.records();
		}finally{
			_stateLock.readLock().unlock();
		}
	}


	public String toString() {
        StringBuilder sb = new StringBuilder() ;

		_stateLock.readLock().lock();
		try {
	        sb.append( _cached.isSet() && !_precious.isSet()   ? "C" : "-" ) ;
	        sb.append( _precious.isSet()   ? "P" : "-" ) ;
	        sb.append( _fromClient.isSet() ? "C" : "-" ) ;
	        sb.append( _fromStore.isSet()  ? "S" : "-" ) ;
	        sb.append( _toClient.isSet()   ? "c" : "-" ) ;
	        sb.append( _toStore.isSet()    ? "s" : "-" ) ;
	        sb.append( _removed.isSet()    ? "R" : "-" ) ; // REMOVED
	        sb.append(                             "-" ) ; // DESTROYED
	        sb.append( _sticky.isSet()     ? "X" : "-" ) ;
	        sb.append( _error.isSet()      ? "E" : "-" ) ;
		}finally{
			_stateLock.readLock().unlock();
		}

        return sb.toString() ;
	}


	/*
	 * test code
	 */

	public static void main(String[] args) {


		String repositoryBase = "/tmp";

		File controlFile = new File(repositoryBase, "CacheRepositoryEntry-Test.control");

		try {


			CacheRepositoryEntryState entry = new CacheRepositoryEntryState(controlFile);
			System.out.println(entry);


			entry.setCached();
			System.out.println(entry);
		} catch (IllegalStateException e) {

			e.printStackTrace();
		} catch (IOException e) {

			e.printStackTrace();
		}

	}

}
