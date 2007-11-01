package org.dcache.pool.repository.v3;

import java.util.Date;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.dcache.pool.repository.StickyRecord;

import diskCacheV111.repository.CacheRepository;
import diskCacheV111.repository.CacheRepositoryEntry;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;


/**
 *
 * StickyInspector is a module, which keeps track of sticky files and
 * removed sticky flag as soon as sticky life time expired.
 *
 * @since 1.7.1
 *
 */



public class StickyInspector implements Runnable {


	private static class StickyFile implements Delayed {

		private final PnfsId _pnfsId;
		private final StickyRecord _stickyRecord;

		StickyFile(PnfsId pnfsId, StickyRecord stickyRecord) {
			_pnfsId = pnfsId;
			_stickyRecord = stickyRecord;
		}

		public long getDelay(TimeUnit unit) {
			return _stickyRecord.expire() - System.currentTimeMillis();
		}

		public int compareTo(Delayed o) {

			if( !( o instanceof StickyFile ) ) {
				throw new ClassCastException("Wrong object to compare");
			}

			StickyFile aStickyFile = (StickyFile)o;

			return (int)(record().expire() - aStickyFile.record().expire());
		}

		public StickyRecord record() {
			return _stickyRecord;
		}

		public PnfsId pnfsId() {
			return _pnfsId;
		}

		public String toString() {
			return _pnfsId.toString() + " " + _stickyRecord.owner() + " " + new Date(_stickyRecord.expire() );
		}
		
		public boolean equals(Object o) {
			
			if( !(o instanceof StickyFile) ) return false;
			
			StickyFile otherFile = (StickyFile)o;
			
			return otherFile._pnfsId.equals(_pnfsId) && otherFile._stickyRecord.equals(_stickyRecord);
			
		}
		
		public int hashCode() {
			return _pnfsId.hashCode();
		}
		
	}


	// new logger concept
	private static Logger _logRepository     = Logger.getLogger("logger.org.dcache.repository");
	private static Logger _logRepositoryDev = Logger.getLogger("logger.dev.org.dcache.repository");

	/**
	 *  are we done ?
	 */

	private final AtomicBoolean _isDone = new AtomicBoolean(false);

	/**
	 * did we already start ?
	 */
	private final AtomicBoolean _isStarted = new AtomicBoolean(false);
	/*
	 *  Queue of known sticky files with lifetime
	 */
	private final DelayQueue<StickyFile> _fileQueue = new DelayQueue<StickyFile>();

	/**
	 * repository
	 */
	private final CacheRepository _repository;


	public StickyInspector(CacheRepository repository) {
		_repository = repository;
	}


	/*
	 * yet another helper class
	 */
	private static class UnstickyHelper implements Runnable {

		private final CacheRepository _repository;
		private final StickyFile _stickyFile;

		UnstickyHelper( CacheRepository repository, StickyFile stickyFile ) {
			_repository = repository;
			_stickyFile = stickyFile;
		}

		public void run() {

			try {
				CacheRepositoryEntry repositoryEntry = 
                                    _repository.getEntry( _stickyFile.pnfsId() );
				repositoryEntry.setSticky(false, _stickyFile.record().owner(), _stickyFile.record().expire() );

			} catch (CacheException e) {
				// it's is not our responsibility to react on exceptions
			}
		}
	}


	/**
	 * add a new file to the list of files with sticky flags. Record is added only if
	 * lifetime is not infinite ( != -1 ) and dir not expired yet.
	 *
	 * @param pnfsId
	 * @param record
	 */

	public void add(PnfsId pnfsId, StickyRecord record) {

		/*
		 * record.expire() may return -1, which means infinite and
		 * we do not care about them. So check only for valid 'timed' records
		 */
		if( record.expire() > System.currentTimeMillis() ) {
			StickyFile stickyFile = new StickyFile(pnfsId, record);
			_fileQueue.put(stickyFile);
		}
	}


	/*
	 * worker threads, two of them
	 * TODO: do we need to configure it?
	 */

	private final ExecutorService _executor = Executors.newFixedThreadPool(2);


	public void run() {

		_isStarted.set(true);

		while(!_isDone.get() ) {

			try {
				StickyFile stickyFile = _fileQueue.take();
				if( _logRepository.isDebugEnabled() ) {
					_logRepository.debug("Unsticky for: " + stickyFile );
				}
				if( _repository != null ) {
					_executor.execute( new UnstickyHelper(_repository, stickyFile)	);
				}

			} catch (InterruptedException e) {
				_isDone.set(true);
			}
		}

	}



	/**
	 * test code
	 */

	public static void main(String[] args) {

		Logger logRepository     = Logger.getLogger("logger.org.dcache.repository");

		logRepository.setLevel( Level.ALL );

		StickyInspector inspector = new StickyInspector(null);

		inspector.add( new PnfsId("000000000000000000000001") , new StickyRecord("tigran", System.currentTimeMillis() + 2000));
		inspector.add( new PnfsId("000000000000000000000002") , new StickyRecord("tigran", System.currentTimeMillis() + 3000));
		inspector.add( new PnfsId("000000000000000000000003") , new StickyRecord("tigran", System.currentTimeMillis() + 7000));
		inspector.add( new PnfsId("000000000000000000000004") , new StickyRecord("tigran", System.currentTimeMillis() + 9000));
		inspector.add( new PnfsId("000000000000000000000005") , new StickyRecord("tigran", System.currentTimeMillis() + 10000));
		inspector.add( new PnfsId("000000000000000000000006") , new StickyRecord("tigran", System.currentTimeMillis() + 25000));

		new Thread(inspector, "StickyInspector").start();
		logRepository.debug("started");
	}
}
