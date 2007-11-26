package org.dcache.pool.repository.v3;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;
import org.dcache.pool.repository.AbstractCacheRepository;
import org.dcache.pool.repository.EventType;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.pool.repository.v3.entry.CacheRepositoryEntryV3Impl;

import diskCacheV111.repository.CacheRepository;
import diskCacheV111.repository.CacheRepositoryEntry;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileInCacheException;
import diskCacheV111.util.FileNotInCacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.event.CacheRepositoryEvent;
import dmg.util.Logable;

public class CacheRepositoryV3 extends AbstractCacheRepository {


	// new logger concept
	private static Logger _logRepository = Logger.getLogger("logger.org.dcache.repository");
	private static Logger _logRepositoryDev = Logger.getLogger("logger.dev.org.dcache.repository");

	/**
	 *     all entries
	 */
	private final ReadWriteLock _repositoryOperationLock = new ReentrantReadWriteLock();
	private final Map<PnfsId, CacheRepositoryEntry > _allEntries = new HashMap<PnfsId, CacheRepositoryEntry>();


	
	/**
	 * space reservation
	 */
	private final File   _spaceReservation ;
	
	/**
	 * file layout with in pool
	 */
	private final RepositoryTree _repositoryTree;

	/**
	 * unsticky file sticky flag if it's expired. StickyInspector thread started
	 * as soon as inventory is finished.
	 *
	 */
	private final StickyInspector _stickyInspector;
	private final Thread _stickyInspectorThread;

	public CacheRepositoryV3( File baseDir ) throws IOException {

		_repositoryTree = new FlatDirectoryTree(baseDir);
		_spaceReservation = _repositoryTree.getControlFile("SPACE_RESERVATION");

		
		_stickyInspector = new StickyInspector(this);
		_stickyInspectorThread = new Thread(_stickyInspector, "StickyInspectorThread");
	}

	public boolean contains(PnfsId pnfsId) {
		_repositoryOperationLock.readLock().lock();
		boolean contains = false;

		try {
			contains = _allEntries.containsKey(pnfsId);
		}finally{
			_repositoryOperationLock.readLock().unlock();
		}

		return contains;
	}

	public CacheRepositoryEntry createEntry(PnfsId pnfsId)
			throws CacheException {


		if(_logRepository.isInfoEnabled() ) {
			_logRepository.info("create new entry for: " + pnfsId.toString() );
		}

		String name = pnfsId.toString();

		File dataFile =    _repositoryTree.getDataFile(name);
		File controlFile = _repositoryTree.getControlFile(name);
		File siFile =      _repositoryTree.getSiFile(name);


		if( dataFile.exists() || controlFile.exists() || siFile.exists() ) {
			_logRepository.fatal("Entry exists: " + pnfsId);
			throw new
			CacheException( 203,"Entry already exists (fs) : "+pnfsId ) ;
		}


		CacheRepositoryEntry newEntry = null;
		try {
			newEntry = new CacheRepositoryEntryV3Impl(this, pnfsId, controlFile, dataFile, siFile);
		} catch (IOException e) {
			_logRepository.fatal("Low Level Exc : ",e);
			throw new
	           CacheException( ERROR_IO_DISK , "Low Level Exc : "+e) ;
		}


		_repositoryOperationLock.writeLock().lock();
		try {
			if(_allEntries.containsKey(pnfsId) ) {
				_logRepository.error("create for existing entry: " + pnfsId.toString() );
				throw new
		           FileInCacheException( "Entry already exists (mem) : "+pnfsId.toString() ) ;
			}
			_allEntries.put(pnfsId, newEntry);
		}finally{
			_repositoryOperationLock.writeLock().unlock();
		}

		return newEntry;
	}



	/**
	 * @return CacheRepositoryEntry on pnfsid, exclude in removed state
	 * @throw FileNotInCacheException in case of file is not in repository or in removed state
	 */
	public CacheRepositoryEntry getEntry(PnfsId pnfsId) throws CacheException {

		CacheRepositoryEntry entry = getGenericEntry(pnfsId);

		if( entry.isRemoved() ) {
			throw new FileNotInCacheException("Entry not in repository ( removed ): "+pnfsId);
		}

		return entry;
	}

	/**
	 * @return CacheRepositoryEntry on pnfsid, including in removed state
	 * @throw FileNotInCacheException in case of file is not in repository
	 */
	public CacheRepositoryEntry getGenericEntry(PnfsId pnfsId)
			throws CacheException {

		CacheRepositoryEntry entry = null;
		_repositoryOperationLock.readLock().lock();
		try {
			entry = _allEntries.get(pnfsId);
		}finally{
			_repositoryOperationLock.readLock().unlock();
		}

		if(entry == null) {
			throw new FileNotInCacheException("Entry not in repository : "+pnfsId);
		}

		return entry;

	}


	public List<PnfsId> getValidPnfsidList() {

		/*
		 * create a copy of the map and work on it
		 */

		Map<PnfsId, CacheRepositoryEntry> allEntries = null;
		_repositoryOperationLock.readLock().lock();
		try {
			allEntries = new HashMap<PnfsId, CacheRepositoryEntry>(_allEntries);
		}finally{
			_repositoryOperationLock.readLock().unlock();
		}

		List<PnfsId> validIds = new ArrayList<PnfsId>();
		Set<Map.Entry<PnfsId,CacheRepositoryEntry>> allEntriesSet = allEntries.entrySet();
		for(Map.Entry<PnfsId,CacheRepositoryEntry> mapEntry: allEntriesSet) {
			CacheRepositoryEntry entry = mapEntry.getValue();
			try {
				if( entry.isCached() || entry.isPrecious() ) {
					validIds.add( mapEntry.getKey() );
				}
			}catch(CacheException ingnored) {
				// check for a mask, exception is never thrown
			}
		}

		return validIds;
	}

	/**
	 * return all known iterator pnfsid
	 */
	public Iterator<PnfsId> pnfsids() throws CacheException {
		List<PnfsId> allEntries = null;
		_repositoryOperationLock.readLock().lock();
		try {
			allEntries = new ArrayList<PnfsId>(_allEntries.keySet());
		}finally{
			_repositoryOperationLock.readLock().unlock();
		}

		return allEntries.iterator();
	}

	/**
	 * mark entry as removed. destroy ( physically remove from disk) if file is not in use
	 */
	public boolean removeEntry(CacheRepositoryEntry entry)	throws CacheException {


		if (_logRepository.isInfoEnabled()) {
			_logRepository.info("remove entry for: " + entry.getPnfsId().toString());
		}

		_repositoryOperationLock.writeLock().lock();

		try {


			PnfsId pnfsId = entry.getPnfsId();

			if (entry.isLocked()) return false;

			entry.setRemoved();


	        CacheRepositoryEvent removeEvent = new CacheRepositoryEvent( this , entry ) ;

	        processEvent(EventType.REMOVE, removeEvent ) ;

	        if( entry.getLinkCount() == 0 ){

	    		if (_allEntries.remove(pnfsId) == null) {
	    			throw new FileNotInCacheException("Entry already removed");
	    		}

	        	String name = pnfsId.toString();

	        	File dataFile = _repositoryTree.getDataFile(name);

	    		if( dataFile.exists() ) {
	    			long size = dataFile.length() ;
	    			freeSpace(size);
	    			if( entry.isPrecious() ) {
	    				_preciousSpace.addAndGet(-size);
	    			}
	    		}

	    		_repositoryTree.destroy(name);

	            processEvent(EventType.DESTROY, new CacheRepositoryEvent( this , entry ) ) ;
	        }

		}finally{
			_repositoryOperationLock.writeLock().unlock();
		}
		return true;
	}

    protected void storeReservedSpace() throws CacheException {
        try{
            PrintWriter pw = new PrintWriter( new FileOutputStream( _spaceReservation ) ) ;
            try{
                pw.println(""+getReservedSpace());
            }finally{
               try{ pw.close() ; }catch(Exception eee){}
            }
        }catch(IOException ioe ){
            throw new
            CacheException(103,"Io Exception, writing "+_spaceReservation) ;
        }
     }

	public void runInventory(Logable log) throws CacheException {
		runInventory(null, null, 0);
	}

	public void runInventory(Logable log, PnfsHandler pnfs, int flags)
			throws CacheException {


		List<String> knownEntries = _repositoryTree.list();
		long usedDataSpace = 0L;

		RepositoryEntryHealer repositoryEntryHealer = new  RepositoryEntryHealer(this, pnfs, _repositoryTree);

		_repositoryOperationLock.writeLock().lock();
		try {

			for ( String entryName: knownEntries )  {


				CacheRepositoryEntry repositoryEntry = repositoryEntryHealer.entryOf(entryName);
				if( repositoryEntry == null) continue;

				usedDataSpace += repositoryEntry.getSize();

				if( repositoryEntry.isPrecious() ) {
					_preciousSpace.addAndGet( repositoryEntry.getSize() );
				}

				if(_logRepository.isDebugEnabled() ) {
					_logRepository.debug(entryName +" " + repositoryEntry.getState() );
				}

				_allEntries.put(repositoryEntry.getPnfsId(), repositoryEntry);

				processEvent(EventType.SCAN, new CacheRepositoryEvent(this, repositoryEntry) );
				/*
				 * track sticky flag
				 */

				for( StickyRecord record:repositoryEntry.stickyRecords() ) {
					_stickyInspector.add(repositoryEntry.getPnfsId(), record );
				}

			}


		       try{
		           allocateSpace( usedDataSpace , 1000 ) ;
		        }catch(InterruptedException ee ){
		        	_logRepository.fatal("Not enough space in repository to store inventory");
		           throw new
		           CacheException(
		        		   CacheException.PANIC , "Not enough space in repository to store inventory ???");
		        }

				if(_logRepository.isDebugEnabled() ) {
					_logRepository.debug("Space used : " + usedDataSpace );
					_logRepository.debug("runInventory : #="+_allEntries.size()+
			                 ";space="+getFreeSpace()+
			                 "/"+getTotalSpace() );
				}


				_stickyInspectorThread.start();

		}catch(IOException ioe) {
			throw new
	           CacheException( ERROR_IO_DISK , "Low Level Exc : "+ioe) ;
		}finally{
			_repositoryOperationLock.writeLock().unlock();
		}

	}


	/*
	 * Test code
	 */

	public static void main(String[] args) {
		try {
			CacheRepository repository = new CacheRepositoryV3( new File("/home/tigran/dCacheAllInOne/pool/0"));

			repository.runInventory(null, null, 0);

		}catch(CacheException ce) {
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public boolean isRepositoryOk() {
		// TODO: fake for now
		return true;
	}
}
