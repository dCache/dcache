package org.dcache.pool.repository.v3;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import org.apache.log4j.Logger;
import org.dcache.pool.repository.v3.entry.CacheRepositoryEntryState;
import org.dcache.pool.repository.v3.entry.CacheRepositoryEntryV3Impl;

import diskCacheV111.repository.CacheRepositoryEntry;
import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.vehicles.StorageInfo;

class RepositoryEntryHealer {

	private static Logger _logRepository = Logger.getLogger("logger.org.dcache.repository");


	private final PnfsHandler _pnfsHandler;
	private final RepositoryTree _repositoryTree;
	private final CacheRepositoryV3 _repository;

	RepositoryEntryHealer(CacheRepositoryV3 repository, PnfsHandler pnfsHandler, RepositoryTree repositoryTree) {
		_pnfsHandler = pnfsHandler;
		_repositoryTree = repositoryTree;
		_repository = repository;
	}


	CacheRepositoryEntry entryOf( String entryName ) throws IOException, IllegalArgumentException, CacheException {

		File dataFile    = _repositoryTree.getDataFile(entryName);
		File controlFile = _repositoryTree.getControlFile(entryName);
		File siFile      = _repositoryTree.getSiFile(entryName);

		PnfsId pnfsId = null;
		CacheRepositoryEntry repositoryEntry = null;

		// throw IllegalArgumentException if it's not a legal pnfsid
		pnfsId = new PnfsId(entryName);


		try {
			_logRepository.debug("entryOf: " +  entryName);
			
			if( !controlFile.exists() ) {
				throw new StateNotFoundRepositoryException("State file missing");
			}
			
			repositoryEntry = new CacheRepositoryEntryV3Impl(_repository, pnfsId, controlFile, dataFile, siFile );
			if( repositoryEntry.getStorageInfo() == null ) {
				throw new SiFileCorruptedException("SI-file not found");
			}

			
		} catch (PartialFromClientException pfce) {

			/*
			 * well, following steps have to be done:
			 *
			 *    1. get storageInfo from pnfs
			 *    2. mark file as CACHED + STICKY
			 *    3. add cache location
			 */


			try {

				StorageInfo storageInfo  =  _pnfsHandler.getStorageInfo(entryName);
				storageInfo.setFileSize(dataFile.length());
				saveStorageInfo(storageInfo, siFile);

				CacheRepositoryEntryState entryState = new CacheRepositoryEntryState(controlFile);

				entryState.setCached();
				entryState.setSticky("system",-1);
				entryState.setError();


				_pnfsHandler.addCacheLocation(pnfsId);


			}catch(CacheException ce) {
				if(ce.getRc() == CacheException.FILE_NOT_FOUND ) {
					/*
					 * the file is already gone
					 */
					_logRepository.info(entryName + ": partialy recived from client removed by client. removeing...");
					_repositoryTree.destroy(entryName);
				}
			}


		} catch (PartialFromStoreException pfse) {
			// it's safe to remove partialyFromStore file, we have a copy on HSM anyway
			_logRepository.info(entryName + ": partialy recived from store file. removeing...");
			_repositoryTree.destroy(entryName);

		} catch( SiFileCorruptedException sice) {

			_logRepository.warn("Missing or bad SI-file for " + entryName + " : " + sice.getMessage() );
			/*
			 * well, we remove bad SI-file,
			 * get a new one from Pnfs and try again
			 *
			 * TODO: we have to build some kind of logic to prevent infinit recursion
			 */

			siFile.delete();

			try {

				StorageInfo storageInfo = _pnfsHandler.getStorageInfo(entryName);
				/*
				 * update file size if it's wrong and update pnfs  as well
				 */
				if( storageInfo.getFileSize() != dataFile.length() ) {
					storageInfo.setFileSize(dataFile.length());
					_pnfsHandler.setFileSize(pnfsId,storageInfo.getFileSize() );
				}
				saveStorageInfo(storageInfo, siFile);


			}catch( CacheException ce ) {

				if( ce.getRc() == CacheException.FILE_NOT_FOUND ) {
					
					_repositoryTree.destroy(entryName);
					return null;
					
					/*
					 * TODO: this part should take care that we do not remove files is pnfs manager in trouble 
					 * 
					CacheRepositoryEntryState entryState = new CacheRepositoryEntryState(controlFile);
					if( entryState.canRemove() ) {
						_logRepository.warn("removing missing removable entry : " + entryName );
						_repositoryTree.destroy(entryName);
						
						 // everybody is happy						 

						return null;
					}else{
						_logRepository.warn("mark as bad non removable missing entry : " + entryName );
						entryState.setSticky("system",-1);
						entryState.setError();

						return repositoryEntry;
					}
 					*/
				}else {
					throw ce;
				}
			}

			_logRepository.warn("SI-file for " + entryName + " recovered" );

			return entryOf(entryName);

		} catch (RepositoryMissmatchException rmme) {
			_logRepository.warn("file size missmatch for " + entryName + " : " + rmme.getMessage() );
			siFile.delete();
			return entryOf(entryName);
		}catch (StateNotFoundRepositoryException snf) {
			_logRepository.warn("missing state for " + entryName );
			StorageInfo storageInfo = _pnfsHandler.getStorageInfo(entryName);
			
			CacheRepositoryEntryState fileState = new CacheRepositoryEntryState(controlFile);			
			
			if( storageInfo.getRetentionPolicy().equals(RetentionPolicy.CUSTODIAL) && !storageInfo.isStored() ) {
				fileState.setPrecious(true);
			}else{
				fileState.setCached();
			}
			
			if(storageInfo.getAccessLatency().equals(AccessLatency.ONLINE) ) {
				fileState.setSticky("system", -1);
			}
			_logRepository.warn("file state recovered : " + entryName + " : " + fileState.toString() );
			return entryOf(entryName);
			
		} catch (RepositoryException unhadledRepositoryException) {
			_logRepository.warn("Unhandled Repository exception for " + entryName + " : " + unhadledRepositoryException.getMessage());
		}

		return repositoryEntry;

	}


	private static void saveStorageInfo(StorageInfo storageInfo, File siFile) throws IOException {

		ObjectOutputStream objectOut = null;

		try {

			siFile.createNewFile();

			objectOut = new ObjectOutputStream( new FileOutputStream( siFile) );
			objectOut.writeObject( storageInfo ) ;

		}finally {
			if( objectOut != null ) try {	objectOut.close(); } catch (IOException ignore) {}
		}
	}

}
