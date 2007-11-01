package diskCacheV111.services;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileMetaData;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PnfsGetFileMetaDataMessage;
import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellPath;

public class PnfsManagerFileMetaDataSource implements FileMetaDataSource {

	
	private final CellAdapter _cell ;
	private static final int __pnfsTimeout = 5 * 60 * 1000 ;
	private final PnfsHandler _handler;
	
	public PnfsManagerFileMetaDataSource(CellAdapter cell) {
		_cell = cell;
		String pnfsManager = _cell.getArgs().getOpt("pnfsManager");
		if(pnfsManager == null ) {
			pnfsManager = "PnfsManager";
		}
		
		_handler = new PnfsHandler(  _cell ,  new CellPath( pnfsManager )  ) ;
		_handler.setPnfsTimeout(__pnfsTimeout*1000L);
	}
	
	public FileMetaData getMetaData(String path) throws CacheException {
		
		PnfsGetFileMetaDataMessage info = _handler.getFileMetaDataByPath(path);
		if( info.getReturnCode() != 0) {
			throw new CacheException( info.getReturnCode(), "unable to get metadata of " + path);
		}
				
		return info.getMetaData();
	}

	public FileMetaData getMetaData(PnfsId pnfsId) throws CacheException {
		PnfsGetFileMetaDataMessage info = _handler.getFileMetaDataById(pnfsId);
		if( info.getReturnCode() != 0) {
			throw new CacheException( info.getReturnCode(), "unable to get metadata of " + pnfsId);
		}
				
		return info.getMetaData();
	}

}
