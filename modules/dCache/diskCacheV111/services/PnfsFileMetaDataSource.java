/*
 * $Id: PnfsFileMetaDataSource.java,v 1.1.2.2 2007-02-20 16:02:10 tigran Exp $
 */
package diskCacheV111.services;

import java.io.File;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileMetaData;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.PnfsFile;
import diskCacheV111.util.PnfsId;

import dmg.cells.nucleus.CellAdapter;
import dmg.util.Args;

public class PnfsFileMetaDataSource implements FileMetaDataSource {

	private CellAdapter _cell ;
	private File _mp;
	
	public PnfsFileMetaDataSource(CellAdapter cell) throws Exception {
		_cell = cell;
		Args args = _cell.getArgs();
		String fsRoot = args.getOpt("root");
		if( fsRoot == null ) {
			throw new IllegalArgumentException("-root not defined");			
		}
		
		_mp = new File(fsRoot);
		if(!_mp.exists() ){
			throw new IllegalArgumentException("fsRoot does not exist");
		}

		if(!_mp.isDirectory() ){
			throw new IllegalArgumentException("fsRoot not a directory");
		}		
	}
	
	public FileMetaData getMetaData(String path) throws CacheException {
	      PnfsFile   pnfsFile =  new PnfsFile( path ) ;
	      if( pnfsFile==null || !pnfsFile.exists() ) {
	    	  throw new FileNotFoundCacheException(path + " not found");
	      }
	      
	      PnfsId     pnfsId       = pnfsFile.getPnfsId() ;
	      
	      if( pnfsId == null ) {
	          throw new CacheException
	          ( "can't get pnfsId (not a pnfsfile)" ) ;
	      }
	      	      
	      return this.getMetaData(pnfsId);
	}

	public FileMetaData getMetaData(PnfsId pnfsId) throws CacheException {
	      
		  FileMetaData meta;
	      try {
	          meta = PnfsFile.getFileMetaData( _mp, pnfsId );
	      }catch (Exception e){
	          throw new CacheException("Path do not exist");
	      }
	      
	      return meta;
	}

}
/*
 * $Log: not supported by cvs2svn $
 * Revision 1.1.2.1  2007/01/05 11:43:38  radicke
 * backport of FileMetaDataSource from cvs-head:
 *
 * This allows to configure the provider of FileMetadata (mounted Pnfs or PnfsManager). GridFTP-door had to be changed to make use of the new provider (using to mounted pnfs).
 *
 * Revision 1.1  2006/11/07 10:40:44  tigran
 * new interface FileMataDataSource :
 *   returns fileMetaData by path or pnfsId
 *
 * two implementations: filesystem based and PnfsManager based
 *
 */
