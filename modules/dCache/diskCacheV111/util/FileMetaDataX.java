package diskCacheV111.util;

public class FileMetaDataX {
	
	private final PnfsId _pnfsId;
	private final FileMetaData _fileMetaData;

	
	public FileMetaDataX(PnfsId pnfsId, FileMetaData fileMetaData) {
		_pnfsId = pnfsId;
		_fileMetaData = fileMetaData;
	}
	
	
	public PnfsId getPnfsId() {
		return _pnfsId;
	}
	
	
	public FileMetaData getFileMetaData() {
		return _fileMetaData;
	}
}
