package diskCacheV111.util;

/**
 *
 * Extended file metadata
 *
 * @since 1.8.0-7
 * @Imutable
 */
public class FileMetaDataX {

	private final PnfsId _pnfsId;
	private final FileMetaData _fileMetaData;


	public FileMetaDataX(PnfsId pnfsId, FileMetaData fileMetaData) {
		_pnfsId = pnfsId;
		_fileMetaData = fileMetaData;
	}

	/**
	 *
	 * @return pnfsId of the file
	 */
	public PnfsId getPnfsId() {
		return _pnfsId;
	}


	/**
	 *
	 * @return file metadata
	 */
	public FileMetaData getFileMetaData() {
		return _fileMetaData;
	}
}
