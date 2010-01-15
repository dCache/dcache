package diskCacheV111.services;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileMetaData;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellPath;

public class PnfsManagerFileMetaDataSource implements FileMetaDataSource
{
    private static final int PNFS_TIMEOUT = 5 * 60 * 1000;
    private final PnfsHandler _handler;

    public PnfsManagerFileMetaDataSource(CellEndpoint cell)
    {
        String pnfsManager = cell.getArgs().getOpt("pnfsManager");
        if (pnfsManager == null) {
            pnfsManager = "PnfsManager";
        }
        _handler = new PnfsHandler(cell,  new CellPath(pnfsManager));
        _handler.setPnfsTimeout(PNFS_TIMEOUT);
    }

    public FileMetaData getMetaData(String path) throws CacheException
    {
        return new FileMetaData(_handler.getFileAttributes(path, FileMetaData.getKnownFileAttributes()));
    }

    public FileMetaData getMetaData(PnfsId pnfsId) throws CacheException
    {
        return new FileMetaData(_handler.getFileAttributes(pnfsId, FileMetaData.getKnownFileAttributes()));
    }
}
