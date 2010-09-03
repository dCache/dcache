package org.dcache.pool.classic;

import org.dcache.pool.repository.ReadHandle;
import org.dcache.pool.repository.Repository;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.FileNotInCacheException;
import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.StorageInfo;
import org.dcache.pool.movers.MoverProtocol;

import java.io.File;
import java.io.RandomAccessFile;
import java.io.FileNotFoundException;

/**
 * Encapsulates a read transfer, that is, sending a file. It acts as a
 * bridge between the repository and a mover.
 */
public class PoolIOReadTransfer
    extends PoolIOTransfer
{
    private final ReadHandle _handle;
    private final long _size;

    public PoolIOReadTransfer(PnfsId pnfsId,
                              ProtocolInfo protocolInfo,
                              StorageInfo storageInfo,
                              MoverProtocol mover,
                              Repository repository)
        throws CacheException, InterruptedException
    {
        super(pnfsId, protocolInfo, storageInfo, mover);
        _handle = repository.openEntry(pnfsId);
        _size = _handle.getFile().length();
    }

    public void transfer()
        throws Exception
    {
        long transferTimer = System.currentTimeMillis();

        File file = _handle.getFile();

        try {
            //                 say("Trying to open " + file);
            long fileSize = file.length();

            RandomAccessFile raf = new RandomAccessFile(file, "r");
            try {
                _mover.runIO(raf,
                             _protocolInfo,
                             _storageInfo,
                             _pnfsId,
                             null,
                             MoverProtocol.READ);
            } finally {
                /* This may throw an IOException, although it
                 * is not clear when this would happen. If it
                 * does, we are probably better off
                 * propagating the exception.
                 */
                raf.close();
            }

            if (_mover.wasChanged()) {
                throw new RuntimeException("Bug: Mover changed read-only file");
            }

            //                 say(_pnfsId.toString() + ";length=" + fileSize + ";timer="
            //                     + sysTimer.getDifference().toString());

        } catch (FileNotFoundException e) {
            throw new CacheException(CacheException.ERROR_IO_DISK,
                                     "File could not be opened  [" +
                                     e.getMessage() +
                                     "]; please check the file system");
        } finally {
            transferTimer = System.currentTimeMillis() - transferTimer;
        }
    }

    public void close()
    {
        _handle.close();
    }

    public long getFileSize()
    {
        return _size;
    }
}