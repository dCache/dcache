package org.dcache.pool.classic;

import org.dcache.pool.repository.Repository;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.StorageInfo;
import org.dcache.pool.movers.MoverProtocol;

import java.io.File;
import java.io.FileNotFoundException;
import javax.security.auth.Subject;
import java.util.Set;
import org.dcache.pool.movers.IoMode;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.pool.repository.FileRepositoryChannel;
import org.dcache.pool.repository.ReplicaDescriptor;
import org.dcache.pool.repository.Repository.OpenFlags;

/**
 * Encapsulates a read transfer, that is, sending a file. It acts as a
 * bridge between the repository and a mover.
 */
public class PoolIOReadTransfer
    extends PoolIOTransfer
{
    private final ReplicaDescriptor _handle;
    private final long _size;

    public PoolIOReadTransfer(PnfsId pnfsId,
                              ProtocolInfo protocolInfo,
                              Subject subject,
                              StorageInfo storageInfo,
                              MoverProtocol mover,
                              Set<OpenFlags> openFlags,
                              Repository repository)
        throws CacheException, InterruptedException
    {
        super(pnfsId, protocolInfo, subject, storageInfo, mover);        
        _handle = repository.openEntry(pnfsId, openFlags);
        _size = _handle.getFile().length();
    }

    @Override
    public void transfer()
        throws Exception
    {
        long transferTimer = System.currentTimeMillis();

        File file = _handle.getFile();

        try {
            //                 say("Trying to open " + file);
            long fileSize = file.length();

            RepositoryChannel fileIoChannel = new FileRepositoryChannel(file, "r");
            try {
                _mover.runIO(fileIoChannel,
                             _protocolInfo,
                             _storageInfo,
                             _pnfsId,
                             null,
                             IoMode.READ);
            } finally {
                /* This may throw an IOException, although it
                 * is not clear when this would happen. If it
                 * does, we are probably better off
                 * propagating the exception.
                 */
                fileIoChannel.close();
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

    @Override
    public void close()
    {
        _handle.close();
    }

    @Override
    public long getFileSize()
    {
        return _size;
    }

    @Override
    public ReplicaDescriptor getIoHandle()
    {
        return _handle;
    }

    @Override
    public IoMode getIoMode() {
        return IoMode.READ;
    }
}