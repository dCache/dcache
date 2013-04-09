package org.dcache.pool.classic;

import javax.security.auth.Subject;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Set;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.DiskErrorCacheException;
import diskCacheV111.vehicles.ProtocolInfo;

import org.dcache.cells.CellStub;
import org.dcache.pool.movers.IoMode;
import org.dcache.pool.movers.MoverProtocol;
import org.dcache.pool.repository.FileRepositoryChannel;
import org.dcache.pool.repository.ReplicaDescriptor;
import org.dcache.pool.repository.Repository;
import org.dcache.pool.repository.Repository.OpenFlags;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.vehicles.FileAttributes;

/**
 * Encapsulates a read transfer, that is, sending a file. It acts as a
 * bridge between the repository and a mover.
 */
public class PoolIOReadTransfer
    extends PoolIOTransfer
{
    private final ReplicaDescriptor _handle;
    private final long _size;

    public PoolIOReadTransfer(long id, String initiator, boolean isPoolToPoolTransfer,
                              String queue, CellStub door,
                              FileAttributes fileAttributes,
                              ProtocolInfo protocolInfo,
                              Subject subject,
                              MoverProtocol mover,
                              TransferService transferService,
                              PostTransferService postTransferService,
                              Set<OpenFlags> openFlags,
                              Repository repository)
        throws CacheException, InterruptedException
    {
        super(id, initiator, isPoolToPoolTransfer, queue,  door, fileAttributes,
                protocolInfo, subject, mover, transferService, postTransferService);
        _handle = repository.openEntry(fileAttributes.getPnfsId(), openFlags);
        _size = _handle.getFile().length();
    }

    @Override
    public void transfer()
        throws Exception
    {
        File file = _handle.getFile();

        try {
            try (RepositoryChannel fileIoChannel = new FileRepositoryChannel(file, "r")) {
                _mover.runIO(_fileAttributes,
                        fileIoChannel,
                        _protocolInfo,
                        null,
                        IoMode.READ);
            }

            if (_mover.wasChanged()) {
                throw new RuntimeException("Bug: Mover changed read-only file");
            }
        } catch (FileNotFoundException e) {
            throw new DiskErrorCacheException(
                                     "File could not be opened  [" +
                                     e.getMessage() +
                                     "]; please check the file system");
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
