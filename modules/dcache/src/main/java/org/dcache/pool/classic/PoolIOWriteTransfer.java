package org.dcache.pool.classic;

import com.google.common.base.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.SyncFailedException;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.List;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.DiskErrorCacheException;
import diskCacheV111.util.FileInCacheException;
import diskCacheV111.vehicles.ProtocolInfo;

import org.dcache.pool.movers.ChecksumMover;
import org.dcache.pool.movers.IoMode;
import org.dcache.pool.movers.MoverProtocol;
import org.dcache.pool.repository.EntryState;
import org.dcache.pool.repository.FileRepositoryChannel;
import org.dcache.pool.repository.ReplicaDescriptor;
import org.dcache.pool.repository.Repository;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.util.Checksum;
import org.dcache.vehicles.FileAttributes;

/**
 * Encapsulates a write transfer, that is, receiving a file. It acts
 * as a bridge between the repository and a mover.
 */
public class PoolIOWriteTransfer
    extends PoolIOTransfer
{
    private final static Logger _log =
        LoggerFactory.getLogger(PoolIOWriteTransfer.class);

    private final ReplicaDescriptor _handle;
    private final File _file;
    private final ChecksumModule _checksumModule;

    public PoolIOWriteTransfer(FileAttributes fileAttributes,
                               ProtocolInfo protocolInfo,
                               Subject subject,
                               MoverProtocol mover,
                               Repository repository,
                               ChecksumModule checksumModule,
                               EntryState targetState,
                               List<StickyRecord> stickyRecords)
        throws FileInCacheException, IOException
    {
        super(fileAttributes, protocolInfo, subject, mover);

        _checksumModule = checksumModule;
        _handle = repository.createEntry(fileAttributes,
                                         EntryState.FROM_CLIENT,
                                         targetState,
                                         stickyRecords);
        _file = _handle.getFile();
        _file.createNewFile();
    }

    private void runMover(RepositoryChannel fileIoChannel)
        throws Exception
    {
        _mover.runIO(_fileAttributes,
                     fileIoChannel,
                     _protocolInfo,
                     _handle,
                     IoMode.WRITE);
    }

    @Override
    public void transfer()
        throws Exception
    {
        try {
            RepositoryChannel fileIoChannel = new FileRepositoryChannel(_file, "rw");
            try {
                if (_checksumModule.hasPolicy(ChecksumModule.PolicyFlag.ON_TRANSFER) && _mover instanceof ChecksumMover) {
                    ((ChecksumMover) _mover).enableTransferChecksum(
                            _checksumModule.getPreferredChecksumFactory(_handle).getType());
                }
                runMover(fileIoChannel);
            } finally {
                try {
                   fileIoChannel.sync();
                } catch (SyncFailedException e) {
                    fileIoChannel.sync();
                    _log.info("First sync failed [" + e + "], but second sync suceeded");
                } finally {
                    /* This may throw an IOException, although it is
                     * not clear when this would happen. If it does,
                     * we are probably better off propagating the
                     * exception, which is why we do not catch it
                     * here.
                     */
                    fileIoChannel.close();
                }
            }
        } catch (FileNotFoundException e) {
            throw new DiskErrorCacheException("File could not be created; please check the file system");
        }
    }

    @Override
    public void close()
        throws CacheException, InterruptedException, IOException
    {
        try {
            if (_mover instanceof ChecksumMover) {
                ChecksumMover cm = (ChecksumMover) _mover;
                _handle.addChecksums(Optional.fromNullable(cm.getExpectedChecksum()).asSet());
                _checksumModule.enforcePostTransferPolicy(_handle,
                        Optional.fromNullable(cm.getActualChecksum()).asSet());
            } else {
                _checksumModule.enforcePostTransferPolicy(_handle, Collections.<Checksum>emptySet());
            }

            _handle.commit();
        } catch (NoSuchAlgorithmException e) {
            throw new CacheException("Checksum calculation failed: " + e.getMessage());
        } finally {
            _handle.close();

            /* Temporary workaround to ensure that the correct size is
             * logged in billing and send back to the door.
             */
            _fileAttributes.setSize(getFileSize());
        }
    }

    @Override
    public long getFileSize()
    {
        return _file.length();
    }

    @Override
    public ReplicaDescriptor getIoHandle()
    {
        return _handle;
    }

    @Override
    public IoMode getIoMode()
    {
        return IoMode.WRITE;
    }
}
