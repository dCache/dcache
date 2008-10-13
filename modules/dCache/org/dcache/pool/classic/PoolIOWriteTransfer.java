package org.dcache.pool.classic;

import org.dcache.pool.repository.WriteHandle;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.pool.repository.EntryState;
import org.dcache.pool.repository.Repository;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.FileNotInCacheException;
import diskCacheV111.util.Checksum;
import diskCacheV111.util.ChecksumFactory;
import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileInCacheException;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.StorageInfo;
import org.dcache.pool.movers.MoverProtocol;
import org.dcache.pool.movers.ChecksumMover;
import diskCacheV111.repository.SpaceMonitor;
import diskCacheV111.repository.CacheRepository;

import dmg.cells.nucleus.NoRouteToCellException;

import java.io.File;
import java.io.RandomAccessFile;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Collections;

/**
 * Encapsulates a write transfer, that is, receiving a file. It acts
 * as a bridge between the repository and a mover.
 */
class PoolIOWriteTransfer
    extends PoolIOTransfer
{
    private final WriteHandle _handle;
    private final ChecksumFactory _checksumFactory;
    private final File _file;
    private final ChecksumModuleV1 _checksumModule;
    private final SpaceMonitor _monitor;

    private Checksum _clientChecksum;
    private Checksum _transferChecksum;
    private boolean _success = false;
    private long _size;

    private static List<StickyRecord> getStickyRecords(StorageInfo info)
    {
        AccessLatency al = info.getAccessLatency();
        if (al != null && al.equals( AccessLatency.ONLINE)) {
            return Collections.singletonList(new StickyRecord("system", -1));
        } else {
            return Collections.emptyList();
        }
    }

    private static EntryState getTargetState(StorageInfo info)
    {
        // flush to tape only if the file defined as a 'tape
        // file'( RP = Custodial) and the HSM is defined
        RetentionPolicy rp = info.getRetentionPolicy();
        if (info.getKey("overwrite") != null) {
            return EntryState.CACHED;
        } else if (rp != null && rp.equals(RetentionPolicy.CUSTODIAL)) {
            return EntryState.PRECIOUS;
        } else {
            return EntryState.CACHED;
        }
    }

    private static ChecksumFactory getChecksumFactory(ChecksumModuleV1 cm,
                                                      MoverProtocol mover,
                                                      ProtocolInfo info)
    {
        ChecksumFactory factory = null;
        if (mover instanceof ChecksumMover) {
            factory =
                ((ChecksumMover)mover).getChecksumFactory(info);
        }
        if (factory == null) {
            factory = cm.getDefaultChecksumFactory();
        }
        return factory;
    }

    public PoolIOWriteTransfer(PnfsId pnfsId,
                               ProtocolInfo protocolInfo,
                               StorageInfo storageInfo,
                               MoverProtocol mover,
                               Repository repository,
                               ChecksumModuleV1 checksumModule)
        throws FileInCacheException
    {
        super(pnfsId, protocolInfo, storageInfo, mover);

        _checksumModule = checksumModule;
        _checksumFactory =
            getChecksumFactory(checksumModule, mover, protocolInfo);

        /* Due to support of <AccessLatency> and <RetentionPolicy>
         * the file state in the pool has changed it's meaning:
         *
         *     precious: have to goto tape
         *     cached: free to be removed by sweeper
         *     cached+sticky: does not go to tape, isn't removed by sweeper
         *
         * new states depending on AL and RP:
         *
         *     Custodial+ONLINE   (T1D1) : precious+sticky  => cached+sticky
         *     Custodial+NEARLINE (T1D0) : precious         => cached
         *     Output+ONLINE      (T0D1) : cached+sticky    => cached+sticky
         */
        List<StickyRecord> stickyRecords = getStickyRecords(storageInfo);
        EntryState target = getTargetState(storageInfo);

        _handle = repository.createEntry(pnfsId,
                                         _storageInfo,
                                         EntryState.FROM_CLIENT,
                                         target,
                                         stickyRecords);
        _file = _handle.getFile();
        _monitor = new WriteHandleSpaceMonitorAdapter(repository, _handle);
    }

    private void runMover(RandomAccessFile raf)
        throws Exception
    {
        _mover.runIO(raf,
                     _protocolInfo,
                     _storageInfo,
                     _pnfsId,
                     _monitor,
                     MoverProtocol.WRITE
                     | MoverProtocol.READ);
    }

    public void transfer()
        throws Exception
    {
        try {
            RandomAccessFile raf = new RandomAccessFile(_file, "rw");
            try {
                if (_checksumModule.checkOnTransfer() &&
                    _mover instanceof ChecksumMover) {
                    ChecksumMover cm = (ChecksumMover)_mover;
                    cm.setDigest(_checksumFactory.create());
                    runMover(raf);
                    _clientChecksum = cm.getClientChecksum();
                    _transferChecksum = cm.getTransferChecksum();
                } else {
                    runMover(raf);
                }

                _success = true;
            } finally {
                /* This may throw an IOException, although it is not
                 * clear when this would happen. If it does, we are
                 * probably better off propagating the exception,
                 * which is why we do not catch it here.
                 */
                raf.close();
            }
        } catch (FileNotFoundException e) {
            throw new CacheException(CacheRepository.ERROR_IO_DISK,
                                     "File could not be created; please check the file system");

        }
    }

    public void close()
        throws CacheException, InterruptedException,
               IOException, NoRouteToCellException
    {
        _size = _file.length();
        boolean exists = _size > 0;
        try {
            if (_success) {
                _checksumModule.setMoverChecksums(_pnfsId,
                                                  _file,
                                                  _checksumFactory,
                                                  _clientChecksum,
                                                  _transferChecksum);
            }
        } finally {
            if (!_success) {
                _handle.cancel(exists);
            }
            _handle.close();
        }
    }

    public long getFileSize()
    {
        return _size;
    }
}