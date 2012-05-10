package org.dcache.pool.classic;

import java.io.File;
import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import dmg.util.Args;

import diskCacheV111.util.PnfsId;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.FileNotInCacheException;
import diskCacheV111.util.NotInTrashCacheException;
import diskCacheV111.vehicles.StorageInfo;

import org.dcache.cells.CellCommandListener;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;
import org.dcache.namespace.FileAttribute;

import org.dcache.pool.repository.Repository;
import org.dcache.pool.repository.Repository.OpenFlags;
import org.dcache.pool.repository.ReplicaDescriptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChecksumScanner
    implements CellCommandListener
{
    private final static Logger _log =
        LoggerFactory.getLogger(ChecksumScanner.class);

    private final FullScan _fullScan = new FullScan();
    private final SingleScan _singleScan = new SingleScan();

    private Repository _repository;
    private PnfsHandler _pnfs;
    private ChecksumModuleV1 _csm;

    /** Errors found while running 'csm check'.
     */
    private final Map<PnfsId,Checksum> _bad =
        new ConcurrentHashMap<PnfsId,Checksum>();

    public void setRepository(Repository repository)
    {
        _repository = repository;
    }

    public void setPnfs(PnfsHandler pnfs)
    {
        _pnfs = pnfs;
    }

    public void setChecksumModule(ChecksumModuleV1 csm)
    {
        _csm = csm;
    }

    private Checksum checkFile(File file)
        throws IOException, InterruptedException
    {
        return _csm.getDefaultChecksumFactory().computeChecksum(file);
    }

    private class FullScan extends Singleton
    {
        private volatile int _totalCount;
        private volatile int _badCount;

        public FullScan()
        {
            super("FullScan");
        }

        public void runIt() throws Exception
        {
            _totalCount = _badCount = 0;
            _bad.clear();

            for (PnfsId id: _repository) {
                try {
                    ReplicaDescriptor handle = _repository.openEntry(id, EnumSet.of(OpenFlags.NOATIME));
                    try {
                        Checksum file, replica;

                        replica = checkFile(handle.getFile());
                        StorageInfo info = handle.getEntry().getStorageInfo();
                        String flags =
                            (info == null ? null : info.getKey("flag-c"));

                        if (flags == null) {
                            file = _csm.getDefaultChecksumFactory().find(_pnfs.getFileAttributes(id, EnumSet.of(FileAttribute.CHECKSUM)).getChecksums());
                        } else {
                            file = Checksum.parseChecksum(flags);
                        }

                        _totalCount++;

                        if (file != null && !file.equals(replica)) {
                            _bad.put(id, replica);
                            _badCount++;
                        }
                    } finally {
                        handle.close();
                    }
                } catch (FileNotInCacheException e) {
                    /* It was removed before we could get it. No problem.
                     */
                } catch (NotInTrashCacheException e) {
                    /* orphan or lost file. Not our problem.
                     */
                }
            }
        }

        public String toString()
        {
            return super.toString() + " "
                + _totalCount + " checked; "
                + _badCount + " errors detected";
        }
    }

    private class SingleScan extends Singleton
    {
        private volatile PnfsId _pnfsId;
        private volatile Checksum _fileCRC;
        private volatile Checksum _infoCRC;

        public SingleScan()
        {
            super("SingeScan");
        }

        public synchronized void go(PnfsId pnfsId) throws Exception
        {
            _pnfsId = pnfsId;
            start();
        }

        public void runIt() throws Exception
        {
            _fileCRC = null;
            _infoCRC = null;
            ReplicaDescriptor handle = _repository.openEntry(_pnfsId, EnumSet.of(OpenFlags.NOATIME));
            try {
                _fileCRC = checkFile(handle.getFile());
                StorageInfo info = handle.getEntry().getStorageInfo();
                String flags = (info == null) ? null : info.getKey("flag-c");
                _infoCRC =
                    (flags == null) ? null : Checksum.parseChecksum(flags);

                if (_infoCRC != null && !_infoCRC.equals(_fileCRC)) {
                    _bad.put(_pnfsId, _fileCRC);
                }
            } finally {
                handle.close();
            }
        }

        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append(super.toString());
            if (_pnfsId != null) {
                sb.append("  ").append(_pnfsId).append(" ");
                if ((_fileCRC != null) && (_infoCRC == null)) {
                    sb.append("No StorageInfo, crc = ").append(_fileCRC);
                } else if ((_fileCRC == null) || (_infoCRC == null)) {
                    sb.append("BUSY");
                } else if (_fileCRC.equals(_infoCRC)) {
                    sb.append("OK ").append(_fileCRC.toString());
                } else {
                    sb.append("BAD File = ").append(_fileCRC)
                        .append(" Expected = ").append(_infoCRC);
                }
            }
            return sb.toString();
        }
    }

    abstract private class Singleton
    {
        private final String  _name;

        private Exception _lastException;
        private Thread _currentThread;

        private Singleton(String name)
        {
            _name = name;
        }

        abstract protected void runIt() throws Exception;

        public synchronized void kill()
        {
            if (isActive()) {
                _currentThread.interrupt();
            }
        }

        public synchronized boolean isActive()
        {
            return (_currentThread != null);
        }

        private synchronized void stopped()
        {
            _currentThread = null;
        }

        public synchronized void setException(Exception exception)
        {
            _lastException = exception;
            _log.error(exception.toString());
        }

        public synchronized Exception getException()
        {
            return _lastException;
        }

        public synchronized void start() throws Exception
        {
            if (isActive()) {
                throw new IllegalStateException("Still active");
            }
            _lastException = null;
            _currentThread = new Thread(_name) {
                    public void run() {
                        try {
                            runIt();
                        } catch (Exception ee) {
                            _lastException = ee;
                        } finally {
                            stopped();
                        }
                    }
                };
            _currentThread.start();
        }

        public synchronized String toString()
        {
            return _name + (isActive() ? " Active " : " Idle ") +
                (_lastException == null ? "" : _lastException.toString());
        }
    }

    public final static String hh_csm_check = " [ * | <pnfsId> ]";
    public String ac_csm_check_$_1(Args args) throws Exception
    {
        if (args.argv(0).equals("*")) {
            _fullScan.start();
        } else {
            _singleScan.go(new PnfsId(args.argv(0)));
        }
        return "Started ...; check 'csm status' for status";
    }

    public String ac_csm_status(Args args)
    {
        return _fullScan.toString() + "\n" + _singleScan.toString();
    }

    public final static String hh_csm_show_errors =
        "# show errors found with 'csm check'";
    public String ac_csm_show_errors(Args args)
    {
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<PnfsId,Checksum> e: _bad.entrySet()) {
            builder
                .append(e.getKey())
                .append(" -> ")
                .append(e.getValue())
                .append('\n');
        }
        return builder.toString();
    }
}
