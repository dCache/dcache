package org.dcache.pool.classic;

import com.google.common.collect.Iterables;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileCorruptedCacheException;
import diskCacheV111.util.FileNotInCacheException;
import diskCacheV111.util.NotInTrashCacheException;
import diskCacheV111.util.PnfsId;

import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellLifeCycleAware;
import dmg.util.command.Argument;
import dmg.util.command.Command;

import org.dcache.alarms.AlarmMarkerFactory;
import org.dcache.alarms.PredefinedAlarm;
import org.dcache.pool.repository.ReplicaState;
import org.dcache.pool.repository.ReplicaDescriptor;
import org.dcache.pool.repository.Repository;
import org.dcache.pool.repository.Repository.OpenFlags;
import org.dcache.util.Checksum;
import org.dcache.util.Exceptions;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.dcache.util.Exceptions.messageOrClassName;

public class ChecksumScanner
    implements CellCommandListener, CellLifeCycleAware
{
    private static final Logger _log =
        LoggerFactory.getLogger(ChecksumScanner.class);

    private final FullScan _fullScan = new FullScan();
    private final Scrubber _scrubber = new Scrubber();
    private final SingleScan _singleScan = new SingleScan();

    private Repository _repository;
    private ChecksumModuleV1 _csm;
    private String poolName;

    private File _scrubberStateFile;

    /** Errors found while running 'csm check'.
     */
    private final Map<PnfsId,Iterable<Checksum>> _bad =
        new ConcurrentHashMap<>();

    private final Runnable listener = this::onConfigChange;

    private void onConfigChange()
    {
        if (_csm.hasPolicy(ChecksumModule.PolicyFlag.SCRUB)) {
            startScrubber();
        } else {
            stopScrubber();
        }
    }

    private void startScrubber()
    {
        _scrubber.start();
    }

    private void stopScrubber()
    {
        _scrubber.kill();
    }

    public void setRepository(Repository repository)
    {
        _repository = repository;
    }

    public void setChecksumModule(ChecksumModuleV1 csm)
    {
        _csm = csm;
    }

    public void setScrubberStateFile(File path)
    {
        _scrubberStateFile = path;
    }

    public void setPoolName(String poolName) {
        this.poolName = poolName;
    }

    private class FullScan extends Singleton
    {
        private volatile int _totalCount;
        private volatile int _badCount;
        private volatile int _unableCount;

        public FullScan()
        {
            super("FullScan");
        }

        @Override
        public void runIt() throws Exception
        {
            stopScrubber();
            try {
                _totalCount = _badCount = 0;
                _bad.clear();

                for (PnfsId id: _repository) {
                    try {
                        ReplicaDescriptor handle =
                            _repository.openEntry(id, EnumSet.of(OpenFlags.NOATIME));
                        try {
                            _csm.verifyChecksum(handle);
                        } finally {
                            handle.close();
                        }
                    } catch (FileNotInCacheException | NotInTrashCacheException e) {
                        /* It was removed before we could get it. No problem.
                         */
                    } catch (FileCorruptedCacheException e) {
                        if (e.getActualChecksums().isPresent()) {
                            _bad.put(id, e.getActualChecksums().get());
                            _badCount++;
                        } else {
                            _log.warn("csm scan command unable to verify {}: {}", id, e.getMessage());
                            _unableCount++;
                        }
                    } catch (CacheException e) {
                        _log.warn("csm scan command unable to verify {}: {}", id, e.getMessage());
                        _unableCount++;
                    } catch (IOException e) {
                        _unableCount++;
                        throw new IOException("failed to read " + id + ": " + messageOrClassName(e), e);
                    }
                    _totalCount++;
                }
            } catch (IOException e) {
                _log.error("Aborting 'cms check' full-scan: {}", messageOrClassName(e));
                setAbortMessage("failure in underlying storage: " + messageOrClassName(e));
            } finally {
                startScrubber();
            }
        }

        public String toString()
        {
            return super.toString() + " "
                + _totalCount + " files: "
                + _badCount + " corrupt, "
                + _unableCount + " unable to check";
        }
    }

    private class SingleScan extends Singleton
    {
        private volatile PnfsId _pnfsId;
        private volatile Iterable<Checksum> _actualChecksums;
        private volatile Iterable<Checksum> _expectedChecksums;

        public SingleScan()
        {
            super("SingleScan");
        }

        public synchronized void go(PnfsId pnfsId)
        {
            _expectedChecksums = null;
            _actualChecksums = null;
            _pnfsId = pnfsId;
            start();
        }

        @Override
        public void runIt()
                throws CacheException, InterruptedException, IOException, NoSuchAlgorithmException
        {
            stopScrubber();
            try {
                ReplicaDescriptor handle =
                    _repository.openEntry(_pnfsId, EnumSet.of(OpenFlags.NOATIME));
                try {
                    _actualChecksums = _csm.verifyChecksum(handle);
                } catch (FileCorruptedCacheException e) {
                    _expectedChecksums = e.getExpectedChecksums().get();
                    _actualChecksums = e.getActualChecksums().get();
                    _bad.put(_pnfsId, _actualChecksums);
                } finally {
                    handle.close();
                }
            } finally {
                startScrubber();
            }
        }

        public synchronized String toString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append(super.toString());
            if (_pnfsId != null) {
                sb.append("  ").append(_pnfsId).append(" ");
                if (_actualChecksums == null) {
                    sb.append("BUSY");
                } else if (_expectedChecksums == null) {
                    sb.append("OK ").append(_actualChecksums);
                } else {
                    sb.append("BAD File = ").append(_actualChecksums)
                        .append(" Expected = ").append(_expectedChecksums);
                }
            }
            return sb.toString();
        }
    }

    private class Scrubber extends Singleton
    {
        private final long CHECKPOINT_INTERVAL = TimeUnit.MINUTES.toMillis(1);
        private final long FAILURE_RATELIMIT_DELAY =
            TimeUnit.SECONDS.toMillis(10);

        private volatile int _badCount;
        private volatile int _numFiles;
        private volatile int _totalCount;
        private volatile int _unableCount;

        private PnfsId _lastFileChecked;
        private long _lastCheckpoint;
        private long _lastStart;

        public Scrubber()
        {
            super("Scrubber");
        }

        /**
         * Save scrubber state to <code>_scrubberStateFile</code>. The format is
         * the start time of the last scrub (<code>_lastStart</code>) separated
         * by a whitespace followed by the pnfs id of the file last checked
         * (<code>_lastFileChecked</code>). If there's no last checked file,
         * write a dash instead.
         */
        private void saveState()
        {
            String line = _lastStart + " " +
                          ((_lastFileChecked == null) ? "-" : _lastFileChecked);
            try {
                Files.write(line, _scrubberStateFile, Charset.defaultCharset());
            } catch (IOException e) {
                _log.error("Failed to save scrubber state ({}) to {}: {}", line, _scrubberStateFile, messageOrClassName(e));
            }
        }

        /**
         * Read the saved state information from disk written by <code>
         * saveState()</code>. The following fields are initialized:<code>
         * _lastFileChecked</code> - the pnfs id of the file that was last
         * checksummed; <code>_lastStart</code> - time when the last scrub
         * started, if there's no saved state it's initialized to the current
         * time.
         */
        private void initializeFromSavedState()
        {
            String line;
            try {
                line = Files.readFirstLine(_scrubberStateFile,
                                           Charset.defaultCharset());
            } catch (FileNotFoundException e) {
                /**
                 * ignored - start immediately and check whole pool
                 */
                _lastStart = System.currentTimeMillis();
                return;
            } catch (IOException e) {
                _log.error("Failed to read scrubber saved state from {}: {}",
                          _scrubberStateFile, messageOrClassName(e));
                return;
            }

            String[] fields = line.split(" ");
            if (fields.length != 2) {
                _log.error("scrubber saved state in {} has an invalid format: {}",
                          _scrubberStateFile, line);
                return;
            }

            try {
                _lastStart = Long.parseLong(fields[0]);
            } catch (NumberFormatException e) {
                _log.error("Failed to read the last scrubber start time from {}: {}",
                          _scrubberStateFile, e.getMessage());
                return;
            }

            if (PnfsId.isValid(fields[1])) {
                _log.debug("Resuming scrubbing from the first file with a pnfs id greater than {}",
                           fields[1]);
                _lastFileChecked = new PnfsId(fields[1]);
            } else if (!fields[1].equals("-")) {
                _log.error("Last checked pnfs id within {} has an invalid format: {}",
                           _scrubberStateFile, fields[1]);
            }
        }

        private boolean isFirstStart()
        {
            return !_scrubberStateFile.exists();
        }

        private boolean isResuming()
        {
            return (_lastFileChecked != null);
        }

        private void waitUntil(long t) throws InterruptedException
        {
            long now;
            while ((now = System.currentTimeMillis()) < t) {
                Thread.sleep(t - now);
            }
        }

        @Override
        public synchronized void start()
        {
            if (_csm.hasPolicy(ChecksumModule.PolicyFlag.SCRUB) && !isActive()) {
                super.start();
            }
        }

        @Override
        public void runIt() throws InterruptedException
        {
            initializeFromSavedState();
            boolean isFinished = !isFirstStart() && !isResuming();

            try {
                while (true) {
                    if (isFinished) {
                        _log.debug("Next scrub start is {}",
                             new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").
                                 format(new Date(_lastStart +
                                                 _csm.getScrubPeriod())));
                        if (System.currentTimeMillis() - _lastStart > _csm.getScrubPeriod()) {
                            _log.warn("The last scrub took longer time to finish ({} s.) than the configured period ({} s.) - consider increasing the scrubbing period",
                                      System.currentTimeMillis() - _lastStart,
                                      _csm.getScrubPeriod());
                        }
                        waitUntil(_lastStart + _csm.getScrubPeriod());
                        _lastStart = System.currentTimeMillis();
                        isFinished = false;
                    }

                    try {
                        PnfsId[] toScan = getFilesToVerify();
                        _numFiles = toScan.length;
                        _badCount = 0;
                        _totalCount = 0;
                        _unableCount = 0;
                        scanFiles(toScan);
                        if (_badCount > 0) {
                            _log.warn("Finished scrubbing. Found {} bad files of {}",
                                       _badCount, _numFiles);
                        }
                        isFinished = true;
                    } catch (IOException e) {
                        _log.error("Aborting scrubber run: {}", messageOrClassName(e));
                        setAbortMessage("failure in underlying storage: " + messageOrClassName(e));
                        Thread.sleep(FAILURE_RATELIMIT_DELAY);
                    } catch (IllegalStateException e) {
                        _log.error("Aborting scrubber run: {}", e.getMessage());
                        setAbortMessage("illegal state: " + e.getMessage());
                        Thread.sleep(FAILURE_RATELIMIT_DELAY);
                    } catch (NoSuchAlgorithmException e) {
                        _log.error("Aborting scrubber run: {}", e.getMessage());
                        setAbortMessage("checksum algorithm not supported: " + e.getMessage());
                        Thread.sleep(FAILURE_RATELIMIT_DELAY);
                    }
                }
            } finally {
                _log.debug("Stopping scrubber");
                saveState();
            }
        }

        /**
         * Return array of pnfs id's that has not yet been verified. Any files
         * added to the pool after this array has been generated will be
         * included the next time the array is generated.
         * @return array of pnfs id's that needs to be verified. No check is
         *         done on in which state the files are in.
         */
        private PnfsId[] getFilesToVerify()
        {
            PnfsId[] repcopy = Iterables.toArray(_repository, PnfsId.class);
            Arrays.sort(repcopy);

            if (!isResuming()) {
                return repcopy;
            }

            int index = Arrays.binarySearch(repcopy, _lastFileChecked);
            if (index >= 0) {
                /**
                 * Found. 0 <= index <= repcopy.length - 1
                 */
                return Arrays.copyOfRange(repcopy, index + 1, repcopy.length);
            } else {
                /**
                 * Not found. insertionPoint == -index - 1
                 * where: 0 <= insertionPoint <= repcopy.length
                 */
                return Arrays.copyOfRange(repcopy, -index - 1, repcopy.length);
            }
        }

        /**
         * Save state information only every <code>CHECKPOINT_INTERVAL</code>
         * period.
         */
        private void checkpointIfNeeded()
        {
            if (System.currentTimeMillis() - _lastCheckpoint > CHECKPOINT_INTERVAL) {
                saveState();
                _lastCheckpoint = System.currentTimeMillis();
            }
        }

        private void scanFiles(PnfsId[] repository)
                throws InterruptedException, NoSuchAlgorithmException, IOException
        {
            for (PnfsId id : repository) {
                try {
                    if (_repository.getState(id) == ReplicaState.CACHED ||
                        _repository.getState(id) == ReplicaState.PRECIOUS) {
                        ReplicaDescriptor handle =
                            _repository.openEntry(id, EnumSet.of(OpenFlags.NOATIME));
                        try {
                            _csm.verifyChecksumWithThroughputLimit(handle);
                        } finally {
                            handle.close();
                        }
                    }
                } catch (FileCorruptedCacheException e) {
                    _badCount++;
                    _log.error(AlarmMarkerFactory.getMarker(PredefinedAlarm.CHECKSUM,
                                                            id.toString(),
                                                            poolName),
                                    "Marking {} on {} as BROKEN: {}",
                                    id,
                                    poolName,
                                    e.getMessage());
                    try {
                        _repository.setState(id, ReplicaState.BROKEN);
                    } catch (CacheException f) {
                        _log.warn("Failed to mark {} as BROKEN: {}", id, f.getMessage());
                    }
                } catch (IOException e) {
                    _unableCount++;
                    throw new IOException("Unable to read " + id + ": " + messageOrClassName(e), e);
                } catch (FileNotInCacheException | NotInTrashCacheException e) {
                    /* It was removed before we could get it. No problem.
                     */
                } catch (CacheException e) {
                    _log.warn("Scrubber unable to verify {}: {}", id, e.getMessage());
                    _unableCount++;
                }
                _lastFileChecked = id;
                _totalCount++;
                checkpointIfNeeded();
            }
            _lastFileChecked = null;
        }

        @Override
        public String toString()
        {
            return super.toString() + " processed "
                + _totalCount + " of " + _numFiles + " files: "
                + _badCount + " corrupt, "
                + _unableCount + " unable to check";
        }
    }

    private abstract static class Singleton
    {
        private final String  _name;

        private Exception _lastException;
        private Thread _currentThread;
        private String _abortMessage;

        private Singleton(String name)
        {
            _name = name;
        }

        protected abstract void runIt() throws Exception;

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

        public synchronized void setAbortMessage(String reason)
        {
            _abortMessage = checkNotNull(reason);
        }

        private synchronized void stopped()
        {
            _currentThread = null;
        }

        public synchronized void setException(Exception e)
        {
            _lastException = e;
        }

        public synchronized void start()
        {
            if (isActive()) {
                throw new IllegalStateException("Still active");
            }
            _abortMessage = null;
            _lastException = null;
            _currentThread = new Thread(_name) {
                    @Override
                    public void run() {
                        try {
                            runIt();
                        } catch (Exception ee) {
                            setException(ee);
                        } finally {
                            stopped();
                        }
                    }
                };
            _currentThread.start();
        }

        @Override
        public synchronized String toString()
        {
            return _name + " " + getState() + (_lastException == null ? "" : " " + _lastException.toString()) + " ";
        }

        private synchronized String getState()
        {
            if (isActive()) {
                return "Active";
            } else if (_abortMessage != null) {
                return "Aborted (" + _abortMessage + ")";
            } else {
                return "Idle";
            }
        }
    }

    @Command(name = "csm check",
            hint = "verify checksum of files on this pool",
            description = "Do a checksum verification on a file or all files in " +
                    "this pool. The result of the check can be view using the " +
                    "'csm status' command.")
    public class CsmCheckCommand implements Callable<String>
    {
        @Argument(valueSpec = "PNFSID|*",
                usage = "Specify the pnfsID of the file to scan. If all " +
                        "files in this pool are to be scan, simply use '*'.")
        String pnfsID;

        @Override
        public String call() throws
                CacheException, InterruptedException,
                IOException, NoSuchAlgorithmException
        {
            if (pnfsID.equals("*")) {
                _fullScan.start();
            } else {
                _singleScan.go(new PnfsId(pnfsID));
            }
            return "Started ...; check 'csm status' for status";
        }
    }

    @Command(name = "csm status",
            hint = "get checksum verification result",
            description = "Display the detailed result of the checksum check " +
                    "performed by scanning all or a single file in this pool. " +
                    "This status report will contain the full scan, single scan " +
                    "and the scrubber result.")
    public class CsmStatusCommand implements Callable<String>
    {
        @Override
        public String call()
        {
            return _fullScan.toString() + "\n" + _singleScan.toString() + "\n" +
                    _scrubber.toString();
        }
    }

    @Command(name = "csm show errors",
            hint = "show errors found during checksum verification",
            description = "Display the list of all errors found while running " +
                    "the 'csm check' command. The shown information (based on " +
                    "these errors) will contain the pnfsID and the actual " +
                    "checksum value of the (corrupted) file. Nothing is returned " +
                    "when no error is found.")
    public class CsmShowErrorsCommand implements Callable<String>
    {
        @Override
        public String call()
        {
            StringBuilder builder = new StringBuilder();
            for (Map.Entry<PnfsId,Iterable<Checksum>> e: _bad.entrySet()) {
                builder
                        .append(e.getKey())
                        .append(" -> ")
                        .append(e.getValue())
                        .append('\n');
            }
            return builder.toString();
        }
    }

    @Override
    public void afterStart()
    {
        _csm.addListener(listener);
        startScrubber();
    }

    @Override
    public void beforeStop()
    {
        _csm.removeListener(listener);
        stopScrubber();
    }
}
