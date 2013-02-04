package org.dcache.services.hsmcleaner;

import java.net.URI;

import java.io.File;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.PrintWriter;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Semaphore;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;

import org.dcache.cells.CellStub;
import org.dcache.cells.AbstractCell;
import org.dcache.cells.Option;
import org.dcache.util.BroadcastRegistrationTask;

import dmg.util.Args;
import dmg.cells.nucleus.CellPath;

/**
 * This cell watches the PNFS trash directories for HSM and cleanes
 * files from HSM.
 *
 * The cell is designed on the following observations:
 *
 * a) Crashes (JVM, host, etc) of the HSM cleaner may happen, but
 *    are infrequent.
 *
 * b) Bulk deletion of files do occur and we must protect the cleaner
 *    and pool cells against them.
 *
 * c) HSMs may be down for a while, and the number of unprocessed
 *    deleted files may grow large.
 *
 * d) Since PNFS IDs are unique, deletion is idempotent.
 *
 * b) and c) force us to keep some state on disk, as we cannot keep
 * all information in memory. Due to c), we have to consolidate trash
 * files into index files. This is implemented in the
 * FailureRepository class.
 *
 * a) and d) allow us to use an optimistic approach. Although we have
 * to be able to recover from a crash, the recovery may be inexact in
 * the sense that deleting files already deleted is acceptable. This
 * simplifies the design, as we can keep most state information in
 * memory.
 */
public class HSMCleaner extends AbstractCell
{
    private static final long BROADCAST_REGISTRATION_PERIOD = TimeUnit.MINUTES.toMillis(5);
    private static final long BROADCAST_REGISTRATION_EXPIRATION = TimeUnit.MINUTES.toMillis(6);

    private final EventHistogram _histogram =
        new EventHistogram(12, 60 * 60 * 1000);

    private FailureRepository _failures;
    private Trash _trash;
    private RequestTracker _requests;
    private ScheduledExecutorService _executor;
    private BroadcastRegistrationTask _broadcastRegistration;
    private PoolInformationBase _pools;

    private final static Class<?> POOLUP_MESSAGE =
        diskCacheV111.vehicles.PoolManagerPoolUpMessage.class;


    private final Runnable _scanTask = new Runnable() {
            @Override
            public void run() {
                scan();
            }
        };
    private final Runnable _recoverTask = new Runnable() {
            @Override
            public void run() {
                recover();
            }
        };
    private final Runnable _flushTask = new Runnable() {
            @Override
            public void run() {
                flush();
            }
        };

    /**
     * Semaphore protecting how many URIs are processed at a time,
     * that is, how many outstanding deletion requests we have handed
     * over to the request tracker.
     */
    private Semaphore _limit;

    @Option(
        name = "hsmCleanerScan",
        description = "Scan interval",
        required = true,
        unit = "seconds"
    )
    protected int _scanInterval;

    @Option(
        name = "hsmCleanerRecover",
        description = "Recover interval",
        required = true,
        unit = "seconds"
    )
    protected int _recoverInterval;

    @Option(
        name = "hsmCleanerFlush",
        description = "Flush interval",
        required = true,
        unit = "seconds"
    )
    protected int _flushInterval;

    @Option(
        name = "hsmCleanerTrash",
        description = "Trash directory",
        required = true
    )
    protected File _trashLocation;

    @Option(
        name = "hsmCleanerRepository",
        description = "Repository directory",
        required = true
    )
    protected File _failureLocation;

    @Option(
        name = "hsmCleanerQueue",
        description = "Max. number of queued files",
        required = true
    )
    protected int _maxQueueLength;

    @Option(
        name = "hsmCleanerTimeout",
        description = "Time after which a pool request is considered failed",
        required = true,
        unit = "seconds"
    )
    protected int _timeout;

    @Option(
        name = "hsmCleanerRequest",
        description = "Max. files per pool request",
        required = true,
        unit = "files"
    )
    protected int _maxRequests;

    public HSMCleaner(String cellName, String args)
        throws InterruptedException, ExecutionException
    {
	super(cellName, args);
        doInit();
    }

    @Override
    protected void init()
        throws Exception
    {
        super.init();

	useInterpreter(true);

        if (!_trashLocation.isDirectory()) {
            throw new
                IllegalArgumentException("Not a directory: "
                                         + _trashLocation);
        }

        _failureLocation.mkdirs();
        if (!_failureLocation.isDirectory()) {
            throw new IOException("Cannot create: " + _failureLocation);
        }

        CellPath me = new CellPath(getCellName(), getCellDomainName());
        _broadcastRegistration =
            new BroadcastRegistrationTask(this, POOLUP_MESSAGE, me);
        _broadcastRegistration.setExpires(BROADCAST_REGISTRATION_EXPIRATION);
        _trash = new OSMTrash(_trashLocation);
        _requests = new RequestTracker();
        _failures = new FailureRepository(_failureLocation);
        _limit = new Semaphore(_maxQueueLength);
        _pools = new PoolInformationBase();

        _requests.setPoolStub(new CellStub(this));
        _requests.setMaxFilesPerRequest(_maxRequests);
        _requests.setTimeout(_timeout * 1000);
        _requests.setPoolInformationBase(_pools);
        _requests.setSuccessSink(new Sink<URI>() {
                @Override
                public void push(URI uri) {
                    onSuccess(uri);
                }
            });
        _requests.setFailureSink(new Sink<URI>() {
                @Override
                public void push(URI uri) {
                    onFailure(uri);
                }
            });

        addMessageListener(_pools);
        addCommandListener(_pools);
        addMessageListener(_requests);
        addCommandListener(_requests);

        _executor = Executors.newSingleThreadScheduledExecutor();
        _executor.scheduleWithFixedDelay(_scanTask, _scanInterval,
                                         _scanInterval, TimeUnit.SECONDS);
        _executor.scheduleWithFixedDelay(_recoverTask, _recoverInterval,
                                         _recoverInterval, TimeUnit.SECONDS);
        _executor.scheduleWithFixedDelay(_flushTask, _flushInterval,
                                         _flushInterval, TimeUnit.SECONDS);
        _executor.scheduleAtFixedRate(_broadcastRegistration, 0, BROADCAST_REGISTRATION_PERIOD,
                                      TimeUnit.MILLISECONDS);
    }

    @Override
    public void cleanUp()
    {
        if (_requests != null) {
            _requests.shutdown();
        }
        if (_executor != null) {
            _executor.shutdownNow();
        }
        if (_broadcastRegistration != null) {
            _broadcastRegistration.unregister();
        }
        super.cleanUp();
    }

    @Override
    public void getInfo(PrintWriter out)
    {
        super.getInfo(out);
        writeOptions(out);

        out.println("Histogram:");
        _histogram.write(out);
    }

    /**
     * Perform a scan of the trash directory.
     */
    protected void scan()
    {
        try {
            info("Scanning " + _trashLocation);
            _trash.scan(new Sink<URI>() {
                    @Override
                    public void push(URI uri) {
                        onScan(uri);
                    }
                });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Recover failures.
     */
    protected void recover()
    {
        try {
            _failures.recover(new Sink<URI>() {
                    @Override
                    public void push(URI uri) {
                        onRecover(uri);
                    }
                });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IllegalStateException | IOException e) {
            error(e.getMessage());
        }
    }

    /**
     * Flush the failure repository to disk.
     */
    protected void flush()
    {
        try {
            _failures.flush(new Sink<URI>() {
                    @Override
                    public void push(URI uri) {
                        onQuarantine(uri);
                    }
                });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            error(e.getMessage());
        }
    }

    /**
     * Called when a new file was discovered in the trash directory.
     */
    protected void onScan(URI uri)
    {
        try {
            debug("Detected " + uri);
            _limit.acquire();
            _requests.submit(uri);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Called when a file was sucessfully deleted from the HSM.
     */
    protected void onSuccess(URI uri)
    {
        debug("Deleted " + uri);
        _limit.release();
        _trash.remove(uri);
        _failures.remove(uri);
        _histogram.add();
    }

    /**
     * Called when a file could not be deleted from the HSM.
     */
    protected void onFailure(URI uri)
    {
        debug("Failed " + uri);
        _limit.release();
        _failures.add(uri);
    }

    /**
     * Called when a failure was registered on permanent storage.
     */
    protected void onQuarantine(URI uri)
    {
        debug("Flushed " + uri);
        _trash.remove(uri);
    }

    /**
     * Called for each URI to recover.
     */
    protected void onRecover(URI uri)
    {
        try {
            debug("Retrying " + uri);
            _limit.acquire();
            _requests.submit(uri);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static final String hh_scan =
        "# Trigger scan of the trash directory";
    public String ac_scan(Args args)
    {
        _executor.submit(_scanTask);
        return "";
    }

    public static final String hh_recover =
        "# Trigger recovery from failures";
    public String ac_recover(Args args)
    {
        _executor.submit(_recoverTask);
        return "";
    }

    public static final String hh_flush =
        "# Force information about failures to stable storage";
    public String ac_flush(Args args)
    {
        _executor.submit(_flushTask);
        return "";
    }
}
