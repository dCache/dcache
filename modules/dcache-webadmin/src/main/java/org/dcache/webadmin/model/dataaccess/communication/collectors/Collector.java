package org.dcache.webadmin.model.dataaccess.communication.collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.dcache.cells.CellStub;
import org.dcache.util.backoff.BackoffController;
import org.dcache.util.backoff.IBackoffAlgorithm;
import org.dcache.util.backoff.IBackoffAlgorithm.Status;
import org.dcache.util.backoff.IBackoffAlgorithmFactory;

/**
 * A collector is a runnable that can be run to collect information with cell-
 * communication via its cellstub and put it into its pagecache to later deliver
 * information to a webpage via DAOs. Each implementation should use a
 * ContextPath of the corresponding constant-interface ContextPaths as key to
 * this information.
 *
 * @author jans
 */
public abstract class Collector implements Runnable,
                Callable<IBackoffAlgorithm.Status> {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected String _name = "";
    protected ConcurrentMap<String, Object> _pageCache;
    protected CellStub _cellStub;
    protected long sleepInterval;
    protected TimeUnit sleepIntervalUnit;

    private boolean enabled = true;
    private IBackoffAlgorithmFactory factory;
    private BackoffController controller;

    public String getName() {
        return _name;
    }

    public Map<String, Object> getPageCache() {
        return _pageCache;
    }

    public void initialize() {
        controller = new BackoffController(factory);
    }

    public synchronized boolean isEnabled() {
        return enabled;
    }

    @Override
    public void run() {
        Status status = Status.SUCCESS;
        long timeout = sleepIntervalUnit.toMillis(sleepInterval);
        try {
            while (isEnabled()) {
                logger.debug("collector {} calling controller.call", this);
                status = controller.call(this);

                if (status == Status.FAILURE) {
                    logger.error("call returned failure status; exiting collection loop");
                    break;
                }

                synchronized (this) {
                    wait(timeout);
                }
            }
            /*
             * BackoffController preserves the generic Exception
             * contract on Callable, so we have to deal with
             * RuntimeExceptions as a subset of Exception first
             */
        } catch (RuntimeException t) {
            throw t;
        } catch (InterruptedException t) {
            logger.debug("controller.call() was interrupted; exiting");
        } catch (Exception t) {
            logger.error("unexpected exception: {}; exiting collection loop",
                            t.getMessage());
            logger.debug("controller.call()", t);
        }
    }

    @Required
    public void setAlgorithmFactory(IBackoffAlgorithmFactory factory) {
        this.factory = factory;
    }

    public void setCellStub(CellStub cellstub) {
        _cellStub = cellstub;
    }

    @Required
    public void setName(String name) {
        _name = name;
    }

    public void setPageCache(ConcurrentMap<String, Object> pageCache) {
        _pageCache = pageCache;
    }

    @Required
    public void setSleepInterval(long sleepInterval) {
        this.sleepInterval = sleepInterval;
    }

    @Required
    public void setSleepIntervalUnit(TimeUnit sleepIntervalUnit) {
        this.sleepIntervalUnit = sleepIntervalUnit;
    }

    protected synchronized void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }
}
