package org.dcache.webadmin.model.dataaccess.communication.collectors;

import java.util.Map;
import java.util.concurrent.Callable;

import org.dcache.cells.CellStub;
import org.dcache.util.backoff.BackoffControllerBuilder;
import org.dcache.util.backoff.BackoffControllerBuilder.BackoffController;
import org.dcache.util.backoff.IBackoffAlgorithm;
import org.dcache.util.backoff.IBackoffAlgorithm.Status;
import org.dcache.util.backoff.IBackoffAlgorithmFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    protected Map<String, Object> _pageCache;
    protected CellStub _cellStub;
    protected Long sleepInterval;

    private IBackoffAlgorithmFactory factory;
    private BackoffController controller;

    public String getName() {
        return _name;
    }

    public Map<String, Object> getPageCache() {
        return _pageCache;
    }

    public void initialize() {
        controller = new BackoffControllerBuilder().using(factory).build();
    }

    @Override
    public void run() {
        Status status = Status.SUCCESS;
        while (true) {
            try {
                status = controller.call(this);
            } catch (InterruptedException t) {
                break;
            } catch (Exception t) {
                logger.error("call threw unexpected exception", t);
                break;
            }

            if (status == Status.FAILURE) {
                logger.error("call failed unexpectedly");
                break;
            }

            synchronized (this) {
                try {
                    wait(sleepInterval);
                } catch (InterruptedException t) {
                    break;
                }
            }
        }
    }

    public void setAlgorithmFactory(IBackoffAlgorithmFactory factory) {
        this.factory = factory;
    }

    public void setCellStub(CellStub cellstub) {
        _cellStub = cellstub;
    }

    public void setName(String name) {
        _name = name;
    }

    public void setPageCache(Map<String, Object> pageCache) {
        _pageCache = pageCache;
    }

    public void setSleepInterval(Long sleepInterval) {
        this.sleepInterval = sleepInterval;
    }
}
