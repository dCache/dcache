package org.dcache.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import diskCacheV111.util.CacheException;

/**
 * Task that queries the pools for a set of movers. Will eventually
 * terminate a transfer if the mover is missing or does not respond.
 */
public class PingMoversTask<T extends Transfer> implements Runnable
{
    private static final Logger _log =
        LoggerFactory.getLogger(PingMoversTask.class);

    private final Collection<T> _transfers;

    /**
     * Movers which we tried to ping, but we failed to locate on
     * the pool.
     */
    private Set<Transfer> _missing = new HashSet<>();

    /**
     * Constructs a PingMoverTask for a set of transfers. The set is
     * supposed to be live in the sense that it always represents the
     * current set of transfers to monitor. The set must be thread
     * safe.
     */
    public PingMoversTask(Collection<T> transfers)
    {
        _transfers = transfers;
    }

    @Override
    public void run()
    {
        try {
            Set<Transfer> missingLastTime = _missing;
            _missing = new HashSet<>();

            for (Transfer transfer: _transfers) {
                try {
                    if (transfer.hasMover()) {
                        transfer.queryMoverInfo();
                        _log.debug("Mover {}/{} is alive",
                                   transfer.getPool(), transfer.getMoverId());
                    }
                } catch (IllegalStateException e) {
                    // The transfer terminated before we could query it.
                    _log.debug(e.toString());
                } catch (CacheException e) {
                    _log.info("Failed to check status of mover {}/{}: {}", transfer.getPool(), transfer.getMoverId(),
                              e.getMessage());
                    if (missingLastTime.contains(transfer)) {
                        transfer.finished(CacheException.TIMEOUT,
                                          String.format("Transfer killed by door due to failure for mover %s/%d: %s",
                                                        transfer.getPool(),
                                                        transfer.getMoverId(),
                                                        e.getMessage()));
                    } else {
                        _missing.add(transfer);
                    }
                }
            }
        } catch (InterruptedException e) {
        }
    }
}
