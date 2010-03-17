package org.dcache.util;

import java.util.Collection;
import java.util.Set;
import java.util.HashSet;

import diskCacheV111.util.CacheException;

/**
 * Task that queries the pools for a set of movers. Will eventually
 * terminate a transfer if the mover is missing or does not respond.
 */
public class PingMoversTask<T extends Transfer> implements Runnable
{
    private final Collection<T> _transfers;

    /**
     * Movers which we tried to ping, but we failed to locate on
     * the pool.
     */
    private Set<Transfer> _missing = new HashSet<Transfer>();

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

    public void run()
    {
        try {
            Set<Transfer> missingLastTime = _missing;
            _missing = new HashSet<Transfer>();

            for (Transfer transfer: _transfers) {
                try {
                    transfer.queryMoverInfo();
                } catch (CacheException e) {
                    if (missingLastTime.contains(transfer)) {
                        transfer.finished(CacheException.TIMEOUT,
                                          "Mover timeout");
                    } else {
                        _missing.add(transfer);
                    }
                }
            }
        } catch (InterruptedException e) {
        }
    }
}
