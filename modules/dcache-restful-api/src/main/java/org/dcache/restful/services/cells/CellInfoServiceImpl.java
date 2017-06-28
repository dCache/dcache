/*
COPYRIGHT STATUS:
Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
software are sponsored by the U.S. Department of Energy under Contract No.
DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
non-exclusive, royalty-free license to publish or reproduce these documents
and software for U.S. Government purposes.  All documents and software
available from this server are protected under the U.S. and Foreign
Copyright Laws, and FNAL reserves all rights.

Distribution of the software available from this server is free of
charge subject to the user following the terms of the Fermitools
Software Legal Information.

Redistribution and/or modification of the software shall be accompanied
by the Fermitools Software Legal Information  (including the copyright
notice).

The user is asked to feed back problems, benefits, and/or suggestions
about the software to the Fermilab Software Providers.

Neither the name of Fermilab, the  URA, nor the names of the contributors
may be used to endorse or promote products derived from this software
without specific prior written permission.

DISCLAIMER OF LIABILITY (BSD):

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.

Liabilities of the Government:

This software is provided by URA, independent from its Prime Contract
with the U.S. Department of Energy. URA is acting independently from
the Government and in its own private capacity and is not acting on
behalf of the U.S. Government, nor as its contractor nor its agent.
Correspondingly, it is understood and agreed that the U.S. Government
has no connection to this software and in no manner whatsoever shall
be liable for nor assume any responsibility or obligation for any claim,
cost, or damages arising out of or resulting from the use of the software
available from this server.

Export Control:

All documents and software available from this server are subject to U.S.
export control laws.  Anyone downloading information from this server is
obligated to secure any necessary Government licenses before exporting
documents or software obtained from this server.
 */
package org.dcache.restful.services.cells;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.util.command.Command;
import org.dcache.restful.services.admin.CellDataCollectingService;
import org.dcache.restful.util.cells.CellInfoCollector;
import org.dcache.restful.util.cells.CellInfoCollectorUtils;
import org.dcache.restful.util.cells.CellInfoFutureProcessor;
import org.dcache.restful.util.cells.ListenableFutureWrapper;
import org.dcache.vehicles.cells.json.CellData;

/**
 * <p>Responsible for serving up data from the cache.</p>
 */
public class CellInfoServiceImpl extends
                CellDataCollectingService<Map<String, ListenableFutureWrapper<CellInfo>>, CellInfoCollector>
                implements CellInfoService, CellMessageReceiver {
    @Command(name = "cells set timeout",
                    hint = "Set the timeout interval between refreshes",
                    description = "Changes the interval between "
                                    + "queries for cell information.")
    class CellsSetTimeoutCommand extends SetTimeoutCommand {
    }

    @Command(name = "cells refresh",
                    hint = "Query for current cell info of well-known services",
                    description = "Interrupts current wait to run query "
                                    + "immediately.")
    class CellsRefreshCommand extends RefreshCommand {
        @Override
        public String call() {
            processor.cancel();
            return super.call();
        }
    }

    @Command(name = "cells ls",
                     hint = "List cell info",
                     description = "Displays a list of info for all "
                                     + "current well-known services.")
    class CellsLsCommand implements Callable<String> {
        @Override
        public String call() throws Exception {
            return Arrays.stream(getAddresses())
                         .map(CellInfoServiceImpl.this::getCellData)
                         .map(CellData::toString)
                         .collect(Collectors.joining("\n"));
        }
    }

    /**
     * <p>Fetches from disk. Will return a new object for a file
     * which does not exist.</p>
     */
    class CellInfoCacheLoader extends CacheLoader<String, CellData> {
        @Override
        public CellData load(String key) throws Exception {
            return CellInfoCollectorUtils.fetch(key, storageDir);
        }
    }

    /**
     * <p>In-memory caching.</p>
     *
     * <p>The processor always reads through the cache, triggering a
     * load if the object is not present.</p>
     *
     * <p>The cache is invalidated during the
     * periodic collection-process cycle; lifetime is
     * equal to the cycle period.</p>
     */
    private final CacheLoader<String, CellData> cacheLoader
                    = new CellInfoCacheLoader();

    private LoadingCache<String, CellData> cache;

    private String[] currentKnownCells = new String[0];

    /**
     * <p>Does the brunt of the updating work on the data returned
     * by the collector.</p>
     */
    private CellInfoFutureProcessor processor;

    /**
     * <p>Size is set fairly low.  There is no reason to maintain
     * all the cells in memory if they are not being
     * accessed between cyclical collection of data.</p>
     */
    private long maxCacheSize = 100;

    /**
     * <p>Location of persistent JSON files.</p>
     */
    private File storageDir;

    @Override
    public synchronized String[] getAddresses() {
        return currentKnownCells;
    }

    @Override
    public CellData getCellData(String address) {
        CellData cached = fetch(address);
        if (cached == null) {
            throw new NoSuchElementException(address);
        }
        return cached;
    }

    public void setMaxCacheSize(long maxCacheSize) {
        this.maxCacheSize = maxCacheSize;
    }

    public void setProcessor(CellInfoFutureProcessor processor) {
        this.processor = processor;
    }

    public void setStorageDir(File storageDir) {
        this.storageDir = storageDir;
    }

    @Override
    protected synchronized void configure() {
        Map<String, CellData> current = cache == null ?
                        Collections.EMPTY_MAP : cache.asMap();

        cache = CacheBuilder.newBuilder()
                            .maximumSize(maxCacheSize)
                            .expireAfterWrite(timeout, timeoutUnit)
                            .build(cacheLoader);

        cache.putAll(current);

        processor.setCache(cache);
    }

    /**
     * <p>The processor refreshes/overwrites data on disk (the JSON
     *      files representing the cell info).
     *      It invalidates the cache entries so that the next read will
     *      fetch and deserialize the object from file.</p>
     */
    @Override
    protected void update(Map<String, ListenableFutureWrapper<CellInfo>> data) {
        try {
            processor.process(data);
            synchronized (this) {
                currentKnownCells = data.keySet().toArray(new String[0]);
                Arrays.sort(currentKnownCells);
            }
        } catch (IllegalStateException e) {
            LOGGER.warn("Processing cycle has overlapped; you may wish to "
                                        + "increase the interval between "
                                        + "collections, which is currently "
                                        + "set to {} {}.",
                        timeout, timeoutUnit);
        } catch (IllegalArgumentException e) {
            LOGGER.warn("Processing failed for the current cycle: {}.",
                        e.getMessage());
        }
    }

    /**
     * <p>Wrapper around cache.get().</p>
     *
     * <p>Cache Loader uses read method which should not return <code>null</code>
     *  if this is a new cell. If the get fails, there is a different issue.</p>
     */
    private CellData fetch(String key) {
        try {
            return cache.get(key);
        } catch (ExecutionException e) {
            LOGGER.error("Problem fetching info object for {} "
                                         + "from cache: "
                                         + "{}, cause: {}.",
                         key,
                         e.getMessage(),
                         e.getCause());
            return null;
        }
    }
}
