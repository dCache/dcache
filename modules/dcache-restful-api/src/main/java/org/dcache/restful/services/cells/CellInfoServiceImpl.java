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

import java.util.Arrays;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.util.command.Command;
import org.dcache.restful.services.admin.CellDataCollectingService;
import org.dcache.restful.util.admin.ReadWriteData;
import org.dcache.restful.util.cells.CellInfoCollector;
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

    private final ReadWriteData<String, CellData> cache
                    = new ReadWriteData<>(true);

    private String[] currentKnownCells = new String[0];

    /**
     * <p>Does the brunt of the updating work on the data returned
     * by the collector.</p>
     */
    private CellInfoFutureProcessor processor;

    @Override
    public synchronized String[] getAddresses() {
        return currentKnownCells;
    }

    @Override
    public CellData getCellData(String address) {
        CellData cached = cache.read(address);
        if (cached == null) {
            throw new NoSuchElementException(address);
        }
        return cached;
    }

    public void setProcessor(CellInfoFutureProcessor processor) {
        this.processor = processor;
    }

    /**
     * <p>Callback invoked by processor when it has completed
     *    the update.</p>
     *
     * @param next updated data.
     */
    public void updateCache(Map<String, CellData> next) {
        cache.clearAndWrite(next);
    }

    /**
     * <p>Delegates processing to the processor.  Refreshes the list of
     *    known cells.</p>
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
}
