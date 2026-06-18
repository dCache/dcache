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

import dmg.util.command.Command;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.dcache.cells.json.DomainData;
import org.dcache.restful.util.domains.DomainInfoCollector;
import org.dcache.services.collector.CellDataCollectingService;

/**
 * <p>Responsible for collecting domain info via {@link DomainInfoCollector}
 * and serving it from a local cache.</p>
 */
public class DomainInfoServiceImpl extends
      CellDataCollectingService<Map<String, DomainData>, DomainInfoCollector>
      implements DomainInfoService {

    @Command(name = "domains set timeout",
          hint = "Set the timeout interval between refreshes",
          description = "Changes the interval between "
                + "queries for domain information.")
    class DomainsSetTimeoutCommand extends SetTimeoutCommand {

    }

    @Command(name = "domains refresh",
          hint = "Query for current domain info",
          description = "Interrupts current wait to run query immediately.")
    class DomainsRefreshCommand extends RefreshCommand {

    }

    @Command(name = "domains ls",
          hint = "List domain info",
          description = "Displays a list of all currently known domains "
                + "and their hostnames.")
    class DomainsLsCommand implements Callable<String> {

        @Override
        public String call() {
            return Arrays.stream(getAddresses())
                  .map(DomainInfoServiceImpl.this::getDomainData)
                  .sorted(Comparator.comparing(DomainData::getDomainName))
                  .map(DomainData::toString)
                  .collect(Collectors.joining("\n"));
        }
    }

    private volatile Map<String, DomainData> cache = Map.of();

    @Override
    public synchronized String[] getAddresses() {
        return cache.keySet().toArray(String[]::new);
    }

    @Override
    public DomainData getDomainData(String domainName) {
        DomainData cached = cache.get(domainName);
        if (cached == null) {
            cached = new DomainData();
            cached.setDomainName(domainName);
        }
        return cached;
    }

    @Override
    protected synchronized void update(Map<String, DomainData> data) {
        cache = Map.copyOf(data);
    }
}

