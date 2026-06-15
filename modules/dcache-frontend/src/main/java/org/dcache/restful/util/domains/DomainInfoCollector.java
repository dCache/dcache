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
package org.dcache.restful.util.domains;

import diskCacheV111.util.SpreadAndWait;
import dmg.cells.nucleus.CellPath;
import dmg.cells.services.GetAllDomainsReply;
import dmg.cells.services.GetAllDomainsRequest;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import org.dcache.cells.json.DomainData;
import org.dcache.util.collector.CellMessagingCollector;

/**
 * <p>Collects domain names from the Routing Manager and their hostnames
 * from each domain's System cell, returning a map of domain name to
 * {@link DomainData}.</p>
 */
public final class DomainInfoCollector extends CellMessagingCollector<Map<String, DomainData>> {

    @Override
    public Map<String, DomainData> collectData() throws InterruptedException {
        GetAllDomainsReply reply;

        try {
            reply = stub.send(new CellPath("RoutingMgr"),
                    new GetAllDomainsRequest(),
                    GetAllDomainsReply.class).get();
        } catch (ExecutionException e) {
            LOGGER.error("Could not contact Routing Manager: {}, {}.",
                    e.getMessage(), String.valueOf(e.getCause()));
            return Collections.emptyMap();
        }

        Map<String, Collection<String>> domains = reply.getDomains();

        /*
         * Fan out "get hostname" to the System cell of every domain.
         * SpreadAndWait silently drops domains that don't respond, so
         * those simply won't appear in getReplies().
         */
        SpreadAndWait<String> spreader = new SpreadAndWait<>(stub);
        for (String domainName : domains.keySet()) {
            spreader.send(new CellPath("System", domainName), String.class, "get hostname");
        }
        spreader.waitForReplies();

        Map<String, DomainData> result = new TreeMap<>();

        /*
         * Build DomainData for each domain that replied.
         * toAddressString() for new CellPath("System", "dCacheDomain") returns "System@dCacheDomain".
         */
        for (Map.Entry<CellPath, String> entry : spreader.getReplies().entrySet()) {
            String addressString = entry.getKey().toAddressString();
            int atIndex = addressString.indexOf('@');
            if (atIndex >= 0) {
                String domainName = addressString.substring(atIndex + 1);
                DomainData data = new DomainData();
                data.setDomainName(domainName);
                data.setHostname(entry.getValue());
                result.put(domainName, data);
            }
        }

        /*
         * Include domains that did not reply (hostname will be null).
         */
        for (String domainName : domains.keySet()) {
            result.computeIfAbsent(domainName, name -> {
                DomainData data = new DomainData();
                data.setDomainName(name);
                return data;
            });
        }

        return result;
    }
}

