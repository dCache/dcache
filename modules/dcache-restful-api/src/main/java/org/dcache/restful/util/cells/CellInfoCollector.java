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
package org.dcache.restful.util.cells;

import com.google.common.util.concurrent.Futures;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellInfoAware;
import dmg.cells.nucleus.CellPath;
import dmg.cells.services.GetAllDomainsReply;
import dmg.cells.services.GetAllDomainsRequest;
import org.dcache.util.collector.CellMessagingCollector;
import org.dcache.util.collector.ListenableFutureWrapper;

/**
 * <p>This collector provides a thin layer on top of cell adapter
 *      functionality.  It is mainly responsible for sending messages
 *      and returning message reply futures.  The principal collect method
 *      scatter/gathers requests for {@link CellInfo} to all the well-known
 *      cells of all domains visible to the Routing Manager.</p>
 */
public final class CellInfoCollector extends
                CellMessagingCollector<Map<String, ListenableFutureWrapper<CellInfo>>>
                implements CellInfoAware {

    private Supplier<CellInfo>     supplier;

    @Override
    public Map<String, ListenableFutureWrapper<CellInfo>> collectData()
                    throws InterruptedException {
        GetAllDomainsReply reply;

        try {
            reply = stub.send(new CellPath("RoutingMgr"),
                              new GetAllDomainsRequest(),
                              GetAllDomainsReply.class).get();
        } catch (ExecutionException e) {
            LOGGER.error("Could not contact Routing Manager: {}, {}.",
                         e.getMessage(), String.valueOf(e.getCause()));
            return Collections.EMPTY_MAP;
        }

        Map<String, ListenableFutureWrapper<CellInfo>> map = new TreeMap<>();

        CellInfo frontendInfo = supplier.get();

        /*
         *  Remove Frontend, and substitute with the supplier.
         *  Otherwise, the message fails.
         */
        Collection<String> cells
                        = reply.getDomains().get(frontendInfo.getDomainName());
        cells.remove(frontendInfo.getCellName());

        reply.getDomains().entrySet()
             .stream()
             .map(this::getCellPaths)
             .flatMap(Collection::stream)
             .map(this::getCellInfo)
                    .forEach((future) -> map.put(future.getKey(), future));

        ListenableFutureWrapper<CellInfo> wrapper = new ListenableFutureWrapper<>();
        wrapper.setKey(frontendInfo.getCellName() + "@" + frontendInfo.getDomainName());
        wrapper.setSent(System.currentTimeMillis());
        wrapper.setFuture(Futures.immediateFuture(frontendInfo));
        map.put(wrapper.getKey(), wrapper);

        return map;
    }

    @Override
    public void setCellInfoSupplier(Supplier<CellInfo> supplier) {
        this.supplier = supplier;
    }

    private List<CellPath> getCellPaths(Entry<String, Collection<String>> entry) {
        return entry.getValue().stream()
                    .map(cell -> new CellPath(cell, entry.getKey()))
                    .collect(Collectors.toList());
    }

    private ListenableFutureWrapper<CellInfo> getCellInfo(CellPath path) {
        ListenableFutureWrapper<CellInfo> wrapper = new ListenableFutureWrapper<>();
        wrapper.setKey(path.toAddressString());
        wrapper.setSent(System.currentTimeMillis());
        wrapper.setFuture(stub.send(path, "xgetcellinfo", CellInfo.class));
        return wrapper;
    }
}
