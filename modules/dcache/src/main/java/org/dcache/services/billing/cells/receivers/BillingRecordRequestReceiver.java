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
package org.dcache.services.billing.cells.receivers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import dmg.cells.nucleus.CellMessageReceiver;

import diskCacheV111.util.PnfsId;

import org.dcache.services.billing.db.IBillingInfoAccess;
import org.dcache.services.billing.db.data.RecordEntry;
import org.dcache.services.billing.db.data.StorageRecord;
import org.dcache.services.billing.db.data.TransferRecord;
import org.dcache.util.FieldSort;
import org.dcache.vehicles.billing.RecordRequestMessage;
import org.dcache.vehicles.billing.StorageRecordRequestMessage;
import org.dcache.vehicles.billing.TransferRecordRequestMessage;

/**
 * <p>Serves up record data for a given file.  An optional date range
 *    can be used to limit the search.</p>
 */
public final class BillingRecordRequestReceiver implements CellMessageReceiver {
    private static <R extends RecordEntry>
            Function<FieldSort, Comparator<R>> nextComparator(Class<R> clzz) {
        if (clzz.isAssignableFrom(TransferRecord.class)) {
            return (sort) -> {
                Comparator<TransferRecord> comparator;
                switch (sort.getName()) {
                    case "date":
                        comparator = Comparator.comparing(TransferRecord::getDateStamp);
                        break;
                    case "pool":
                        comparator = Comparator.comparing(TransferRecord::getCellName);
                        break;
                    case "door":
                        comparator = Comparator.comparing(TransferRecord::getInitiator);
                        break;
                    case "serverPool":
                        comparator = Comparator.comparing(TransferRecord::getCellName);
                        break;
                    case "clientPool":
                        comparator = Comparator.comparing(TransferRecord::getInitiator);
                        break;
                    case "client":
                        comparator = Comparator.comparing(TransferRecord::getClient);
                        break;
                    case "connected":
                        comparator = Comparator.comparing(TransferRecord::getConnectionTime);
                        break;
                    case "queued":
                        comparator = Comparator.comparing(TransferRecord::getQueuedTime);
                        break;
                    case "trasferred":
                        comparator = Comparator.comparing(TransferRecord::getTransferSize);
                        break;
                    case "error":
                        comparator = Comparator.comparing(TransferRecord::getErrorMessage);
                        break;
                    default:
                        throw new IllegalArgumentException(
                                        "sort field " + sort.getName()
                                                        + " not supported.");
                }

                if (sort.isReverse()) {
                    return (Comparator<R>)comparator.reversed();
                }

                return (Comparator<R>)comparator;
            };
        } else if (clzz.isAssignableFrom(StorageRecord.class)) {
            return (sort) -> {
                Comparator<StorageRecord> comparator;
                switch (sort.getName()) {
                    case "date":
                        comparator = Comparator.comparing(StorageRecord::getDateStamp);
                        break;
                    case "pool":
                        comparator = Comparator.comparing(StorageRecord::getCellName);
                        break;
                    case "connected":
                        comparator = Comparator.comparing(StorageRecord::getConnectionTime);
                        break;
                    case "queued":
                        comparator = Comparator.comparing(StorageRecord::getQueuedTime);
                        break;
                    case "error":
                        comparator = Comparator.comparing(StorageRecord::getErrorMessage);
                        break;
                    default:
                        throw new IllegalArgumentException(
                                        "sort field " + sort.getName()
                                                        + " not supported.");
                }

                if (sort.isReverse()) {
                    return (Comparator<R>)comparator.reversed();
                }

                return (Comparator<R>) comparator;
            };
        }

        throw new IllegalArgumentException(
                        "record entry class " + clzz + " not supported.");
    }

    private IBillingInfoAccess access;

    public TransferRecordRequestMessage messageArrived(
                    TransferRecordRequestMessage request) {
        return process(request, TransferRecord.class);
    }

    public StorageRecordRequestMessage messageArrived(
                    StorageRecordRequestMessage request) {
        return process(request, StorageRecord.class);
    }

    public void setAccess(IBillingInfoAccess access) {
        this.access = access;
    }

    private <R extends RecordEntry & Comparable<R>, M extends RecordRequestMessage<R>>
            M process(M request, Class<R> clzz) {

        if (access == null) {
            request.setFailed(-1,
                              "No database connection; "
                                              + "cannot provide record data.");
            return request;
        }

        PnfsId pnfsid = request.getPnfsId();

        if (pnfsid == null) {
            request.setFailed(-1,
                              "PnfsId must be provided.");
            return request;
        }

        request.clearReply();

        StringBuilder query = new StringBuilder();
        StringBuilder params = new StringBuilder();
        List<Object> values = new ArrayList<>();

        request.buildDAOQuery(query, params, values);

        Collection<R> result = access.get(clzz,
                                          query.toString(),
                                          params.toString(),
                                          values.toArray());

        Comparator<R> sorter = FieldSort.getSorter(request.sortList(),
                                                   nextComparator(clzz));
        List<R> records = result.stream()
                                .filter(request.filter())
                                .sorted(sorter)
                                .collect(Collectors.toList());

        request.setTotal(records.size());

        int offset = request.getOffset();
        int limit = request.getLimit();
        request.setRecords(records.stream()
                                  .skip(offset)
                                  .limit(limit)
                                  .collect(Collectors.toList()));

        request.setSucceeded();

        return request;
    }
}
