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
package org.dcache.vehicles.billing;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.Message;

import org.dcache.services.billing.db.data.RecordEntry;
import org.dcache.util.FieldSort;

/**
 * <p>Base class for messages requesting billing records.</p>
 */
public abstract class RecordRequestMessage<T extends RecordEntry>
                extends Message {
    private static final long serialVersionUID = 8715103130379892004L;

    public enum Type {
        READ, WRITE, P2P, STORE, RESTORE
    }

    private final PnfsId pnfsId;
    private final Date   before;
    private final Date   after;
    private final int    offset;
    private final int    limit;
    private final Type   type;
    private final String sort;

    private List<T> records;
    private int     total;

    protected RecordRequestMessage(PnfsId pnfsId,
                                   Date before,
                                   Date after,
                                   Type type,
                                   int limit,
                                   int offset,
                                   String sort) {
        this.pnfsId = pnfsId;
        this.before = before;
        this.after = after;
        this.type = type;
        this.limit = limit;
        this.offset = offset;
        this.sort = sort;
    }

    public void buildDAOQuery(StringBuilder filter,
                              StringBuilder params,
                              List<Object> values) {
        filter.append("pnfsid == pnfsid1 && type == type1");
        params.append("java.lang.String pnfsid1, java.lang.String type1");
        values.add(pnfsId.toString());
        values.add(type.name());

        if (before != null) {
            filter.append(" && datestamp <= before");
            params.append(", java.util.Date before");
            values.add(before);
        }

        if (after != null) {
            filter.append(" && datestamp >= after");
            params.append(", java.util.Date after");
            values.add(after);
        }
    }

    public Date getAfter() {
        return after;
    }

    public Date getBefore() {
        return before;
    }

    public int getLimit() {
        return limit;
    }

    public int getOffset() {
        return offset;
    }

    public PnfsId getPnfsId() {
        return pnfsId;
    }

    public List<T> getRecords() {
        return records;
    }

    public int getTotal() {
        return total;
    }

    public Type getType() {
        return type;
    }

    public void setRecords(List<T> records) {
        this.records = records;
    }

    public void setTotal(int total) {
        this.total = total;
    }

    public List<FieldSort> sortList() {
        if (sort == null) {
            return Collections.EMPTY_LIST;
        }

        return Arrays.stream(sort.split(","))
                     .map(FieldSort::new)
                     .collect(Collectors.toList());
    }

    public abstract Predicate<T> filter();
}
