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
package org.dcache.alarms.dao;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import javax.jdo.FetchPlan;
import javax.jdo.PersistenceManager;
import javax.jdo.Query;
import javax.jdo.Transaction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.dcache.alarms.LogEntry;

/**
 * Convenience methods for generating JDO queries from {@link LogEntry}
 * fields.
 *
 * @author arossi
 */
public class AlarmJDOUtils {
    private static final int MAXIMUM_QUERY_RESULTS = 10000;

    public static class AlarmDAOFilter {
        private String filter;
        private String parameters;
        private Object[] values;
        private Integer rangeStart;
        private Integer rangeEnd;

        @Override
        public String toString() {
            return filter + ", " + parameters
                    + (values == null ? null : ", " + Arrays.asList(values));
        }

        public String getFilter() {
            return filter;
        }

        public Integer getRangeStart() {
            return rangeStart;
        }

        public String getParameters() {
            return parameters;
        }

        public Integer getRangeEnd() {
            return rangeEnd;
        }

        public Object[] getValues() {
            if (values == null) {
                return null;
            }
            return Arrays.asList(values).toArray();
        }

        private void normalizeRange() {
            if (rangeStart != null) {
                if (rangeEnd == null) {
                    rangeEnd = Integer.MAX_VALUE;
                }
            } else if (rangeEnd != null) {
                if (rangeStart == null) {
                    rangeStart = 0;
                }
            }
        }
    }

    public static long delete(PersistenceManager pm, AlarmDAOFilter filter) {
        Query query = AlarmJDOUtils.createQuery(pm, filter);
        return filter.values == null ? query.deletePersistentAll()
                        : query.deletePersistentAll(filter.values);
    }

    public static Collection<LogEntry> execute(PersistenceManager pm,
                    AlarmDAOFilter filter) {
        Query query = AlarmJDOUtils.createQuery(pm, filter);

        /*
         * 2013/12/11 -- added a range limit guard.  This can be hard-coded
         * as effectively the capacity to hold more than 10000 entries in
         * memory should not be required.  One can always adjust the numbers or
         * refine the query.
         */
        Integer from = filter.rangeStart == null ? 0
                        : filter.rangeStart;
        int limit = from + MAXIMUM_QUERY_RESULTS;
        Integer to = filter.rangeEnd == null ? limit
                        : Math.min(filter.rangeEnd, limit);
        query.setRange(from, to);

        /*
         * evidently required by DataNucleus 3.1.3+ to get most recent
         * updates from other JVMs
         */
        query.setIgnoreCache(true);
        return (Collection<LogEntry>) (filter.values == null ? query.execute()
                        : query.executeWithArray(filter.values));
    }

    /**
     * Construct filter based on values for the before and closed (AND'd).
     */
    public static AlarmDAOFilter getDeleteBeforeFilter(Long before) {
        Preconditions.checkNotNull(before);
        AlarmDAOFilter filter = new AlarmDAOFilter();
        filter.filter = "lastUpdate<=b && closed==t";
        filter.parameters = "java.lang.Long b, java.lang.Boolean t";
        filter.values = new Object[] { before, true };
        return filter;
    }

    /**
     * Construct filter based on values for the parameter fields (AND'd).
     * <br>
     *
     * @param after
     *            closed lower bound (>=) of date range; may be
     *            <code>null</code>.
     * @param before
     *            closed upper bound (<=) of date range; may be
     *            <code>null</code>.
     * @param type
     *            may be <code>null</code>.
     * @param isAlarm
     *            may be <code>null</code>.
     * @param rangeStart
     *            range beginning
     *            may be <code>null</code>.
     * @param rangeEnd
     *            range ending
     *            may be <code>null</code>.
     */
    public static AlarmDAOFilter getFilter(Date after,
                                           Date before,
                                           String type,
                                           Boolean isAlarm,
                                           Integer rangeStart,
                                           Integer rangeEnd) {
        StringBuilder f = new StringBuilder();
        StringBuilder p = new StringBuilder();
        List<Object> values = new ArrayList<>();

        if (after != null) {
            f.append("lastUpdate>=a");
            p.append("java.lang.Long a");
            values.add(after.getTime());
        }

        if (before != null) {
            if (f.length() > 0) {
                f.append(" && ");
                p.append(", ");
            }
            f.append("lastUpdate<=b");
            p.append("java.lang.Long b");
            values.add(before.getTime());
        }

        if (type != null) {
            if (f.length() > 0) {
                f.append(" && ");
                p.append(", ");
            }
            f.append("type==t");
            p.append("java.lang.String t");
            values.add(type);
        }

        if (isAlarm != null) {
            if (f.length() > 0) {
                f.append(" && ");
                p.append(", ");
            }
            f.append("alarm==l");
            p.append("java.lang.Boolean l");
            values.add(isAlarm);
        }

        AlarmDAOFilter filter = new AlarmDAOFilter();
        filter.filter = trimToNull(f);
        filter.parameters = trimToNull(p);
        filter.values = emptyListToNull(values);
        filter.rangeStart = rangeStart;
        filter.rangeEnd = rangeEnd;
        return filter;
    }

    /**
     * Construct filter from multiple alarm entry keys (OR'd).
     */
    public static AlarmDAOFilter getIdFilter(Collection<LogEntry> selected) {
        StringBuilder f = new StringBuilder();
        StringBuilder p = new StringBuilder();
        List<Object> values = new ArrayList<>();
        Iterator<LogEntry> i = selected.iterator();
        int k = 0;

        if (i.hasNext()) {
            f.append("key==k").append(k);
            p.append("java.lang.String k").append(k++);
            values.add(i.next().getKey());
        }

        /*
         * create successive 'or' clauses if any
         */
        while (i.hasNext()) {
            f.append(" || key==k").append(k);
            p.append(", java.lang.String k").append(k++);
            values.add(i.next().getKey());
        }

        AlarmDAOFilter filter = new AlarmDAOFilter();
        filter.filter = Strings.emptyToNull(f.toString());
        filter.parameters = Strings.emptyToNull(p.toString());
        filter.values = emptyListToNull(values);
        return filter;
    }

    public static void rollbackIfActive(Transaction tx) {
        if (tx.isActive()) {
            tx.rollback();
        }
    }

    /**
     * Construct an actual JDO query from the filter.
     */
    private static Query createQuery(PersistenceManager pm,
                                     AlarmDAOFilter filter) {
        filter.normalizeRange();

        Query query = pm.newQuery(LogEntry.class);
        query.setFilter(filter.filter);
        query.declareParameters(filter.parameters);
        query.addExtension("datanucleus.query.resultCacheType", "none");
        query.addExtension("datanucleus.rdbms.query.resultSetType",
                           "scroll-insensitive");
        query.getFetchPlan().setFetchSize(FetchPlan.FETCH_SIZE_OPTIMAL);
        return query;
    }

    private static Object[] emptyListToNull(List<Object> values) {
        return values.isEmpty() ? null : values.toArray();
    }

    private static String trimToNull(StringBuilder buffer) {
        String trimmed = buffer.toString().trim();
        if (trimmed.isEmpty()) {
            return null;
        }
        return trimmed;
    }

    private AlarmJDOUtils() {
    }
}
