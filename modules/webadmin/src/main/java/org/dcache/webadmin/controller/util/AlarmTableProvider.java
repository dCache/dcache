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
package org.dcache.webadmin.controller.util;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.wicket.extensions.markup.html.repeater.util.SortParam;
import org.dcache.alarms.dao.AlarmEntry;
import org.dcache.webadmin.model.dataaccess.IAlarmDAO;
import org.dcache.webadmin.model.exceptions.DAOException;

/**
 * Encapsulates the in-memory datasets used to display the alarms and to update
 * the underlying data store via user input. Implements iterator as sorted
 * stream.
 * 
 * @author arossi
 */
public class AlarmTableProvider extends
                AbstractRegexFilteringProvider<AlarmEntry> {
    private static final long serialVersionUID = 402824287543303781L;

    private final Set<AlarmEntry> updated = new HashSet<>();
    private final Set<AlarmEntry> deleted = new HashSet<>();

    private Date after;
    private Date before;
    private String severity;
    private String type;
    private boolean showClosed;

    public void addToDeleted(AlarmEntry toDelete) {
        synchronized (deleted) {
            deleted.add(toDelete);
        }
    }

    public void addToUpdated(AlarmEntry toUpdate) {
        synchronized (updated) {
            updated.add(toUpdate);
        }
    }

    public void delete(IAlarmDAO access) throws DAOException {
        synchronized (deleted) {
            if (!deleted.isEmpty()) {
                access.remove(deleted);
                deleted.clear();
            }
        }
    }

    public Date getAfter() {
        if (after == null) {
            return null;
        }
        return new Date(after.getTime());
    }

    public Date getBefore() {
        if (before == null) {
            return null;
        }
        return new Date(before.getTime());
    }

    public String getSeverity() {
        return severity;
    }

    public String getType() {
        return type;
    }

    public boolean isShowClosed() {
        return showClosed;
    }

    public void removeFromDeleted(AlarmEntry toDelete) {
        synchronized (deleted) {
            deleted.remove(toDelete);
        }
    }

    public void setAfter(Date after) {
        if (after == null) {
            this.after = null;
        } else {
            this.after = new Date(after.getTime());
        }
    }

    public void setBefore(Date before) {
        if (before == null) {
            this.before = null;
        } else {
            this.before = new Date(before.getTime());
        }
    }

    public void setSeverity(String severity) {
        this.severity = severity;
    }

    public void setShowClosed(boolean showClosed) {
        this.showClosed = showClosed;
    }

    public void setType(String type) {
        this.type = type;
    }

    public boolean shouldDelete(AlarmEntry entry) {
        synchronized (deleted) {
            return deleted.contains(entry);
        }
    }

    public void update(IAlarmDAO access) throws DAOException {
        synchronized (updated) {
            if (!updated.isEmpty()) {
                access.update(updated);
                updated.clear();
            }
        }
    }

    @Override
    protected Comparator<AlarmEntry> getComparator() {
        return new Comparator<AlarmEntry>() {

            private <T extends Comparable<T>> int compare(int dir, T a, T b)
            {
                return (a == null) ? dir : dir * a.compareTo(b);
            }

            @Override
            public int compare(AlarmEntry alarm0, AlarmEntry alarm1) {
                SortParam sort = getSort();
                int dir;
                String property;
                if (sort == null) {
                    dir = -1;
                    property = "date";
                } else {
                    dir = sort.isAscending() ? 1 : -1;
                    property = sort.getProperty();
                }

                switch (property) {
                case "date":
                    return compare(dir, alarm0.getDate(), alarm1.getDate());
                case "severity":
                    return compare(dir, alarm0.getSeverity(), alarm1.getSeverity());
                case "type":
                    return compare(dir, alarm0.getType(), alarm1.getType());
                case "count":
                    return compare(dir, alarm0.getCount(), alarm1.getCount());
                case "host":
                    return compare(dir, alarm0.getHost(), alarm1.getHost());
                case "domain":
                    return compare(dir, alarm0.getDomain(), alarm1.getDomain());
                case "service":
                    return compare(dir, alarm0.getService(), alarm1.getService());
                default:
                    return 0;
                }
            }
        };
    }

    /**
     * @return a fresh copy of the internal list, filtered for
     *         <code>expression</code> and <code>closed</code>.
     */
    @Override
    protected List<AlarmEntry> getFiltered() {
        List<AlarmEntry> filtered;
        synchronized (entries) {
            filtered = new ArrayList<>(entries);
        }
        filterOnExpression(filtered);
        filterOnClosed(filtered);
        return filtered;
    }

    /**
     * @param alarms
     *            assumed to be a thread-local copy, hence not synchronized.
     */
    private void filterOnClosed(List<AlarmEntry> alarms) {
        if (!showClosed) {
            for (Iterator<AlarmEntry> it = alarms.iterator(); it.hasNext();) {
                AlarmEntry entry = it.next();
                if (entry.isClosed()) {
                    it.remove();
                }
            }
        }
    }
}
