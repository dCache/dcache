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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.wicket.extensions.markup.html.repeater.util.SortParam;
import org.apache.wicket.extensions.markup.html.repeater.util.SortableDataProvider;
import org.apache.wicket.model.IModel;
import org.apache.wicket.model.Model;
import org.dcache.alarms.dao.AlarmEntry;
import org.dcache.webadmin.model.dataaccess.IAlarmDAO;
import org.dcache.webadmin.model.exceptions.DAOException;

/**
 * Encapsulates the in-memory datasets used to display the alarms and to
 * update the underlying data store via user input.  Implements iterator as
 * sorted stream.
 *
 * @author arossi
 */
public class AlarmTableProvider extends SortableDataProvider<AlarmEntry> {
    private static final long serialVersionUID = 402824287543303781L;

    private final List<AlarmEntry> alarms = new ArrayList<AlarmEntry>();
    private final Set<AlarmEntry> updated = new HashSet<AlarmEntry>();
    private final Set<AlarmEntry> deleted = new HashSet<AlarmEntry>();

    private Date after;
    private Date before;
    private String severity;
    private String type;
    private String expression;
    private boolean regex;
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
        return new Date(after.getTime());
    }

    public List<AlarmEntry> getAlarms() {
        return alarms;
    }

    public Date getBefore() {
        return new Date(before.getTime());
    }

    public String getExpression() {
        return expression;
    }

    public String getSeverity() {
        return severity;
    }

    public String getType() {
        return type;
    }

    public boolean isRegex() {
        return regex;
    }

    public boolean isShowClosed() {
        return showClosed;
    }

    @Override
    public Iterator<? extends AlarmEntry> iterator(int first, int count) {
        List<AlarmEntry> data = getFiltered();
        Collections.sort(data, new Comparator<AlarmEntry>() {
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

                Comparable c0;
                Comparable c1;
                if ("date".equals(property)) {
                    c0 = alarm0.getDate();
                    c1 = alarm1.getDate();
                } else if ("severity".equals(property)) {
                    c0 = alarm0.getSeverity();
                    c1 = alarm1.getSeverity();
                } else if ("type".equals(property)) {
                    c0 = alarm0.getType();
                    c1 = alarm1.getType();
                } else if ("count".equals(property)) {
                    c0 = alarm0.getCount();
                    c1 = alarm1.getCount();
                } else if ("host".equals(property)) {
                    c0 = alarm0.getHost();
                    c1 = alarm1.getHost();
                } else if ("domain".equals(property)) {
                    c0 = alarm0.getDomain();
                    c1 = alarm1.getDomain();
                } else if ("service".equals(property)) {
                    c0 = alarm0.getService();
                    c1 = alarm1.getService();
                } else {
                    return 0;
                }
                if (c0 == null) {
                    return dir;
                }
                return dir * c0.compareTo(c1);
            }
        });

        return data.subList(first, Math.min(first + count, data.size())).iterator();
    }

    @Override
    public IModel<AlarmEntry> model(AlarmEntry object) {
        return Model.of(object);
    }

    public void removeFromDeleted(AlarmEntry toDelete) {
        synchronized (deleted) {
            deleted.remove(toDelete);
        }
    }

    public void setAfter(Date after) {
        this.after = new Date(after.getTime());
    }

    public void setAlarms(Collection<AlarmEntry> refreshed) {
        synchronized (alarms) {
            alarms.clear();
            alarms.addAll(refreshed);
        }
    }

    public void setBefore(Date before) {
        this.before = new Date(before.getTime());
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }

    public void setRegex(boolean regex) {
        this.regex = regex;
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

    @Override
    public int size() {
        return getFiltered().size();
    }

    public void update(IAlarmDAO access) throws DAOException {
        synchronized (updated) {
            if (!updated.isEmpty()) {
                access.update(updated);
                updated.clear();
            }
        }
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

    /**
     * @param alarms
     *            assumed to be a thread-local copy, hence not synchronized.
     */
    private void filterOnExpression(List<AlarmEntry> alarms) {
        if (expression != null) {
            if (regex) {
                try {
                    Pattern pattern = Pattern.compile(expression);
                    for (Iterator<AlarmEntry> it = alarms.iterator(); it.hasNext();) {
                        AlarmEntry entry = it.next();
                        if (!pattern.matcher(entry.toString()).find()) {
                            it.remove();
                        }
                    }
                } catch (PatternSyntaxException e) {
                    alarms.clear();
                    throw new IllegalArgumentException(e.getMessage(),
                                    e.getCause());
                }
            } else {
                for (Iterator<AlarmEntry> it = alarms.iterator(); it.hasNext();) {
                    AlarmEntry entry = it.next();
                    if (entry.toString().indexOf(expression) < 0) {
                        it.remove();
                    }
                }
            }
        }
    }

    /**
     * @return a fresh copy of the internal list, filtered for
     *         <code>expression</code> and <code>closed</code>.
     */
    private List<AlarmEntry> getFiltered() {
        List<AlarmEntry> filtered;
        synchronized (alarms) {
            filtered = new ArrayList<AlarmEntry>(alarms);
        }
        filterOnExpression(filtered);
        filterOnClosed(filtered);
        return filtered;
    }
}
