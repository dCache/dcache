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

import org.apache.wicket.extensions.markup.html.repeater.util.SortParam;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.dcache.alarms.dao.LogEntry;
import org.dcache.webadmin.model.dataaccess.ILogEntryDAO;
import org.dcache.webadmin.view.beans.AbstractRegexFilterBean;
import org.dcache.webadmin.view.beans.AlarmQueryBean;
import org.dcache.webadmin.view.beans.WebAdminInterfaceSession;

/**
 * Encapsulates the in-memory datasets used to display the alarms and to update
 * the underlying data store via user input. Implements iterator as sorted
 * stream. Delegates to the session bean for the query data.
 *
 * @author arossi
 */
public class AlarmTableProvider extends
                AbstractRegexFilteringProvider<LogEntry, String> {
    private static final long serialVersionUID = 402824287543303781L;

    public Date getAfter() {
        return getAlarmQueryBean().getAfter();
    }

    public Date getBefore() {
        return getAlarmQueryBean().getBefore();
    }

    public Collection<LogEntry> getEntries() {
        return getRegexBean().getEntries();
    }

    public Integer getFrom() {
        return getAlarmQueryBean().getFrom();
    }

    public String getSeverity() {
        return getAlarmQueryBean().getSeverity();
    }

    public String getTableTitle() {
        return getAlarmQueryBean().getTableTitle();
    }

    public Integer getTo() {
        return getAlarmQueryBean().getTo();
    }

    public String getType() {
        return getAlarmQueryBean().getType();
    }

    public Boolean isAlarm() {
        return getAlarmQueryBean().isAlarm();
    }

    public boolean isShowClosed() {
        return getAlarmQueryBean().isShowClosed();
    }

    public void removeFromDeleted(LogEntry toDelete) {
        getAlarmQueryBean().removeFromDeleted(toDelete);
    }

    public void setAfter(Date after) {
        getAlarmQueryBean().setAfter(after);
    }

    public void setAlarm(Boolean alarm) {
        getAlarmQueryBean().setAlarm(alarm);
    }

    public void setBefore(Date before) {
        getAlarmQueryBean().setBefore(before);
    }

    public void setEntries(Collection<LogEntry> entries) {
        getRegexBean().setEntries(entries);
    }

    public void setFrom(Integer from) {
        getAlarmQueryBean().setFrom(from);
    }

    public void setSeverity(String severity) {
        getAlarmQueryBean().setSeverity(severity);
    }

    public void setShowClosed(boolean showClosed) {
        getAlarmQueryBean().setShowClosed(showClosed);
    }

    public void setTo(Integer to) {
        getAlarmQueryBean().setTo(to);
    }

    public void setType(String type) {
        getAlarmQueryBean().setType(type);
    }

    @Override
    protected Comparator<LogEntry> getComparator() {
        return new Comparator<LogEntry>() {

            @Override
            public int compare(LogEntry alarm0, LogEntry alarm1) {
                SortParam<String> sort = getSort();
                int dir;
                String property;
                if (sort == null) {
                    dir = -1;
                    property = "last";
                } else {
                    dir = sort.isAscending() ? 1 : -1;
                    property = sort.getProperty();
                }

                switch (property) {
                    case "first":
                        return compare(dir, alarm0.getDateOfFirstArrival(),
                                        alarm1.getDateOfFirstArrival());
                    case "last":
                        return compare(dir, alarm0.getDateOfLastUpdate(),
                                        alarm1.getDateOfLastUpdate());
                    case "severity":
                        return compare(dir, alarm0.getSeverity(),
                                        alarm1.getSeverity());
                    case "type":
                        return compare(dir, alarm0.getType(), alarm1.getType());
                    case "count":
                        return compare(dir, alarm0.getReceived(),
                                        alarm1.getReceived());
                    case "host":
                        return compare(dir, alarm0.getHost(), alarm1.getHost());
                    case "domain":
                        return compare(dir, alarm0.getDomain(),
                                        alarm1.getDomain());
                    case "service":
                        return compare(dir, alarm0.getService(),
                                        alarm1.getService());
                    default:
                        return 0;
                }
            }

            private <T extends Comparable<T>> int compare(int dir, T a, T b) {
                return (a == null) ? dir : dir * a.compareTo(b);
            }
        };
    }

    public void addToDeleted(LogEntry toDelete) {
        getAlarmQueryBean().addToDeleted(toDelete);
    }

    public void addToUpdated(LogEntry toUpdate) {
        getAlarmQueryBean().addToUpdated(toUpdate);
    }

    public void delete(ILogEntryDAO access) {
        getAlarmQueryBean().delete(access);
    }

    public boolean shouldDelete(LogEntry entry) {
        return getAlarmQueryBean().shouldDelete(entry);
    }

    public void update(ILogEntryDAO access) {
        getAlarmQueryBean().update(access);
    }

    /**
     * @return a fresh copy of the internal list, filtered for
     *         <code>expression</code> and <code>closed</code>.
     */
    @Override
    protected List<LogEntry> getFiltered() {
        List<LogEntry> entries = getRegexBean().getEntries();
        List<LogEntry> filtered = new ArrayList<>(entries);
        filterOnExpression(filtered);
        filterOnClosed(filtered);
        return filtered;
    }

    protected AlarmQueryBean getAlarmQueryBean() {
        return WebAdminInterfaceSession.getAlarmQueryBean();
    }

    @Override
    protected AbstractRegexFilterBean<LogEntry> getRegexBean() {
        return WebAdminInterfaceSession.getAlarmQueryBean();
    }

    /**
     * @param alarms
     *            assumed to be a thread-local copy, hence not synchronized.
     */
    private void filterOnClosed(List<LogEntry> alarms) {
        if (!getAlarmQueryBean().isShowClosed()) {
            for (Iterator<LogEntry> it = alarms.iterator(); it.hasNext();) {
                LogEntry entry = it.next();
                if (entry.isClosed()) {
                    it.remove();
                }
            }
        }
    }
}
