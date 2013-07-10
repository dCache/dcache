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
package org.dcache.webadmin.view.panels.alarms;

import org.apache.wicket.extensions.ajax.markup.html.autocomplete.DefaultCssAutoCompleteTextField;
import org.apache.wicket.extensions.markup.html.form.DateTextField;
import org.apache.wicket.extensions.markup.html.repeater.util.SortableDataProvider;
import org.apache.wicket.extensions.yui.calendar.DatePicker;
import org.apache.wicket.markup.html.form.CheckBox;
import org.apache.wicket.markup.html.form.DropDownChoice;
import org.apache.wicket.markup.html.form.Radio;
import org.apache.wicket.markup.html.form.RadioGroup;
import org.apache.wicket.markup.html.form.TextField;
import org.apache.wicket.markup.html.panel.Panel;
import org.apache.wicket.model.IModel;
import org.apache.wicket.model.Model;
import org.apache.wicket.model.PropertyModel;

import java.util.Date;
import java.util.Iterator;

import org.dcache.alarms.Severity;
import org.dcache.alarms.dao.LogEntry;
import org.dcache.webadmin.controller.util.AlarmTableProvider;
import org.dcache.webadmin.view.pages.alarms.AlarmsPage;

/**
 * This section of the form controls the queries for page refresh and table
 * filtering.
 *
 * @author arossi
 */
public class QueryPanel extends Panel {

    private static final long serialVersionUID = -1214958555513880556L;
    private static final String DATE = "yyyy/MM/dd";

    public QueryPanel(String id, final AlarmsPage parent) {
        super(id);
        AlarmTableProvider provider
            = parent.getWebadminApplication().getAlarmDisplayService().getDataProvider();
        addAlarmsGroup(provider);
        addDateFields(provider);
        addSeverityChoice(provider);
        addTypeAutoComplete(parent, provider);
        addExpressionFields(provider);
        addShowClosed(provider);
        addRangeFields(provider);
        add(parent.getRefreshButton());
    }

    private void addAlarmsGroup(SortableDataProvider<LogEntry> provider) {
        IModel<Boolean> selectAlarmValue = new PropertyModel<>(provider,
                        "alarm");
        RadioGroup rgrp = new RadioGroup("selectgroup", selectAlarmValue);
        rgrp.add(new Radio("all", Model.of((Boolean) null)));
        rgrp.add(new Radio("alarmsonly", Model.of((Boolean) true)));
        rgrp.add(new Radio("noalarms", Model.of((Boolean) false)));
        add(rgrp);
    }

    private void addDateFields(SortableDataProvider<LogEntry> provider) {
        DateTextField beginning = new DateTextField("beginDate",
                        new PropertyModel<Date>(provider, "after"), DATE);
        DatePicker dp = new DatePicker();
        dp.setShowOnFieldClick(true);
        beginning.add(dp);
        add(beginning);
        DateTextField ending = new DateTextField("endDate",
                        new PropertyModel<Date>(provider, "before"), DATE);
        dp = new DatePicker();
        dp.setShowOnFieldClick(true);
        ending.add(dp);
        add(ending);
    }

    private void addExpressionFields(SortableDataProvider<LogEntry> provider) {
        IModel<String> filterValue = new PropertyModel<>(provider, "expression");
        add(new TextField<>("filterField", filterValue));
        IModel<Boolean> regexValue = new PropertyModel<>(provider, "regex");
        add(new CheckBox("regex", regexValue) {
            private static final long serialVersionUID = -5500105320665027261L;

            @Override
            protected boolean wantOnSelectionChangedNotifications() {
                return true;
            }
        });
    }

    private void addRangeFields(SortableDataProvider<LogEntry> provider) {
        IModel<Integer> from = new PropertyModel<>(provider, "from");
        add(new TextField<Integer>("rangeFrom", from));

        IModel<Integer> to = new PropertyModel<>(provider, "to");
        add(new TextField<Integer>("rangeTo", to));
    }

    private void addSeverityChoice(SortableDataProvider<LogEntry> provider) {
        IModel<String> choiceValue = new PropertyModel<>(provider, "severity");
        add(new DropDownChoice("levels", choiceValue, Severity.asList()));
    }

    private void addShowClosed(SortableDataProvider<LogEntry> provider) {
        IModel<Boolean> showClosedValue = new PropertyModel<>(provider,
                        "showClosed");
        add(new CheckBox("showClosed", showClosedValue) {
            private static final long serialVersionUID = -5500105320665027261L;

            @Override
            protected boolean wantOnSelectionChangedNotifications() {
                return true;
            }
        });
    }

    private void addTypeAutoComplete(final AlarmsPage parent,
                    final SortableDataProvider<LogEntry> provider) {
        IModel<String> filterValue = new PropertyModel<>(provider, "type");

        add(new DefaultCssAutoCompleteTextField("typeField", filterValue) {
            private static final long serialVersionUID = 2438629315170621032L;

            @Override
            protected Iterator getChoices(String input) {
                return parent.getWebadminApplication()
                                .getAlarmDisplayService()
                                .getPredefinedAlarmTypes().iterator();
            }
        });
    }
}
