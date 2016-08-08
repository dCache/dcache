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

import org.apache.wicket.AttributeModifier;
import org.apache.wicket.ajax.AjaxRequestTarget;
import org.apache.wicket.extensions.ajax.markup.html.AjaxEditableLabel;
import org.apache.wicket.extensions.markup.html.repeater.data.grid.ICellPopulator;
import org.apache.wicket.extensions.markup.html.repeater.data.table.DataTable;
import org.apache.wicket.extensions.markup.html.repeater.data.table.HeadersToolbar;
import org.apache.wicket.extensions.markup.html.repeater.data.table.IColumn;
import org.apache.wicket.extensions.markup.html.repeater.data.table.NavigationToolbar;
import org.apache.wicket.extensions.markup.html.repeater.data.table.PropertyColumn;
import org.apache.wicket.extensions.model.AbstractCheckBoxModel;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.panel.Panel;
import org.apache.wicket.markup.repeater.Item;
import org.apache.wicket.model.IModel;
import org.apache.wicket.model.Model;

import java.util.ArrayList;
import java.util.List;

import org.dcache.alarms.LogEntry;
import org.dcache.webadmin.controller.util.AlarmTableProvider;
import org.dcache.webadmin.view.pages.alarms.AlarmsPage;
import org.dcache.webadmin.view.panels.alarms.CheckPanel.CheckBoxColumn;

/**
 * This section of the form holds the main table displaying the alarms.
 *
 * @author arossi
 */
public class DisplayPanel extends Panel {

    private static final long serialVersionUID = -4499489059537621331L;

    private CheckBoxColumn<LogEntry, String> delete;
    private CheckBoxColumn<LogEntry, String> close;

    public DisplayPanel(String id, final AlarmsPage parent) {
        super(id);
        List<IColumn<LogEntry, String>> columns
            = new ArrayList<>();
        final AlarmTableProvider provider
            = parent.getWebadminApplication().getAlarmDisplayService()
                    .getDataProvider();
        addDeleteColumn(columns, provider);
        addCloseColumn(columns, provider);
        addAttributeColumns(columns);
        addNotesColumn(columns, provider);

        add(new Label("tableTitle", "ALARMS"));
        DataTable<LogEntry, String> table
            = new DataTable<LogEntry, String>("alarms", columns, provider, 100) {
                private static final long serialVersionUID = -6574880701979145714L;
                protected Item<LogEntry> newRowItem(final String id,
                                final int index, final IModel<LogEntry> model) {
                    Item<LogEntry> item = super.newRowItem(id, index, model);
                    item.add(AttributeModifier.replace("style", "color: #880000;"));
                    return item;
                }
            };
        table.addBottomToolbar(new NavigationToolbar(table));
        table.addTopToolbar(new HeadersToolbar(table, provider));

        add(table);
    }

    public void clearHeaders() {
        delete.clearHeader();
        close.clearHeader();
    }

    private void addAttributeColumns(List<IColumn<LogEntry, String>> columns) {
        columns.add(new PropertyColumn<LogEntry, String>(Model.of("First"), "first",
                        "formattedDateOfFirstArrival"));
        columns.add(new PropertyColumn<LogEntry, String>(Model.of("Last"), "last",
                        "formattedDateOfLastUpdate"));
        columns.add(new PropertyColumn<LogEntry, String>(Model.of("Type"), "type",
                        "type"));
        columns.add(new PropertyColumn<LogEntry, String>(Model.of("Count"), "received",
                        "received"));
        columns.add(new PropertyColumn<LogEntry, String>(Model.of("Host"), "host",
                        "host"));
        columns.add(new PropertyColumn<LogEntry, String>(Model.of("Domain"),
                        "domain", "domain"));
        columns.add(new PropertyColumn<LogEntry, String>(Model.of("Service"),
                        "service", "service"));
        columns.add(new PropertyColumn<LogEntry, String>(Model.of("Info"), "info",
                        "info"));
    }

    private void addCloseColumn(List<IColumn<LogEntry, String>> columns,
                    final AlarmTableProvider provider) {
        close = new CheckBoxColumn<LogEntry, String>("Close", Model.of("Close")) {
            private static final long serialVersionUID = -7237325512597811741L;

            @Override
            protected IModel<Boolean> newCheckBoxModel(
                            final IModel<LogEntry> rowModel) {
                return new AbstractCheckBoxModel() {
                    private static final long serialVersionUID = 3616011392436379060L;

                    @Override
                    public void detach() {
                        rowModel.detach();
                    }

                    @Override
                    public boolean isSelected() {
                        return rowModel.getObject().isClosed();
                    }

                    @Override
                    public void select() {
                        LogEntry entry = rowModel.getObject();
                        entry.setClosed(true);
                        provider.addToUpdated(entry);
                    }

                    @Override
                    public void unselect() {
                        LogEntry entry = rowModel.getObject();
                        entry.setClosed(false);
                        provider.addToUpdated(entry);
                    }
                };
            }
        };

        columns.add(close);
    }

    private void addDeleteColumn(List<IColumn<LogEntry, String>> columns,
                    final AlarmTableProvider provider) {
        delete = new CheckBoxColumn<LogEntry, String>("Delete", Model.of("Delete")) {
            private static final long serialVersionUID = -7237325512597811741L;

            @Override
            protected IModel<Boolean> newCheckBoxModel(
                            final IModel<LogEntry> rowModel) {
                return new AbstractCheckBoxModel() {
                    private static final long serialVersionUID = 3616011392436379060L;

                    @Override
                    public void detach() {
                        rowModel.detach();
                    }

                    @Override
                    public boolean isSelected() {
                        return provider.shouldDelete(rowModel.getObject());
                    }

                    @Override
                    public void select() {
                        provider.addToDeleted(rowModel.getObject());
                    }

                    @Override
                    public void unselect() {
                        provider.removeFromDeleted(rowModel.getObject());
                    }
                };
            }
        };

        columns.add(delete);
    }

    private void addNotesColumn(List<IColumn<LogEntry, String>> columns,
                    final AlarmTableProvider provider) {
        columns.add(new PropertyColumn<LogEntry, String>(Model.of("Notes"), "notes") {

            private static final long serialVersionUID = 7225406229492621282L;

            @Override
            public void populateItem(
                            final Item<ICellPopulator<LogEntry>> item,
                            final String componentId,
                            final IModel<LogEntry> rowModel) {
                item.add(new AjaxEditableLabel(componentId,
                                               getDataModel(rowModel)) {
                    private static final long serialVersionUID = -6235564987318418284L;

                    @Override
                    protected void onSubmit(final AjaxRequestTarget target) {
                        super.onSubmit(target);
                        provider.addToUpdated(rowModel.getObject());
                    }
                });
            }
        });
    }
}
