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

import org.apache.wicket.Component;
import org.apache.wicket.behavior.Behavior;
import org.apache.wicket.extensions.markup.html.repeater.data.grid.ICellPopulator;
import org.apache.wicket.extensions.markup.html.repeater.data.table.AbstractColumn;
import org.apache.wicket.markup.ComponentTag;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.form.CheckBox;
import org.apache.wicket.markup.html.panel.Panel;
import org.apache.wicket.markup.repeater.Item;
import org.apache.wicket.model.IModel;
import org.apache.wicket.model.Model;

import java.util.UUID;

/**
 * Provides javascript functionality for selecting and deselecting all
 * checkboxes in a data table column based on a checkbox in the table header for
 * that column.
 *
 * @author arossi
 */
public final class CheckPanel extends Panel {
    private static final long serialVersionUID = 4172391497442451620L;

    private final CheckBox checkBox;

    public abstract static class CheckBoxColumn<T, S> extends AbstractColumn<T, S> {
        private static final long serialVersionUID = 8433056726333659452L;

        private final String uuid
            = UUID.randomUUID().toString().replace("-", "");
        private final String headerLabel;
        private CheckPanel headerPanel;

        public CheckBoxColumn(String headerLabel, IModel<String> displayModel) {
            super(displayModel);
            this.headerLabel = headerLabel;
        }

        @Override
        public void populateItem(Item<ICellPopulator<T>> cellItem,
                        String componentId, IModel<T> rowModel) {
            CheckPanel panel = new CheckPanel(componentId,
                            newCheckBoxModel(rowModel), uuid);
            cellItem.add(panel);
        }

        protected abstract IModel<Boolean> newCheckBoxModel(IModel<T> rowModel);

        @Override
        public Component getHeader(String componentId) {
            headerPanel = new CheckPanel(componentId, headerLabel,
                            new Model<Boolean>(), uuid);
            headerPanel.get("check").add(new Behavior() {
                private static final long serialVersionUID = -4210875030052297922L;

                @Override
                public void onComponentTag(Component component, ComponentTag tag) {
                    tag.put("onclick", "checkAll(this, '" + uuid + "')");
                }
            });

            return headerPanel;
        }

        public void clearHeader() {
           if (headerPanel !=null) {
               headerPanel.checkBox.setModelObject(false);
           }
        }
    }

    public CheckPanel(String id, String label, IModel<Boolean> checkModel,
                    String uuid) {
        super(id);
        add(new Label("label", label));
        checkBox = newCheckBox("check", checkModel, uuid);
        add(checkBox);
    }

    public CheckPanel(String id, IModel<Boolean> checkModel, String uuid) {
        /*
         * label is in the mark-up so it needs to be added here
         */
        this(id, "", checkModel, uuid);
    }

    protected CheckBox newCheckBox(String id, IModel<Boolean> checkModel,
                    final String uuid) {
        return new CheckBox("check", checkModel) {
            private static final long serialVersionUID = -1397557248166283200L;

            @Override
            protected void onComponentTag(ComponentTag tag) {
                super.onComponentTag(tag);
                tag.append("class", uuid, " ");
            }
        };
    }
}
