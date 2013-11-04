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
package org.dcache.webadmin.view.panels.selectall;

import org.apache.wicket.authroles.authorization.strategies.role.metadata.MetaDataRoleAuthorizationStrategy;
import org.apache.wicket.markup.html.form.Button;

import org.dcache.webadmin.view.panels.basepanel.BasePanel;
import org.dcache.webadmin.view.util.Role;

/**
 * Encapsulates the triad of select all, deselect all and submit buttons.
 * The submit and selection functionality depends on the concrete class.
 *
 * @author arossi
 */
public abstract class SelectAllPanel extends BasePanel {

    private static final long serialVersionUID = 8704591801936459097L;

    public SelectAllPanel(String id, Button submit) {
        super(id);
        Button selectAll = new SelectAllButton("selectAllButton");
        Button deselectAll = new DeselectAllButton("deselectAllButton");
        MetaDataRoleAuthorizationStrategy.authorize(submit, RENDER, Role.ADMIN);
        MetaDataRoleAuthorizationStrategy.authorize(selectAll, RENDER, Role.ADMIN);
        MetaDataRoleAuthorizationStrategy.authorize(deselectAll, RENDER, Role.ADMIN);
        add(submit);
        add(selectAll);
        add(deselectAll);
    }

    private class SelectAllButton extends Button {
        private static final long serialVersionUID = -8490536062145002381L;

        public SelectAllButton(String id) {
            super(id);
            this.setDefaultFormProcessing(false);
        }

        @Override
        public void onSubmit() {
            setSubmitCalled();
            setSelectionForAll(Boolean.TRUE);
        }
    }

    private class DeselectAllButton extends Button {
        private static final long serialVersionUID = 8228847931814901840L;

        public DeselectAllButton(String id) {
            super(id);
            this.setDefaultFormProcessing(false);
        }

        @Override
        public void onSubmit() {
            setSubmitCalled();
            setSelectionForAll(Boolean.FALSE);
        }
    }

    protected abstract void setSubmitCalled();

    protected abstract void setSelectionForAll(Boolean selected);
}
