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
package org.dcache.webadmin.view.pages.alarms;

import org.apache.wicket.markup.head.IHeaderResponse;
import org.apache.wicket.markup.head.JavaScriptHeaderItem;
import org.apache.wicket.markup.html.form.Button;
import org.apache.wicket.markup.html.form.Form;
import org.apache.wicket.protocol.https.RequireHttps;

import org.dcache.webadmin.view.pages.AuthenticatedWebPage;
import org.dcache.webadmin.view.pages.basepage.BasePage;
import org.dcache.webadmin.view.panels.alarms.DisplayPanel;
import org.dcache.webadmin.view.panels.alarms.ErrorPanel;
import org.dcache.webadmin.view.panels.alarms.QueryPanel;

/**
 * Auto-updating form which also allows filtering via queries to the store (on
 * date, type and severity) and in-memory expression matching.
 *
 * @author arossi
 */
@RequireHttps
public class AlarmsPage extends BasePage implements AuthenticatedWebPage {
    private static final String CHECK_ALL =
                    "function checkAll(bx, clzz) {\n" +
                    "   var cbs = document.getElementsByClassName(clzz);\n" +
                    "   for(var i=0; i < cbs.length; i++) {\n" +
                    "     if(cbs[i].type == 'checkbox') {\n" +
                    "       cbs[i].checked = bx.checked;\n" +
                    "     }\n" +
                    "   }\n" +
                    "}";

    private static final long serialVersionUID = 993708875580341999L;
    private Button refreshButton;
    private DisplayPanel displayPanel;

    public AlarmsPage() {
        refreshButton = new Button("refresh") {
            private static final long serialVersionUID = -7985680254514578732L;

            @Override
            public void onSubmit() {
                refresh();
                if (displayPanel != null) {
                    displayPanel.clearHeaders();
                }
            }
        };

        Form<?> form = getAutoRefreshingForm("alarmsPageForm", false);
        form.add(new QueryPanel("filterPanel", this));
        if(getWebadminApplication().getAlarmDisplayService().isConnected()) {
            displayPanel = new DisplayPanel("displayPanel", this);
            form.add(displayPanel);
        } else {
            form.add(new ErrorPanel("displayPanel"));
        }

        add(form);
    }

    public Button getRefreshButton() {
        return refreshButton;
    }

    @Override
    protected void refresh() {
        getWebadminApplication().getAlarmDisplayService().refresh();
    }

    @Override
    protected void renderHeadInternal(IHeaderResponse response) {
        super.renderHeadInternal(response);
        response.render(JavaScriptHeaderItem.forScript(CHECK_ALL, "checkall"));
    }
}
