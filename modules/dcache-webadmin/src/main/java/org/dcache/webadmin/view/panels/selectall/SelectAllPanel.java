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

import org.apache.wicket.Component;
import org.apache.wicket.ajax.AbstractDefaultAjaxBehavior;
import org.apache.wicket.ajax.AjaxRequestTarget;
import org.apache.wicket.ajax.attributes.AjaxRequestAttributes;
import org.apache.wicket.ajax.attributes.AjaxRequestAttributes.Method;
import org.apache.wicket.ajax.attributes.CallbackParameter;
import org.apache.wicket.authroles.authorization.strategies.role.metadata.MetaDataRoleAuthorizationStrategy;
import org.apache.wicket.markup.head.IHeaderResponse;
import org.apache.wicket.markup.head.OnEventHeaderItem;
import org.apache.wicket.markup.html.form.Button;
import org.apache.wicket.request.cycle.RequestCycle;
import org.apache.wicket.request.http.WebRequest;
import org.apache.wicket.util.string.StringValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

import org.dcache.webadmin.view.beans.PoolCommandBean;
import org.dcache.webadmin.view.beans.PoolSpaceBean;
import org.dcache.webadmin.view.panels.basepanel.BasePanel;
import org.dcache.webadmin.view.util.Role;

import static org.dcache.webadmin.view.pages.basepage.SortableBasePage.FILTER_EVENT;

/**
 * <p>Encapsulates the triad of select all, deselect all and submit buttons.
 * The submit and selection functionality depends on the concrete class.</p>
 *
 * <p>Also provides AJAX callback for synchronizing the filtered (hidden)
 * rows with the select/deselect all button functions.</p>
 */
public abstract class SelectAllPanel extends BasePanel {

    private static final long serialVersionUID = 8704591801936459097L;

    private static final Logger LOGGER = LoggerFactory.getLogger(SelectAllPanel.class);

    /**
     * <p>Behavior which reacts to a special event triggered by
     *    the picnet filter.  It determines which rows have been
     *    hidden by the filter and sends the name identifiers to the
     *    server via Http POST.</p>
     */
    class PicnetPostHiddenRowsBehavior extends AbstractDefaultAjaxBehavior {
        /**
         * <p>Builds the callback script to send to the client.</p>
         */
        @Override
        public void renderHead(Component component, IHeaderResponse response) {
            super.renderHead(component, response);
            StringBuilder script = new StringBuilder();
            appendHiddenRows(script);
            response.render(OnEventHeaderItem.forScript("document",
                                                        FILTER_EVENT,
                                                        script.toString()));
        }

        /**
         * <p>Processes the POST by updating the set of hidden rows.</p>
         */
        @Override
        protected void respond(AjaxRequestTarget target) {
            RequestCycle cycle = RequestCycle.get();
            WebRequest webRequest = (WebRequest) cycle.getRequest();
            StringValue value = webRequest.getPostParameters()
                                          .getParameterValue("document.hiddenRows");
            setHiddenRows(value.toString(""));
        }

        @Override
        protected void updateAjaxAttributes(AjaxRequestAttributes attributes) {
            super.updateAjaxAttributes(attributes);
            attributes.setMethod(Method.POST);
        }

        String getCallback() {
            return behavior.getCallbackFunctionBody
                            (CallbackParameter.explicit("document.hiddenRows"))
                           .toString();
        }

        /**
         * <p>Searches for rows in the table marked by picnet as 'filtermatch = false'
         * and returns the text of the columns with the key classes.  This is
         * assigned to a document-level variable "hiddenRows".</p>
         *
         * @param script builder representing full script
         */
        private void appendHiddenRows(StringBuilder script) {
            script.append("document.hiddenRows = $('.sortable-").append(tableId)
                  .append("').find(\"tr\").filter(function() {\n")
                  .append("return $(this).attr( \"filtermatch\" ) === \"false\";\n")
                  .append("}).map(function() {\n")
                  .append("return $(this).find('.key1').text() + $(this).find('.key2').text();")
                  .append("}).get();\n");
        }

        private void setHiddenRows(String commaDelimitedList) {
            String[] list = commaDelimitedList.split("[,]");
            synchronized (hidden) {
                hidden.clear();
                for (String key : list) {
                    hidden.add(key);
                }
            }
            LOGGER.debug("setHiddenRows, now {}.", hidden);
        }
    }

    private class SelectAllButton extends Button {
        private static final long serialVersionUID = -8490536062145002381L;

        public SelectAllButton(String id) {
            super(id);
            this.setDefaultFormProcessing(false);

        }

        @Override
        public void renderHead(IHeaderResponse response) {
            super.renderHead(response);
            response.render(OnEventHeaderItem.forScript("selectAll",
                                                        "click",
                                                        behavior.getCallback()));
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
        public void renderHead(IHeaderResponse response) {
            super.renderHead(response);
            response.render(OnEventHeaderItem.forScript("deselectAll",
                                                        "click",
                                                        behavior.getCallback()));
        }

        @Override
        public void onSubmit() {
            setSubmitCalled();
            setSelectionForAll(Boolean.FALSE);
        }
    }

    final PicnetPostHiddenRowsBehavior behavior = new PicnetPostHiddenRowsBehavior();
    final private Set<String> hidden = new HashSet<>();
    final private String tableId;

    public SelectAllPanel(String id, String tableId, Button submit) {
        super(id);
        this.tableId = tableId;
        Button selectAll = new SelectAllButton("selectAllButton");
        Button deselectAll = new DeselectAllButton("deselectAllButton");
        MetaDataRoleAuthorizationStrategy.authorize(submit, RENDER, Role.ADMIN);
        MetaDataRoleAuthorizationStrategy.authorize(selectAll, RENDER, Role.ADMIN);
        MetaDataRoleAuthorizationStrategy.authorize(deselectAll, RENDER, Role.ADMIN);
        add(submit);
        add(selectAll);
        add(deselectAll);
        add(behavior);
    }

    protected boolean isHidden(PoolSpaceBean bean) {
        synchronized (hidden) {
            String key = bean.getName() + bean.getDomainName();
            boolean isHidden = hidden.contains(key);
            LOGGER.debug("{}, hidden = {}.", key, isHidden);
            return isHidden;
        }
    }

    protected boolean isHidden(PoolCommandBean bean) {
        synchronized (hidden) {
            String key = bean.getName() + bean.getDomain();
            boolean isHidden = hidden.contains(key);
            LOGGER.debug("{}, hidden = {}.", key, isHidden);
            return isHidden;
        }
    }

    protected abstract void setSubmitCalled();

    protected abstract void setSelectionForAll(Boolean selected);
}
