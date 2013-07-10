package org.dcache.webadmin.view.panels.basepanel;

import org.apache.wicket.markup.html.panel.Panel;
import org.apache.wicket.model.IModel;
import org.apache.wicket.model.StringResourceModel;

/**
 * Basic Panel with common methods
 * @author jans
 */
public class BasePanel extends Panel {

    private static final long serialVersionUID = -572941307837646077L;

    public BasePanel(String id) {
        super(id);
    }

    protected IModel<String> getStringResource(String resourceKey) {
        return new StringResourceModel(
                resourceKey, this, null);
    }
}
