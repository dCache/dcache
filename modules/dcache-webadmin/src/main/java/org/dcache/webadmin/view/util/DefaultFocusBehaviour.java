package org.dcache.webadmin.view.util;

import org.apache.wicket.markup.head.JavaScriptHeaderItem;

import org.apache.wicket.Component;
import org.apache.wicket.behavior.Behavior;
import org.apache.wicket.markup.head.IHeaderResponse;
import org.apache.wicket.markup.html.form.FormComponent;

/**
 * This Behaviour may be added to a component that is a child of a Form. It
 * makes the component the one which has the focus when the page is displayed by
 * adding the corresponding javascript to it. should be given only to one of the
 * children(because only one can have the focus naturally).
 *
 * @author jans
 */
public class DefaultFocusBehaviour extends Behavior {

    private static final long serialVersionUID = -4891399118136854774L;

    @Override
    public void bind(Component component) {
        if (!(component instanceof FormComponent)) {
            throw new IllegalArgumentException(
                            "DefaultFocusBehavior: component must be instanceof FormComponent");
        }
        component.setOutputMarkupId(true);
    }

    @Override
    public void renderHead(Component component, IHeaderResponse iHeaderResponse) {
        super.renderHead(component, iHeaderResponse);
        iHeaderResponse.render(JavaScriptHeaderItem.forScript(
                        "document.getElementById('" + component.getMarkupId()
                                        + "').focus();", null));
    }
}
