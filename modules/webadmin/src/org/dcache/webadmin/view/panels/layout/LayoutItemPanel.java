package org.dcache.webadmin.view.panels.layout;

import org.apache.wicket.AttributeModifier;
import org.apache.wicket.markup.html.WebMarkupContainer;
import org.apache.wicket.markup.html.panel.Panel;
import org.apache.wicket.model.Model;

/**
 * Reusable layout item nicely css-driven
 * @author jans
 */
public class LayoutItemPanel extends Panel {

    public LayoutItemPanel(String id, float percentagePrecious,
            float percentageUsed, float percentageFree) {
        super(id);
        setPercentages(percentagePrecious, percentageUsed, percentageFree);
    }

    private void setPercentages(float percentagePrecious,
            float percentageUsed, float percentageFree) {
        String preciousStyle = buildStyleAttribute(percentagePrecious);
        setStyleAttribute("percentagePrecious", preciousStyle);
        String usedStyle = buildStyleAttribute(percentageUsed);
        setStyleAttribute("percentageUsed", usedStyle);
        String freeStyle = buildStyleAttribute(percentageFree);
        setStyleAttribute("percentageFree", freeStyle);
    }

    private String buildStyleAttribute(float width) {
        String style = "width: " + Float.toString(width) + "%";
        return style;
    }

    private void setStyleAttribute(String markupContainerName, String styleAttribute) {
        WebMarkupContainer layoutLabel = new WebMarkupContainer(markupContainerName);
        layoutLabel.add(new AttributeModifier("style",
                new Model(styleAttribute)));
        add(layoutLabel);
    }
}
