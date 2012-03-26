package org.dcache.webadmin.view.panels.layout;

import java.text.NumberFormat;
import java.util.Locale;
import org.apache.wicket.AttributeModifier;
import org.apache.wicket.markup.html.WebMarkupContainer;
import org.apache.wicket.model.Model;
import org.dcache.webadmin.view.panels.basepanel.BasePanel;

/**
 * Reusable layout item nicely css-driven
 * @author jans
 */
public class LayoutItemPanel extends BasePanel {

    public LayoutItemPanel(String id, float percentagePrecious,
            float percentagePinned, float percentageRemovable,
            float percentageFree) {
        super(id);
        setPercentages(percentagePrecious, percentagePinned,
                percentageRemovable, percentageFree);
    }

    private void setPercentages(float percentagePrecious,
            float percentagePinned, float percentageRemovable,
            float percentageFree) {
        String preciousStyle = buildStyleAttribute(percentagePrecious);
        setStyleAttribute("percentagePrecious", preciousStyle);
        String pinnedStyle = buildStyleAttribute(percentagePinned);
        setStyleAttribute("percentagePinned", pinnedStyle);
        String removableStyle = buildStyleAttribute(percentageRemovable);
        setStyleAttribute("percentageRemovable", removableStyle);
        String freeStyle = buildStyleAttribute(percentageFree);
        setStyleAttribute("percentageFree", freeStyle);
    }

    private String buildStyleAttribute(float width) {
        NumberFormat numberFormat = NumberFormat.getInstance(Locale.US);
        String style = "width: " + numberFormat.format(width) + "%";
        return style;
    }

    private void setStyleAttribute(String markupContainerName, String styleAttribute) {
        WebMarkupContainer layoutLabel = new WebMarkupContainer(markupContainerName);
        layoutLabel.add(new AttributeModifier("style",
                new Model(styleAttribute)));
        add(layoutLabel);
    }
}
