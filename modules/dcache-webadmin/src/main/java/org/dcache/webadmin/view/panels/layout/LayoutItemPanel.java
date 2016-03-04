package org.dcache.webadmin.view.panels.layout;

import org.apache.wicket.AttributeModifier;
import org.apache.wicket.markup.html.WebMarkupContainer;
import org.apache.wicket.model.Model;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Locale;

import org.dcache.webadmin.view.panels.basepanel.BasePanel;

/**
 * Reusable layout item nicely css-driven
 * @author jans
 */
public class LayoutItemPanel extends BasePanel {

    private static final long serialVersionUID = -2930084452989756434L;
    private String _infoBox = "";
    private String _percentage;
    private String _quotes = "'";
    private String _spanLayoutClass = "<span class=\\'layout_";
    private String _spanLayoutCloseTag = "\\'>";
    private String _spanCloseTag = "</span>";
    private String stingBegin = "showBox("+_quotes;
    private String stingEnd = _quotes+")";
    private String newLine = "<br>";

    public LayoutItemPanel(String id, float percentagePrecious,
            float percentagePinned, float percentageRemovable,
            float percentageFree) {
        super(id);

        buildSpaceUsageLayout(percentagePrecious, percentagePinned,
                percentageRemovable, percentageFree);

    }

    private void buildSpaceUsageLayout(float percentagePrecious,
            float percentagePinned, float percentageRemovable,
            float percentageFree) {

        String layoutID = "percentage";
        String precious = "precious";
        String pinned = "pinned";
        String removable = "removable";
        String free = "free";

        ArrayList<InfoBoxAttributes> attrList = new ArrayList<>();

        String preciousStyle = buildStyleAttribute(percentagePrecious);
        attrList.add(new InfoBoxAttributes(layoutID, precious, preciousStyle));
        _infoBox = stingBegin + buildInfoBox(_percentage, precious);

        String pinnedStyle = buildStyleAttribute(percentagePinned);
        attrList.add(new InfoBoxAttributes(layoutID, pinned, pinnedStyle));
        _infoBox = _infoBox + buildInfoBox(_percentage, pinned);

        String removableStyle = buildStyleAttribute(percentageRemovable);
        attrList.add(new InfoBoxAttributes(layoutID, removable, removableStyle));
        _infoBox = _infoBox + buildInfoBox(_percentage, removable);

        String freeStyle = buildStyleAttribute(percentageFree);
        attrList.add(new InfoBoxAttributes(layoutID, free, freeStyle));
        _infoBox = _infoBox + buildInfoBox(_percentage, free) + stingEnd;

        setAttributes(attrList);
    }

    private String buildStyleAttribute(float width) {
        NumberFormat numberFormat = NumberFormat.getInstance(Locale.US);
        _percentage = numberFormat.format(width);
        return "width: " + _percentage + "%";
    }

    private String buildInfoBox(String percentage, String layout) {
        return _spanLayoutClass + layout + _spanLayoutCloseTag + percentage + "% " + layout + newLine + _spanCloseTag;

    }

    private void setAttributes(ArrayList<InfoBoxAttributes> attrList) {
        WebMarkupContainer layoutInfoBox = new WebMarkupContainer("createInfoBox");
        layoutInfoBox.setOutputMarkupId(true);
        layoutInfoBox.add(new StyleAttributes("layouts", attrList));
        layoutInfoBox.add(new AttributeModifier("onmouseover", new Model(_infoBox))).setEscapeModelStrings(true);
        add(layoutInfoBox);
    }
}
