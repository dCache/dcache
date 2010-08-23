package org.dcache.services.plots;

import org.w3c.dom.Element;

/**
 *
 * @author timur and tao
 */
public class NavigationPage extends PlotPage{
    @Override
    protected void buildBody(){
        Element h2 = document.createElement("h2");
        h2.setTextContent("Billing Plots");
        body.appendChild(h2);
        Element list = document.createElement("ul");
        body.appendChild(list);
        list.appendChild(listItem(PlotPage.TRANSFERRATE_PAGE, "Transfer Rate"));
        list.appendChild(listItem(PlotPage.CONNECTIONTIME_PAGE, "Connection Time"));
        list.appendChild(listItem(PlotPage.CACHEHITS_PAGE, "Cache Hits"));
        list.appendChild(listItem(PlotPage.CUSTOMIZED_PAGE, "Customized Plots"));
    }

    private Element listItem(String link, String text) {
        Element li = document.createElement("li");
        Element a = document.createElement("a");
        li.appendChild(a);
        a.setAttribute("href", link);
        a.setAttribute("style", "color:black");
        a.setAttribute("target", PlotPage.SHOW_FRAME);
        a.setTextContent(text);
        return li;
    }
}
