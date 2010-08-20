package org.dcache.services.plots;

import org.w3c.dom.Element;

/**
 *
 * @author timur and tao
 */
public class NavigationPage extends PlotPage{
    @Override
    protected void buildBody(){
        Element list = document.createElement("ul");
        body.appendChild(list);
        list.appendChild(listItem(PlotPage.TRANSFERRATE_PAGE, "Transfer Rate"));
        list.appendChild(listItem(PlotPage.CONNECTIONTIME_PAGE, "Connection Time"));
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
