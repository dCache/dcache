package org.dcache.services.plots;

import org.apache.xerces.dom.DocumentImpl;
import org.w3c.dom.Element;

/**
 *
 * @author timur and tao
 */
public class HomePage extends PlotPage {

    @Override
    public void buildHtml() {
        document = new DocumentImpl();
        html = document.createElement("html");
        document.appendChild(html);

        Element frameSet = document.createElement("frameset");
        html.appendChild(frameSet);
        frameSet.setAttribute("cols", "200,*");

        Element frame = null;
        frame = document.createElement("frame");
        frame.setAttribute("src", PLOTDIR +"/" + PlotPage.NAV_PAGE);
        frameSet.appendChild(frame);

        frame = document.createElement("frame");
        frame.setAttribute("src", PLOTDIR +"/" + PlotPage.TRANSFERRATE_PAGE);
        frame.setAttribute("name", SHOW_FRAME);
        frameSet.appendChild(frame);
    }
}
