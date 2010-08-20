package org.dcache.services.plots;

import dmg.util.HttpException;
import org.w3c.dom.Element;

/**
 *
 * @author timur and tao
 */
public class PageNotFound extends PlotPage {

    @Override
    protected void buildBody() throws HttpException {
        super.buildBody();
        Element message = document.createElement("h2");
        body.appendChild(message);

        message.setTextContent("Page not found");
        Element list = document.createElement("ul");
        body.appendChild(list);
        String[] tokens = httpRequest.getRequestTokens();

        for (int i = 0; i < tokens.length; i++) {
            Element li = document.createElement("li");
            list.appendChild(li);
            li.setTextContent(tokens[i].split("\\?")[0]);
        }
    }
}
