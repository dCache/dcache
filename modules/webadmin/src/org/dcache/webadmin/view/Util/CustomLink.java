package org.dcache.webadmin.view.Util;

import org.apache.wicket.markup.html.link.Link;

/**
 * Conveniance Class for a simple Link to another page
 * @author jans
 */
public class CustomLink extends Link {

    private final Class _page;

    public CustomLink(String id, Class page) {
        super(id);
        _page = page;
    }

    @Override
    public void onClick() {
        setResponsePage(_page);
    }
}
