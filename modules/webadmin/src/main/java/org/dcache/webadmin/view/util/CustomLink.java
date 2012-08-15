package org.dcache.webadmin.view.util;

import org.apache.wicket.markup.html.link.Link;

/**
 * Conveniance Class for a simple Link to another page
 * @author jans
 */
public class CustomLink extends Link {

    private static final long serialVersionUID = -7984913509576011949L;
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
