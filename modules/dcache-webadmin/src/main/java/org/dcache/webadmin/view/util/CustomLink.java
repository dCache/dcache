package org.dcache.webadmin.view.util;

import org.apache.wicket.markup.html.link.Link;

import org.dcache.webadmin.view.pages.login.LogIn;

/**
 * Conveniance Class for a simple Link to another page
 * @author jans
 */
public class CustomLink extends Link {

    private static final long serialVersionUID = -7984913509576011949L;
    private final Class<LogIn> _page;

    public CustomLink(String id, Class<LogIn> page) {
        super(id);
        _page = page;
    }

    @Override
    public void onClick() {
        setResponsePage(_page);
    }
}
