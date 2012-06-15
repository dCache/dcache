package org.dcache.webdav;

import com.bradmcevoy.http.Resource;

/**
 * Indicates that a resource is to be accessed at an alternate
 * location. The server is to generate a redirect to that location.
 */
public class RedirectException extends WebDavException
{
    private final String _url;

    public RedirectException(Resource resource, String url)
    {
        super(resource);
        _url = url;
    }

    public String getUrl()
    {
        return _url;
    }
}
