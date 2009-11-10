package org.dcache.webdav;

import com.bradmcevoy.http.ServletRequest;
import com.bradmcevoy.http.ServletResponse;
import com.bradmcevoy.http.HttpManager;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.ServletException;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import java.io.IOException;

/**
 * A Jetty handler that wraps a Milton HttpManager. Makes it possible
 * to embed Milton in Jetty without using the Milton servlet.
 */
public class MiltonHandler extends AbstractHandler
{
    private HttpManager _httpManager;

    public void setHttpManager(HttpManager httpManager)
    {
        _httpManager = httpManager;
    }

    public void handle(String target, Request baseRequest,
                       HttpServletRequest request,HttpServletResponse response)
        throws IOException, ServletException
    {
        try {
            ServletRequest req = new ServletRequest(request);
            ServletResponse resp = new ServletResponse(response);
            baseRequest.setHandled(true);
            _httpManager.process(req, resp);
        } finally {
            response.getOutputStream().flush();
            response.flushBuffer();
        }
    }
}