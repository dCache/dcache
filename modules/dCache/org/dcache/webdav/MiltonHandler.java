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

import org.dcache.cells.CellMessageSender;

import dmg.cells.nucleus.CDC;
import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellEndpoint;
import org.apache.log4j.MDC;
import org.apache.log4j.NDC;

/**
 * A Jetty handler that wraps a Milton HttpManager. Makes it possible
 * to embed Milton in Jetty without using the Milton servlet.
 */
public class MiltonHandler
    extends AbstractHandler
    implements CellMessageSender
{
    private HttpManager _httpManager;
    private String _cellName;
    private String _domainName;

    public void setHttpManager(HttpManager httpManager)
    {
        _httpManager = httpManager;
    }

    public void setCellEndpoint(CellEndpoint endpoint)
    {
        CellInfo info = endpoint.getCellInfo();
        _cellName = info.getCellName();
        _domainName = info.getDomainName();
    }

    public void handle(String target, Request baseRequest,
                       HttpServletRequest request,HttpServletResponse response)
        throws IOException, ServletException
    {
        MDC.put(CDC.MDC_CELL, _cellName);
        MDC.put(CDC.MDC_DOMAIN, _domainName);
        CDC.createSession();
        NDC.push(CDC.getSession().toString());
        try {
            ServletRequest req = new ServletRequest(request) {
                    @Override
                    public String getExpectHeader() {
                        /* Jetty deals with expect headers, so no
                         * reason for Milton to worry about them.
                         */
                        return "";
                    }
                };
            ServletResponse resp = new ServletResponse(response);
            baseRequest.setHandled(true);
            _httpManager.process(req, resp);
            response.getOutputStream().flush();
            response.flushBuffer();
        } finally {
            MDC.remove(CDC.MDC_DOMAIN);
            MDC.remove(CDC.MDC_CELL);
            MDC.remove(CDC.MDC_SESSION);
            NDC.remove();
        }
    }
}