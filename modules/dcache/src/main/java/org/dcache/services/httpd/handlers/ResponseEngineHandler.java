package org.dcache.services.httpd.handlers;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.net.URISyntaxException;

import dmg.util.HttpException;
import dmg.util.HttpRequest;
import dmg.util.HttpResponseEngine;

import org.dcache.services.httpd.util.StandardHttpRequest;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Wraps calls to {@link HttpResponseEngine} aliases with the Jetty handler API.
 *
 * @author arossi
 */
public class ResponseEngineHandler extends AbstractHandler
{
    private final HttpResponseEngine engine;

    public ResponseEngineHandler(HttpResponseEngine engine) {
        this.engine = engine;
    }

    @Override
    public void handle(String target, Request baseRequest,
                    HttpServletRequest request, HttpServletResponse response)
                    throws IOException, ServletException {
        checkNotNull(engine);
        try {
            HttpRequest proxy = new StandardHttpRequest(request, response);
            engine.queryUrl(proxy);
            proxy.getPrintWriter().flush();
        } catch (HttpException e) {
            response.sendError(e.getErrorCode(), e.getMessage());
        } catch (URISyntaxException e) {
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                    e.getMessage());
        }
    }

    @Override
    protected void doStart() throws Exception
    {
        engine.startup();
        super.doStart();
    }

    @Override
    protected void doStop() throws Exception
    {
        super.doStop();
        engine.shutdown();
    }
}
