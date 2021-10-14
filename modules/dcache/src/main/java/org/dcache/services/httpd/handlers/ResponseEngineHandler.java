package org.dcache.services.httpd.handlers;

import static java.util.Objects.requireNonNull;

import dmg.util.HttpException;
import dmg.util.HttpRequest;
import dmg.util.HttpResponseEngine;
import java.io.IOException;
import java.net.URISyntaxException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.dcache.services.httpd.util.StandardHttpRequest;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

/**
 * Wraps calls to {@link HttpResponseEngine} aliases with the Jetty handler API.
 *
 * @author arossi
 */
public class ResponseEngineHandler extends AbstractHandler {

    private final HttpResponseEngine engine;

    public ResponseEngineHandler(HttpResponseEngine engine) {
        this.engine = engine;
    }

    @Override
    public void handle(String target, Request baseRequest,
          HttpServletRequest request, HttpServletResponse response)
          throws IOException, ServletException {
        requireNonNull(engine);
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
}
