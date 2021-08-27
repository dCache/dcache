package org.dcache.services.httpd.handlers;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;

/**
 * Processes redirection from one URL to another.
 *
 * @author arossi
 */
public class RedirectHandler extends AbstractHandler {

    private static final Logger LOGGER
        = LoggerFactory.getLogger(RedirectHandler.class);

    private final String fromContext;
    private final String toContext;

    public RedirectHandler(String fromContext, String toContext ) {
        if ("<home>".equals(fromContext)) {
            fromContext = "/";
        }
        this.fromContext = fromContext;
        this.toContext = toContext;
    }

    @Override
    public void handle(String target, Request baseRequest,
            HttpServletRequest request, HttpServletResponse response) throws
            IOException, ServletException {
        LOGGER.debug("target: {}", target);

        if (target.contains(fromContext)) {
            StringBuilder targetUrl = new StringBuilder(target);
            int i = targetUrl.indexOf(fromContext);
            String newUrl
            = targetUrl.replace(i, i + fromContext.length(),
                                       toContext)
                       .toString();
            LOGGER.debug("redirected to: {}", newUrl);
            response.sendRedirect(newUrl);
        }
    }
}
