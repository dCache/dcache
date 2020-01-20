package org.dcache.webdav;

import com.google.common.collect.ImmutableList;
import org.eclipse.jetty.server.HttpConnection;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;

public class CrossOriginResourceSharingHandler extends AbstractHandler
{
    private static final ImmutableList<String> ALLOWED_ORIGIN_PROTOCOL = ImmutableList.of("http", "https");
    private List<String> _allowedClientOrigins = Collections.emptyList();

    public void setAllowedClientOrigins(String origins)
    {
        if (origins.isEmpty()) {
            _allowedClientOrigins = Collections.emptyList();
        } else {
            List<String> originList = Arrays.stream(origins.split(","))
                    .map(String::trim)
                    .collect(Collectors.toList());
            originList.forEach(CrossOriginResourceSharingHandler::checkOrigin);
            _allowedClientOrigins = originList;
        }
    }

    private static void checkOrigin(String s)
    {
        try {
            URI uri = new URI(s);

            checkArgument(ALLOWED_ORIGIN_PROTOCOL.contains(uri.toURL().getProtocol()),
                    "Invalid URL: The URL: %s contain unsupported protocol. Use either http or https.", s);
            checkArgument(!uri.getHost().isEmpty(), "Invalid URL: the host name is not provided in %s:", s);
            checkArgument(uri.getUserInfo() == null,
                    "The URL: %s is invalid. User information is not allowed to be part of the URL.", s);
            checkArgument(uri.toURL().getPath().isEmpty(),
                    "The URL: %s is invalid. Remove the \"path\" part of the URL.", s);
            checkArgument(uri.toURL().getQuery() == null,
                    "The URL: %s is invalid. Remove the query-path part of the URL.", s);
            checkArgument(uri.toURL().getRef() == null,
                    "URL: %s is invalid. Reason: no reference or fragment allowed in the URL.", s);
            checkArgument(!uri.isOpaque(), "URL: %s is invalid. Check the scheme part of the URL", s);
        } catch (MalformedURLException | URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public void handle(String target, Request baseRequest,
                       HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException
    {
        String clientOrigin = request.getHeader("origin");
        response.setHeader("Access-Control-Allow-Credentials", "true");

        if (_allowedClientOrigins.isEmpty()) {
            response.setHeader("Access-Control-Allow-Origin", "*");
        } else if (_allowedClientOrigins.contains(clientOrigin)) {
            response.setHeader("Access-Control-Allow-Origin", clientOrigin);

            if (_allowedClientOrigins.size() > 1) {
                response.setHeader("Vary", "Origin");
            }
        }

        if ("OPTIONS".equals(request.getMethod())) {
            response.setHeader("Allow", "GET, PUT, POST, DELETE");
            response.setHeader("Access-Control-Allow-Methods", "GET, PUT, POST, DELETE");
            response.setHeader("Access-Control-Allow-Headers",
                    "Authorization, Content-Type, Suppress-WWW-Authenticate");

            /* Note: we do not mark the request as handled.  This is to allow
             * other handlers to add response headers.
             */
        }
    }
}
