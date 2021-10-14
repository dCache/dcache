package org.dcache.webdav;

import static com.google.common.base.Preconditions.checkArgument;
import static org.eclipse.jetty.servlets.CrossOriginFilter.ALLOWED_HEADERS_PARAM;
import static org.eclipse.jetty.servlets.CrossOriginFilter.ALLOWED_METHODS_PARAM;
import static org.eclipse.jetty.servlets.CrossOriginFilter.ALLOWED_ORIGINS_PARAM;
import static org.eclipse.jetty.servlets.CrossOriginFilter.CHAIN_PREFLIGHT_PARAM;
import static org.eclipse.jetty.servlets.CrossOriginFilter.PREFLIGHT_MAX_AGE_PARAM;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.PostConstruct;
import javax.servlet.Filter;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.servlets.CrossOriginFilter;

/**
 * A Jetty Handler that uses Jetty's CrossOriginFilter to implement support for CORS.
 */
public class CrossOriginResourceSharingHandler extends AbstractHandler implements FilterConfig {

    private static final Map<String, String> DEFAULT_CONFIG = Map.of(
          ALLOWED_METHODS_PARAM, "GET,PUT,POST,DELETE",
          ALLOWED_HEADERS_PARAM, "Content-Type,Authorization,Suppress-WWW-Authenticate",
          PREFLIGHT_MAX_AGE_PARAM, "0", // Disable 'Access-Control-Max-Age' response.
          CHAIN_PREFLIGHT_PARAM, "false");
    private static final ImmutableList<String> ALLOWED_ORIGIN_PROTOCOL = ImmutableList.of("http",
          "https");
    private Filter filter;
    private final Map<String, String> filterConfig = new HashMap<>(DEFAULT_CONFIG);

    public void setAllowedClientOrigins(String origins) {
        String configValue;

        if (origins.isEmpty()) {
            configValue = "*";
        } else {
            // Fail fast
            Splitter.on(',').trimResults().splitToList(origins)
                  .forEach(CrossOriginResourceSharingHandler::checkOrigin);
            configValue = origins;
        }

        filterConfig.put(ALLOWED_ORIGINS_PARAM, configValue);
    }

    private static void checkOrigin(String s) {
        try {
            URI uri = new URI(s);

            checkArgument(ALLOWED_ORIGIN_PROTOCOL.contains(uri.toURL().getProtocol()),
                  "Invalid URL: The URL: %s contain unsupported protocol. Use either http or https.",
                  s);
            checkArgument(!uri.getHost().isEmpty(),
                  "Invalid URL: the host name is not provided in %s:", s);
            checkArgument(uri.getUserInfo() == null,
                  "The URL: %s is invalid. User information is not allowed to be part of the URL.",
                  s);
            checkArgument(uri.toURL().getPath().isEmpty(),
                  "The URL: %s is invalid. Remove the \"path\" part of the URL.", s);
            checkArgument(uri.toURL().getQuery() == null,
                  "The URL: %s is invalid. Remove the query-path part of the URL.", s);
            checkArgument(uri.toURL().getRef() == null,
                  "URL: %s is invalid. Reason: no reference or fragment allowed in the URL.", s);
            checkArgument(!uri.isOpaque(), "URL: %s is invalid. Check the scheme part of the URL",
                  s);
        } catch (MalformedURLException | URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @PostConstruct
    public void setup() {
        filter = new CrossOriginFilter();

        try {
            filter.init(this);
        } catch (ServletException e) {
            throw new RuntimeException("Bad CrossOriginFilter config: "
                  + e.toString(), e);
        }
    }

    @Override
    public void handle(String target, Request baseRequest,
          HttpServletRequest request, HttpServletResponse response)
          throws IOException, ServletException {
        baseRequest.setHandled(true);
        filter.doFilter(request, response, (req, res) -> baseRequest.setHandled(false));
    }

    @Override
    public String getFilterName() {
        return "cors-filter";
    }

    @Override
    public ServletContext getServletContext() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getInitParameter(String name) {
        return filterConfig.get(name);
    }

    @Override
    public Enumeration<String> getInitParameterNames() {
        return Collections.enumeration(filterConfig.keySet());
    }
}
