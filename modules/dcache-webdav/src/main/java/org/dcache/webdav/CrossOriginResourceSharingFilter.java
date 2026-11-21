package org.dcache.webdav;

import static com.google.common.base.Preconditions.checkArgument;

import static org.eclipse.jetty.ee8.servlets.CrossOriginFilter.ALLOWED_HEADERS_PARAM;
import static org.eclipse.jetty.ee8.servlets.CrossOriginFilter.ALLOWED_METHODS_PARAM;
import static org.eclipse.jetty.ee8.servlets.CrossOriginFilter.ALLOWED_ORIGINS_PARAM;
import static org.eclipse.jetty.ee8.servlets.CrossOriginFilter.ALLOW_CREDENTIALS_PARAM;
import static org.eclipse.jetty.ee8.servlets.CrossOriginFilter.CHAIN_PREFLIGHT_PARAM;
import static org.eclipse.jetty.ee8.servlets.CrossOriginFilter.PREFLIGHT_MAX_AGE_PARAM;

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
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import org.eclipse.jetty.ee8.servlets.CrossOriginFilter;

/**
 * A filter that uses Jetty's CrossOriginFilter to implement support for CORS. It answers all
 * OPTIONS requests.
 */
public class CrossOriginResourceSharingFilter implements Filter, FilterConfig {

    private static final Map<String, String> DEFAULT_CONFIG = Map.of(
          ALLOWED_METHODS_PARAM, "GET,PUT,POST,DELETE",
          ALLOWED_HEADERS_PARAM, "Content-Type,Authorization,Suppress-WWW-Authenticate",
          PREFLIGHT_MAX_AGE_PARAM, "0", // Disable 'Access-Control-Max-Age' response.
          CHAIN_PREFLIGHT_PARAM, "false",
          ALLOW_CREDENTIALS_PARAM, "true" //TODO: does not work without this for session to be checked
          );
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
                  .forEach(CrossOriginResourceSharingFilter::checkOrigin);
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
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
          throws IOException, ServletException {
        if ("OPTIONS".equalsIgnoreCase(((HttpServletRequest) request).getMethod())) {
            filter.doFilter(request, response, (req, res) -> {});  // ← empty chain, don't pass through
        } else {
            filter.doFilter(request, response, chain);
        }
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
