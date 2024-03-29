package org.dcache.services.httpd.handlers;

import dmg.cells.nucleus.DomainContextAware;
import dmg.util.HttpRequest;
import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.dcache.services.httpd.exceptions.OnErrorException;
import org.dcache.services.httpd.util.StandardHttpRequest;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

/**
 * Provides lookup in the context map of preconfigured HTML pages.
 *
 * @author arossi
 */
public class ContextHandler extends AbstractHandler implements DomainContextAware {

    private final String specificName;
    private Map<String, Object> context;
    private final int status;

    public ContextHandler(String specificName) {
        this(specificName, HttpServletResponse.SC_OK);
    }

    public ContextHandler(String specificName, int status) {
        this.specificName = specificName;
        this.status = status;
    }

    @Override
    public void setDomainContext(Map<String, Object> context) {
        this.context = context;
    }

    @Override
    public void handle(String target, Request baseRequest,
          HttpServletRequest request, HttpServletResponse response)
          throws IOException, ServletException {

        try {
            final HttpRequest proxy = new StandardHttpRequest(request, response);

            Object value;
            if (specificName.equals("*")) {
                final String[] tokens = proxy.getRequestTokens();
                if (tokens.length < 2 || tokens[1].equals("index.html")) {
                    value = createContextDirectory();
                } else {
                    value = context.get(tokens[1]);
                }

            } else {
                value = context.get(specificName);
            }

            if (value == null) {
                throw new OnErrorException();
            }
            String html = String.valueOf(value);

            response.setStatus(status);
            proxy.getPrintWriter().println(html);
            proxy.getPrintWriter().flush();
            baseRequest.setHandled(true);
        } catch (final Exception t) {
            throw new ServletException("ContextHandler", t);
        }
    }

    private String createContextDirectory() {
        final StringBuilder sb = new StringBuilder();
        sb.append("<html><title>Context directory</title>\n");
        sb.append("<body bgcolor=\"#0088dd\">\n");
        sb.append("<h1>Context Directory</h1>\n");
        sb.append("<blockquote>\n");
        sb.append("<center>\n");
        sb.append("<table border=1 cellspacing=0 cellpadding=4 width=\"%90\">\n");
        sb.append("<tr><th>Context Name</th><th>Class</th><th>Content</th></tr>\n");
        final SortedMap<String, Object> map = new TreeMap<>();
        map.putAll(context);
        for (final Map.Entry<String, Object> e : map.entrySet()) {
            final String key = e.getKey();
            final Object o = e.getValue();
            String str = o.toString();
            str = str.substring(0, Math.min(str.length(), 60)).trim();
            str = toHtmlEscapedString(str);
            sb.append("<tr><td>")
                  .append(key)
                  .append("</td><td>")
                  .append(o.getClass().getName())
                  .append("</td><td>")
                  .append(str.isEmpty() ? "&nbsp;" : str)
                  .append("</td></tr>\n");
        }
        sb.append("</table></center>\n");
        sb.append("</blockquote>\n");
        sb.append("<hr>");
        sb.append("<address>Created : ")
              .append(new Date())
              .append("</address>\n");
        sb.append("</body></html>");
        return sb.toString();
    }

    private String toHtmlEscapedString(String str) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0, n = str.length(); i < n; i++) {
            final char c = str.charAt(i);
            switch (c) {
                case '<':
                    sb.append("&lt;");
                    break;
                case '>':
                    sb.append("&gt;");
                    break;
                case '\n':
                    sb.append("\\n");
                    break;
                default:
                    sb.append(c);
            }
        }
        return sb.toString();
    }
}
