/*
COPYRIGHT STATUS:
  Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
  software are sponsored by the U.S. Department of Energy under Contract No.
  DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
  non-exclusive, royalty-free license to publish or reproduce these documents
  and software for U.S. Government purposes.  All documents and software
  available from this server are protected under the U.S. and Foreign
  Copyright Laws, and FNAL reserves all rights.


 Distribution of the software available from this server is free of
 charge subject to the user following the terms of the Fermitools
 Software Legal Information.

 Redistribution and/or modification of the software shall be accompanied
 by the Fermitools Software Legal Information  (including the copyright
 notice).

 The user is asked to feed back problems, benefits, and/or suggestions
 about the software to the Fermilab Software Providers.


 Neither the name of Fermilab, the  URA, nor the names of the contributors
 may be used to endorse or promote products derived from this software
 without specific prior written permission.



  DISCLAIMER OF LIABILITY (BSD):

  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
  "AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
  LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
  FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
  OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
  FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
  CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
  OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
  BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
  LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
  NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
  SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.


  Liabilities of the Government:

  This software is provided by URA, independent from its Prime Contract
  with the U.S. Department of Energy. URA is acting independently from
  the Government and in its own private capacity and is not acting on
  behalf of the U.S. Government, nor as its contractor nor its agent.
  Correspondingly, it is understood and agreed that the U.S. Government
  has no connection to this software and in no manner whatsoever shall
  be liable for nor assume any responsibility or obligation for any claim,
  cost, or damages arising out of or resulting from the use of the software
  available from this server.


  Export Control:

  All documents and software available from this server are subject to U.S.
  export control laws.  Anyone downloading information from this server is
  obligated to secure any necessary Government licenses before exporting
  documents or software obtained from this server.
 */
package org.dcache.services.httpd.handlers;

import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.SortedMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.dcache.services.httpd.exceptions.OnErrorException;
import org.dcache.services.httpd.util.StandardHttpRequest;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import dmg.util.CollectionFactory;
import dmg.util.HttpException;
import dmg.util.HttpRequest;

public class ContextHandler extends AbstractHandler {

    private String specificName;
    private Map<String, Object> context;

    public ContextHandler(String specificName, Map<String, Object> context) {
        this.specificName = specificName;
        this.context = context;
    }

    @Override
    public void handle(String target, Request baseRequest,
                    HttpServletRequest request, HttpServletResponse response)
                    throws IOException, ServletException {

        try {
            HttpRequest proxy = new StandardHttpRequest(request, response);
            String html = null;
            String[] tokens = proxy.getRequestTokens();
            if ((tokens.length > 1) && (tokens[1].equals("index.html"))) {
                html = createContextDirectory();
            } else {
                if (tokens.length > 1) {
                    final String contextName = tokens[1];
                    if (!contextName.startsWith(specificName)) {
                        throw new HttpException(HttpServletResponse.SC_FORBIDDEN,
                                        "Forbidden");
                    }
                    specificName = contextName;
                }
                html = (String) context.get(specificName);
            }
            if (html == null) {
                throw new OnErrorException();
            } else {
                proxy.getPrintWriter().println(html);
                proxy.getPrintWriter().flush();
            }
        } catch (Exception t) {
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
        final SortedMap<String, Object> map = CollectionFactory.newTreeMap();
        map.putAll(context);
        for (final Map.Entry<String, Object> e : map.entrySet()) {
            final String key = e.getKey();
            final Object o = e.getValue();
            String str = o.toString();
            str = str.substring(0, Math.min(str.length(), 60)).trim();
            str = toHtmlEscapedString(str);
            sb  .append("<tr><td>")
                .append(key)
                .append("</td><td>")
                .append(o.getClass().getName())
                .append("</td><td>")
                .append(str.length() == 0 ? "&nbsp;" : str)
                .append("</td></tr>\n");
        }
        sb.append("</table></center>\n");
        sb.append("</blockquote>\n");
        sb.append("<hr>");
        sb.append("<address>Created : ").append(new Date()).append(
                        "</address>\n");
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
