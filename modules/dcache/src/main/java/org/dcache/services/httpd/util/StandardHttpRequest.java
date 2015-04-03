package org.dcache.services.httpd.util;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import dmg.util.HttpRequest;
import java.util.Base64;

/**
 * Basic implementation of internal interface used to hold objects for processing
 * an Http request.
 *
 * @author arossi
 */
public class StandardHttpRequest implements HttpRequest {
    private final static Splitter PATH_SPLITTER
        = Splitter.on('/').omitEmptyStrings();
    private final static Logger logger
        = LoggerFactory.getLogger(StandardHttpRequest.class);

    private final OutputStream out;
    private final PrintWriter pw;
    private final HttpServletRequest request;
    private final HttpServletResponse response;
    private final Map<String, String> map = new HashMap<>();
    private final int tokenOffset;
    private final String[] tokens;
    private final boolean isDirectory;

    private String userName;
    private String password;
    private boolean authDone;

    public StandardHttpRequest(HttpServletRequest request,
                    HttpServletResponse response) throws IOException, URISyntaxException {
        this.request = request;
        this.response = response;
        out = response.getOutputStream();
        pw = new PrintWriter(new OutputStreamWriter(out));
        final Enumeration<String> names = request.getHeaderNames();
        while (names.hasMoreElements()) {
            final String name = names.nextElement();
            map.put(name, request.getHeader(name));
        }
        final String path = new URI(request.getRequestURI()).getPath();
        isDirectory = path.endsWith("/");
        tokens = Iterables.toArray(PATH_SPLITTER.split(path), String.class);
        tokenOffset = 1;
        setContentType("text/html");
    }

    @Override
    public OutputStream getOutputStream() {
        return out;
    }

    @Override
    public String getParameter(String parameter) {
        return request.getParameter(parameter);
    }

    @Override
    public String getPassword() {
        doAuthorization();
        return password;
    }

    @Override
    public PrintWriter getPrintWriter() {
        return pw;
    }

    @Override
    public Map<String, String> getRequestAttributes() {
        return map;
    }

    @Override
    public int getRequestTokenOffset() {
        return tokenOffset;
    }

    @Override
    public String[] getRequestTokens() {
        return tokens;
    }

    @Override
    public String getUserName() {
        return userName;
    }

    @Override
    public boolean isAuthenticated() {
        doAuthorization();
        return userName != null;
    }

    @Override
    public boolean isDirectory() {
        return isDirectory;
    }

    @Override
    public void printHttpHeader(int size) {
        if (size > 0) {
            response.setContentLength(size);
        }
    }

    @Override
    public void setContentType(String type) {
        response.setContentType(type);
    }

    private synchronized void doAuthorization() {
        if (authDone) {
            return;
        }
        authDone = true;
        String auth = request.getHeader("Authorization");
        if (auth == null) {
            return;
        }
        StringTokenizer st = new StringTokenizer(auth);
        if (st.countTokens() < 2) {
            return;
        }
        if (!st.nextToken().equals("Basic")) {
            return;
        }
        auth = new String(Base64.getDecoder().decode(st.nextToken()));
        logger.info("Authentication : >{}<", auth);
        st = new StringTokenizer(auth, ":");
        if (st.countTokens() < 2) {
            return;
        }
        userName = st.nextToken();
        password = st.nextToken();
    }
}
