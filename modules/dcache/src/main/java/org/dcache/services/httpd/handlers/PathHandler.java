package org.dcache.services.httpd.handlers;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.FileNameMap;
import java.net.URLConnection;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.dcache.services.httpd.util.StandardHttpRequest;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import dmg.util.HttpException;
import dmg.util.HttpRequest;

public class PathHandler extends AbstractHandler {

    private static final FileNameMap mimeTypeMap = URLConnection.getFileNameMap();

    private File path;

    public PathHandler(File path) {
        this.path = path;
    }

    @Override
    public void handle(String target, Request baseRequest,
                    HttpServletRequest request, HttpServletResponse response)
                    throws IOException, ServletException {
        HttpRequest proxy = null;
        try {
            proxy = new StandardHttpRequest(request, response);
            sendFile(path, proxy);
        } catch (Exception t) {
            throw new ServletException("PathHandler failure", t);
        }
    }


    private void sendFile(File base, HttpRequest proxy) throws Exception {
        String filename = null;
        String[] tokens = proxy.getRequestTokens();
        if (tokens.length < 2) {
            filename = "index.html";
        } else {
            final StringBuilder sb = new StringBuilder();
            sb.append(tokens[1]);
            for (int i = 2; i < tokens.length; i++) {
                sb.append("/").append(tokens[i]);
            }
            filename = sb.toString();
        }
        final File f = base.isFile() ? base : new File(base, filename);
        if (!f.getCanonicalFile().getAbsolutePath().startsWith(
                        base.getCanonicalFile().getAbsolutePath())) {
            throw new HttpException(HttpServletResponse.SC_FORBIDDEN,
                            "Forbidden");
        }

        if (!f.isFile()) {
            throw new HttpException(HttpServletResponse.SC_NOT_FOUND,
                            "Not found : " + filename);
        }

        final FileInputStream binary = new FileInputStream(f);
        try {
            int rc = 0;
            final byte[] buffer = new byte[4 * 1024];
            proxy.setContentType(getContentTypeFor(filename));
            while ((rc = binary.read(buffer, 0, buffer.length)) > 0) {
                proxy.getOutputStream().write(buffer, 0, rc);
            }
        } finally {
            proxy.getOutputStream().flush();
            try {
                binary.close();
            } catch (final IOException e) {
            }
        }
    }

    private String getContentTypeFor(String fileName) {
        if (fileName.endsWith(".html")) {
            return "text/html";
        } else if (fileName.endsWith(".css")) {
            return "text/css";
        } else {
            return mimeTypeMap.getContentTypeFor(fileName);
        }
    }
}
