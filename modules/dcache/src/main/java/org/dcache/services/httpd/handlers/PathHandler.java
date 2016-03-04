package org.dcache.services.httpd.handlers;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.io.Files;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.File;
import java.io.IOException;
import java.net.FileNameMap;
import java.net.URLConnection;
import java.util.Arrays;

import dmg.util.HttpException;
import dmg.util.HttpRequest;

import org.dcache.services.httpd.util.StandardHttpRequest;

/**
 * Provides HTML or .css content from static file.
 *
 * @author arossi
 */
public class PathHandler extends AbstractHandler {

    private static final FileNameMap mimeTypeMap = URLConnection.getFileNameMap();

    private final File path;

    public PathHandler(File path) {
        this.path = path;
    }

    @Override
    public void handle(String target, Request baseRequest,
                    HttpServletRequest request, HttpServletResponse response)
                    throws IOException, ServletException {
        try {
            HttpRequest proxy = new StandardHttpRequest(request, response);
            sendFile(path, proxy);
        } catch (Exception t) {
            throw new ServletException("PathHandler failure", t);
        }
    }


    private void sendFile(File base, HttpRequest proxy) throws Exception {
        String filename;
        String[] tokens = proxy.getRequestTokens();
        if (tokens.length < 2) {
            filename = "index.html";
        } else {
            filename = Joiner.on("/").join(Iterables.skip(Arrays.asList(tokens), 1));
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

        try {
            proxy.setContentType(getContentTypeFor(filename));
            Files.copy(f, proxy.getOutputStream());
        } finally {
            proxy.getOutputStream().flush();
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
