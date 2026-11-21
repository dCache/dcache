package org.dcache.services.httpd.handlers;

import static jakarta.servlet.http.HttpServletResponse.SC_FORBIDDEN;
import static jakarta.servlet.http.HttpServletResponse.SC_NOT_FOUND;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import dmg.util.HttpException;
import dmg.util.HttpRequest;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.net.FileNameMap;
import java.net.URLConnection;
import java.nio.file.Files;
import java.util.Arrays;
import org.dcache.services.httpd.util.StandardHttpRequest;

/**
 * Provides HTML or .css content from static file.
 *
 * @author arossi
 */
public class PathAlias implements AliasHandler {

    private static final FileNameMap mimeTypeMap = URLConnection.getFileNameMap();

    private final File path;

    public PathAlias(File path) {
        this.path = path;
    }

    @Override
    public void handle(HttpServletRequest request, HttpServletResponse response)
          throws IOException, HttpException {
        sendFile(path, StandardHttpRequest.of(request, response));
    }


    private void sendFile(File base, HttpRequest proxy) throws HttpException, IOException {
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
            throw new HttpException(SC_FORBIDDEN, "Forbidden");
        }

        if (!f.isFile()) {
            throw new HttpException(SC_NOT_FOUND, "Not found : " + filename);
        }

        try {
            proxy.setContentType(getContentTypeFor(filename));
            Files.copy(f.toPath(), proxy.getOutputStream());
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
