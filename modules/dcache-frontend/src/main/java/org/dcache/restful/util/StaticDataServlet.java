/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2017 Deutsches Elektronen-Synchrotron
 * Copyright (C) 2026 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.restful.util;

import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.dcache.util.spring.Required;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.QuotedQualityCSV;


/**
 * A class that accepts client GET requests and replies with static information.  The data is
 * static, but the representation is negotiable, based on the client-supplied preferences.  If the
 * client expresses no preference then the filename determines the format.
 */
public class StaticDataServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;

    private static enum Media {
        JSON("application/json", "", "\n"),
        JAVASCRIPT("application/javascript", "var CONFIG = ", ";\n");

        private final String pre;
        private final String post;
        private final String mime;

        Media(String mime, String pre, String post) {
            this.mime = mime;
            this.pre = pre;
            this.post = post;
        }
    }

    private String json;

    @Required
    public void setData(Map<String, String> data) {
        json = new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create().toJson(data);
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
          throws IOException {
        Media media = decideMedia(request.getServletPath(), request);

        response.setContentType(media.mime);
        response.setCharacterEncoding("UTF-8");
        byte[] body = (media.pre + json + media.post).getBytes(StandardCharsets.UTF_8);
        response.setContentLength(body.length);
        response.setStatus(HttpServletResponse.SC_OK);

        response.getOutputStream().write(body);
    }

    private Media decideMedia(String target, HttpServletRequest request) {
        QuotedQualityCSV types = new QuotedQualityCSV();
        Collections.list(request.getHeaders(HttpHeader.ACCEPT.asString()))
              .forEach(types::addValue);

        for (String type : types.getValues()) {
            switch (type) {
                case "application/json":
                case "text/json":
                    return Media.JSON;
                case "application/javascript":
                case "text/javascript":
                    return Media.JAVASCRIPT;
            }
        }

        int dot = target.lastIndexOf('.');
        if (dot != -1) {
            String extension = target.substring(dot + 1);
            switch (extension) {
                case "js":
                    return Media.JAVASCRIPT;
                case "json":
                    return Media.JSON;
            }
        }

        return Media.JAVASCRIPT;
    }
}
