/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2017 Deutsches Elektronen-Synchrotron
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

import com.google.common.base.Splitter;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.MetaData;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import com.google.gson.GsonBuilder;
import org.springframework.beans.factory.annotation.Required;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.List;
import java.util.Map;


/**
 * A class that accepts client GET requests and replies with
 * static information.  The data is static, but the representation is
 * negotiable, based on the client-supplied preferences.  If the client
 * expresses no preference then the filename determines the format.
 */
public class StaticDataHandler extends AbstractHandler
{
    private static enum Media
    {
        JSON("application/json", "", "\n"),
        JAVASCRIPT("application/javascript", "var CONFIG = ", ";\n");

        private final String pre;
        private final String post;
        private final String mime;

        Media(String mime, String pre, String post)
        {
            this.mime = mime;
            this.pre = pre;
            this.post = post;
        }
    }

    private List<String> paths;
    private String json;

    @Required
    public void setPath(String path)
    {
        paths = Splitter.on(':').splitToList(path);
    }

    @Required
    public void setData(Map<String,String> data)
    {
        json = new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create().toJson(data);
    }

    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException
    {
        if (isStarted() && !baseRequest.isHandled() && paths.contains(target)) {
            if (request.getMethod().equals("GET")) {
                handleRequest(target, baseRequest, response);
                baseRequest.setHandled(true);
            } else {
                doError(target, baseRequest, request, response);
            }
        }
    }

    private Media decideMedia(String target, Request request)
    {
        MetaData.Request metadata = request.getMetaData();
        List<String> types = metadata == null
                ? Collections.emptyList()
                : metadata.getFields().getQualityCSV(HttpHeader.ACCEPT);

        for (String type : types) {
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
            String extension = target.substring(dot+1);
            switch (extension) {
            case "js":
                return Media.JAVASCRIPT;
            case "json":
                return Media.JSON;
            }
        }

        return Media.JAVASCRIPT;
    }

    private void handleRequest(String target, Request request, HttpServletResponse response) throws IOException
    {
        Media media = decideMedia(target, request);

        response.setContentType(media.mime);
        response.setCharacterEncoding("UTF-8");
        response.setContentLength(media.pre.length() + json.length() + media.post.length());
        response.setStatus(HttpServletResponse.SC_OK);

        PrintWriter writer = response.getWriter();
        writer.print(media.pre);
        writer.print(json);
        writer.print(media.post);
        writer.flush();
    }
}
