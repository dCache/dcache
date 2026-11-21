/*
 * dCache - http://www.dcache.org/
 *
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

import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import org.dcache.util.spring.Required;

/**
 * Serves a single file, e.g. a page mapped to exact paths inside the path space of another
 * servlet; 404 if the file does not exist. The file is read for every request.
 */
public class FileServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;

    private transient Path file;

    @Required
    public void setFile(String file) {
        this.file = Path.of(file);
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
          throws IOException {
        byte[] content;
        try {
            content = Files.readAllBytes(file);
        } catch (NoSuchFileException e) {
            response.sendError(HttpServletResponse.SC_NOT_FOUND);
            return;
        }
        String type = getServletContext().getMimeType(file.getFileName().toString());
        if (type != null) {
            response.setContentType(type);
        }
        response.setContentLength(content.length);
        response.getOutputStream().write(content);
    }
}
