/* dCache - http://www.dcache.org/
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
package org.dcache.http;

import jakarta.servlet.ServletContext;
import java.util.List;
import org.eclipse.jetty.ee11.servlet.ServletContextHandler;
import org.eclipse.jetty.http.MimeTypes;

/**
 * Jetty adds a charset it infers to text content types that have none, e.g. {@code text/plain}
 * becomes {@code text/plain;charset=iso-8859-1}. Doors serving files must send the type stored
 * with the file instead.
 */
public final class InferredCharsets {

    /**
     * The types for which Jetty infers a charset; see Jetty's {@code encoding.properties}.
     */
    private static final List<String> TYPES =
          List.of("text/html", "text/plain", "text/xml", "application/xhtml+xml");

    private InferredCharsets() {
    }

    /**
     * Stops Jetty from inferring a charset for responses of the given servlet context.
     */
    public static void disable(ServletContext context) {
        MimeTypes.Mutable mimeTypes =
              ServletContextHandler.getServletContextHandler(context).getMimeTypes();
        TYPES.forEach(type -> mimeTypes.addInferred(type, null));
    }
}
