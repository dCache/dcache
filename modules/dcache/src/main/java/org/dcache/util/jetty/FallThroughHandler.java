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
package org.dcache.util.jetty;

import java.io.IOException;
import java.util.List;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.eclipse.jetty.ee8.nested.Handler;
import org.eclipse.jetty.ee8.nested.HandlerWrapper;
import org.eclipse.jetty.ee8.nested.Request;
import org.eclipse.jetty.server.Server;

/**
 * Handler wrapper that first offers the request to a list of handlers and passes it to the
 * wrapped handler only if none of them handled it.
 * <p>
 * Used with {@code ContextHandler#insertHandler}, which accepts a single wrapper, to put handlers
 * in front of a servlet context.
 */
public class FallThroughHandler extends HandlerWrapper {

    private List<Handler> first = List.of();

    public void setFirst(List<Handler> handlers) {
        first.forEach(this::removeBean);
        first = List.copyOf(handlers);
        first.forEach(this::addBean);
    }

    @Override
    public void setServer(Server server) {
        super.setServer(server);
        first.forEach(h -> h.setServer(server));
    }

    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request,
          HttpServletResponse response) throws IOException, ServletException {
        for (Handler handler : first) {
            handler.handle(target, baseRequest, request, response);
            if (baseRequest.isHandled()) {
                return;
            }
        }
        super.handle(target, baseRequest, request, response);
    }
}
