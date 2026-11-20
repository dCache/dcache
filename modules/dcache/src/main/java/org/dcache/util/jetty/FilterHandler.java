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
import javax.servlet.Filter;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.dcache.util.spring.Required;
import org.eclipse.jetty.ee8.nested.HandlerWrapper;
import org.eclipse.jetty.ee8.nested.Request;

/**
 * Runs a servlet filter in a chain of nested Jetty handlers: the rest of the filter chain is the
 * wrapped handler. A request the filter does not pass on is handled.
 * <p>
 * The filter's lifecycle is not managed; filters used here do not implement {@code init} or
 * {@code destroy}. Used by doors whose handler chains are not servlet contexts yet; to be removed
 * with them.
 */
public class FilterHandler extends HandlerWrapper {

    private Filter filter;

    @Required
    public void setFilter(Filter filter) {
        this.filter = filter;
    }

    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request,
          HttpServletResponse response) throws IOException, ServletException {
        if (!isStarted() || baseRequest.isHandled()) {
            return;
        }

        boolean[] passedOn = {false};
        filter.doFilter(request, response, (req, resp) -> {
            passedOn[0] = true;
            FilterHandler.super.handle(target, baseRequest, (HttpServletRequest) req,
                  (HttpServletResponse) resp);
        });
        if (!passedOn[0]) {
            baseRequest.setHandled(true);
        }
    }
}
